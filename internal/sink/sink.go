// Package sink 提供有界队列 + 重试的下游分发：
// 下游变慢或失败时施加背压，已确认的迁移记录到 WAL，恢复后不重复、不丢失。
package sink

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/alert"
)

// ErrQueueFull 表示分发队列已满，调用方应施加背压（可区分于下游失败）。
var ErrQueueFull = errors.New("sink: dispatch queue full (backpressure)")

// ErrClosed 表示分发器已关闭。
var ErrClosed = errors.New("sink: dispatcher closed")

// Sink 是下游目的地接口（本地实现：内存/文件，便于测试失败注入）。
type Sink interface {
	Send(ctx context.Context, t alert.Transition) error
}

// AckFunc 在下游确认后回调，用于持久化“已通知”标记。
type AckFunc func(ctx context.Context, notifyKey string) error

// Options 配置分发器。
type Options struct {
	QueueCapacity int           // 有界队列容量，满则 Enqueue 返回 ErrQueueFull
	RetryBackoff  time.Duration // 初始重试退避
	MaxRetries    int           // 最大重试次数（-1 表示无限重试直至确认）
}

func (o *Options) withDefaults() {
	if o.QueueCapacity <= 0 {
		o.QueueCapacity = 1024
	}
	if o.RetryBackoff <= 0 {
		o.RetryBackoff = 50 * time.Millisecond
	}
	if o.MaxRetries == 0 {
		o.MaxRetries = -1 // 默认无限重试：已确认前不丢弃
	}
}

// Dispatcher 以有界队列异步分发告警迁移，失败按退避重试。
type Dispatcher struct {
	sink Sink
	ack  AckFunc
	opts Options

	queue  chan alert.Transition
	done   chan struct{}
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

// NewDispatcher 创建并启动分发器。
func NewDispatcher(s Sink, ack AckFunc, opts Options) *Dispatcher {
	opts.withDefaults()
	d := &Dispatcher{
		sink:  s,
		ack:   ack,
		opts:  opts,
		queue: make(chan alert.Transition, opts.QueueCapacity),
		done:  make(chan struct{}),
	}
	d.wg.Add(1)
	go d.worker()
	return d
}

// Enqueue 提交一次迁移；队列满返回 ErrQueueFull（背压信号，数据未丢失，
// 调用方持有机可重试）。
func (d *Dispatcher) Enqueue(ctx context.Context, t alert.Transition) error {
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return ErrClosed
	}
	select {
	case d.queue <- t:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrQueueFull
	}
}

// worker 串行处理队列：保证同一迁移的确认顺序与产生顺序一致。
func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for {
		select {
		case t := <-d.queue:
			d.deliver(t)
		case <-d.done:
			// 关闭前排空队列，已入队的不丢弃。
			for {
				select {
				case t := <-d.queue:
					d.deliver(t)
				default:
					return
				}
			}
		}
	}
}

// deliver 带退避重试地投递，确认后回调 ack 持久化。
func (d *Dispatcher) deliver(t alert.Transition) {
	backoff := d.opts.RetryBackoff
	for attempt := 0; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := d.sink.Send(ctx, t)
		cancel()
		if err == nil {
			if d.ack != nil {
				// ack 失败只影响“重启后可能重发”，由迁移键幂等兜底。
				_ = d.ack(context.Background(), t.Key)
			}
			return
		}
		if d.opts.MaxRetries >= 0 && attempt >= d.opts.MaxRetries {
			return // 有限重试用尽：放弃（测试场景），不阻塞 worker
		}
		time.Sleep(backoff)
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

// Close 停止 worker 并等待已入队项处理完成。
func (d *Dispatcher) Close() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.mu.Unlock()
	close(d.done)
	d.wg.Wait()
	return nil
}

// MemSink 是测试用内存 Sink，支持注入失败与延迟。
type MemSink struct {
	mu       sync.Mutex
	Sent     []alert.Transition
	FailNext int           // 接下来 N 次 Send 注入失败
	Delay    time.Duration // 每次 Send 注入的延迟（模拟下游变慢）
}

// Send 实现 Sink。
func (m *MemSink) Send(ctx context.Context, t alert.Transition) error {
	m.mu.Lock()
	if m.Delay > 0 {
		d := m.Delay
		m.mu.Unlock()
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
		m.mu.Lock()
	}
	if m.FailNext > 0 {
		m.FailNext--
		m.mu.Unlock()
		return errors.New("memsink: injected failure")
	}
	m.Sent = append(m.Sent, t)
	m.mu.Unlock()
	return nil
}

// SentCount 返回已成功发送的迁移数。
func (m *MemSink) SentCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Sent)
}
