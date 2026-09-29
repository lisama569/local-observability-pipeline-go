// Package pipeline 串联 接入→存储→告警→分发 的完整数据通路，
// 并保证背压、去重、重启恢复语义。
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/alert"
	"github.com/lisama569/local-observability-pipeline-go/internal/model"
	"github.com/lisama569/local-observability-pipeline-go/internal/sink"
	"github.com/lisama569/local-observability-pipeline-go/internal/store"
)

// Result 描述一次接入的判定结果，失败原因可区分。
type Result struct {
	Seq       uint64   // 接受时的存储序号
	Duplicate bool     // 是否为重复投递（被去重）
	Defaulted []string // 被归一化补齐的字段名
}

// Pipeline 是观测数据管道。
type Pipeline struct {
	store *store.Store
	eng   *alert.Engine
	disp  *sink.Dispatcher
}

// Config 配置管道。
type Config struct {
	Store store.Options
	Rules []alert.Rule
	Sink  sink.Options
}

// Open 打开管道：恢复存储、按接收序重放告警引擎、补发未确认的迁移、启动分发。
func Open(cfg Config, downstream sink.Sink) (*Pipeline, error) {
	st, err := store.Open(cfg.Store)
	if err != nil {
		return nil, err
	}
	eng := alert.New(cfg.Rules...)
	p := &Pipeline{store: st, eng: eng}
	p.disp = sink.NewDispatcher(downstream, func(ctx context.Context, key string) error {
		return st.MarkNotified(ctx, key)
	}, cfg.Sink)
	// 恢复：按接收序重放，已确认的迁移不重复通知，未确认的补发。
	for _, ev := range st.ReplayEvents() {
		for _, t := range eng.Observe(ev) {
			if !st.IsNotified(t.Key) {
				p.enqueueBlocking(context.Background(), t)
			}
		}
	}
	return p, nil
}

// Ingest 接入一个事件：校验→归一化→去重→落盘→告警评估→入队通知。
// 返回错误可用 errors.Is 区分：model.ErrMissing* / store.ErrDuplicate / sink.ErrQueueFull。
func (p *Pipeline) Ingest(ctx context.Context, e model.Event) (Result, error) {
	var res Result
	if err := e.Validate(); err != nil {
		return res, err
	}
	res.Defaulted = e.Normalize()
	seq, err := p.store.Append(ctx, e)
	if errors.Is(err, store.ErrDuplicate) {
		res.Duplicate = true
		return res, nil // 重复投递：可判别，不算新数据，不算错误
	}
	if err != nil {
		return res, err
	}
	res.Seq = seq
	for _, t := range p.eng.Observe(e) {
		if p.store.IsNotified(t.Key) {
			continue // 已确认过的迁移（如重放场景）不重复通知
		}
		if err := p.enqueueBlocking(ctx, t); err != nil {
			return res, fmt.Errorf("pipeline: notify backpressure: %w", err)
		}
	}
	return res, nil
}

// enqueueBlocking 在队列满时等待而非丢弃：背压向上游传导，内存有界。
func (p *Pipeline) enqueueBlocking(ctx context.Context, t alert.Transition) error {
	for {
		err := p.disp.Enqueue(ctx, t)
		if !errors.Is(err, sink.ErrQueueFull) {
			return err
		}
		select {
		case <-ctx.Done():
			return sink.ErrQueueFull
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// Query 统一查询三类事件（kind 为空串表示全部）。
func (p *Pipeline) Query(from, to time.Time, kind model.Kind) []model.Event {
	return p.store.Query(from, to, kind)
}

// Alerts 返回当前告警状态快照。
func (p *Pipeline) Alerts() map[string]alert.State {
	return p.eng.Snapshot()
}

// Stats 返回存储统计（接受/去重/拒绝）。
func (p *Pipeline) Stats() store.Stats {
	return p.store.Stats()
}

// Close 关闭管道：先停分发（排空队列并确认），再关存储。
func (p *Pipeline) Close() error {
	if err := p.disp.Close(); err != nil {
		return err
	}
	return p.store.Close()
}
