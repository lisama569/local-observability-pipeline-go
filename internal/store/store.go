// Package store 提供基于 WAL 的持久化事件存储：
// 追加写日志保证重启可恢复，内存索引支持统一查询与乱序事件的确定性排序。
package store

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

// ErrDuplicate 表示同一来源的重复投递被去重（不计为新数据）。
var ErrDuplicate = errors.New("store: duplicate event (deduplicated)")

// Record 是落盘的一条记录：已接收事件或告警通知确认。
type Record struct {
	Seq       uint64       `json:"seq"` // 单调递增序号
	Type      RecordType   `json:"type"`
	Event     *model.Event `json:"event,omitempty"`
	NotifyKey string       `json:"notify_key,omitempty"`
}

// RecordType 区分 WAL 记录类型。
type RecordType string

const (
	RecordEvent    RecordType = "event"
	RecordNotified RecordType = "notified"
)

// walRecord 是实际落盘的行格式。
type walRecord struct {
	Seq       uint64       `json:"seq"`
	Type      RecordType   `json:"type"`
	Event     *model.Event `json:"event,omitempty"`
	NotifyKey string       `json:"notify_key,omitempty"`
}

// Stats 描述存储运行状态，用于测试与观测。
type Stats struct {
	Accepted   int // 接受（去重后）的事件数
	Duplicates int // 被去重的重复投递数
	Rejected   int // 校验失败被拒绝的事件数
}

// Options 配置存储。
type Options struct {
	Dir         string        // WAL 所在目录
	DedupWindow time.Duration // 去重窗口：窗口外的旧去重键可被驱逐，限制内存
	MaxInMemory int           // 内存索引最大事件数，超出驱逐最旧（WAL 仍完整）
	SyncOnWrite bool          // 每次写入后 fsync
}

func (o *Options) withDefaults() {
	if o.DedupWindow <= 0 {
		o.DedupWindow = 10 * time.Minute
	}
	if o.MaxInMemory <= 0 {
		o.MaxInMemory = 100000
	}
}

// dedupEntry 记录一个去重键及其事件时间，用于按窗口驱逐。
type dedupEntry struct {
	ts time.Time
}

// indexedEvent 是内存索引条目，携带接收序号以支持按序重放。
type indexedEvent struct {
	seq uint64
	ev  model.Event
}

// Store 是线程安全的事件存储。
type Store struct {
	mu     sync.Mutex
	opts   Options
	wal    *os.File
	enc    *json.Encoder
	seq    uint64
	dedup  map[string]dedupEntry // DedupKey -> 事件时间
	index  []indexedEvent        // 按 (Timestamp, 接收序) 排序的内存索引
	notify map[string]bool       // 已确认的告警迁移键
	stats  Stats
	closed bool
}

// Open 打开（或创建）存储，并重放 WAL 恢复状态。
func Open(opts Options) (*Store, error) {
	opts.withDefaults()
	if opts.Dir == "" {
		return nil, errors.New("store: Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	s := &Store{
		opts:   opts,
		dedup:  map[string]dedupEntry{},
		notify: map[string]bool{},
	}
	path := filepath.Join(opts.Dir, "wal.log")
	if err := s.replay(path); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open wal: %w", err)
	}
	s.wal = f
	s.enc = json.NewEncoder(f)
	return s, nil
}

// replay 逐行重放 WAL，重建去重表、索引与已通知集合。
// 重放是确定性的：相同 WAL 必然得到相同状态。
func (s *Store) replay(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: open wal for replay: %w", err)
	}
	defer f.Close()
	dec := json.NewDecoder(bufio.NewReader(f))
	for {
		var rec walRecord
		if err := dec.Decode(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("store: replay wal: %w", err)
		}
		switch rec.Type {
		case RecordEvent:
			if rec.Event == nil {
				continue
			}
			s.applyEvent(rec.Seq, *rec.Event)
		case RecordNotified:
			s.notify[rec.NotifyKey] = true
		}
		if rec.Seq > s.seq {
			s.seq = rec.Seq
		}
	}
}

// applyEvent 将事件并入内存状态（重放与新写入共用，保证一致）。
func (s *Store) applyEvent(seq uint64, e model.Event) {
	s.dedup[e.DedupKey()] = dedupEntry{ts: e.Timestamp}
	// 按 (Timestamp, 尾部追加) 插入，保证乱序事件查询结果确定。
	pos := sort.Search(len(s.index), func(i int) bool {
		return s.index[i].ev.Timestamp.After(e.Timestamp)
	})
	s.index = append(s.index, indexedEvent{})
	copy(s.index[pos+1:], s.index[pos:])
	s.index[pos] = indexedEvent{seq: seq, ev: e}
	s.stats.Accepted++
	s.evictLocked(e.Timestamp)
}

// evictLocked 按窗口驱逐旧去重键、按容量驱逐最旧索引，限制内存增长。
func (s *Store) evictLocked(latest time.Time) {
	cutoff := latest.Add(-s.opts.DedupWindow)
	for k, v := range s.dedup {
		if v.ts.Before(cutoff) {
			delete(s.dedup, k)
		}
	}
	if len(s.index) > s.opts.MaxInMemory {
		s.index = append([]indexedEvent(nil), s.index[len(s.index)-s.opts.MaxInMemory:]...)
	}
}

// Append 校验、去重并持久化一个事件；重复投递返回 ErrDuplicate。
func (s *Store) Append(ctx context.Context, e model.Event) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := e.Validate(); err != nil {
		s.mu.Lock()
		s.stats.Rejected++
		s.mu.Unlock()
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errors.New("store: closed")
	}
	key := e.DedupKey()
	if _, dup := s.dedup[key]; dup {
		s.stats.Duplicates++
		return 0, ErrDuplicate
	}
	s.seq++
	seq := s.seq
	rec := walRecord{Seq: seq, Type: RecordEvent, Event: &e}
	if err := s.enc.Encode(rec); err != nil {
		return 0, fmt.Errorf("store: wal write: %w", err)
	}
	if s.opts.SyncOnWrite {
		if err := s.wal.Sync(); err != nil {
			return 0, fmt.Errorf("store: wal sync: %w", err)
		}
	}
	s.applyEvent(seq, e)
	return seq, nil
}

// MarkNotified 记录某条告警迁移已被下游确认，重启后不再重复通知。
func (s *Store) MarkNotified(ctx context.Context, notifyKey string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("store: closed")
	}
	if s.notify[notifyKey] {
		return nil
	}
	s.seq++
	rec := walRecord{Seq: s.seq, Type: RecordNotified, NotifyKey: notifyKey}
	if err := s.enc.Encode(rec); err != nil {
		return fmt.Errorf("store: wal write: %w", err)
	}
	if s.opts.SyncOnWrite {
		if err := s.wal.Sync(); err != nil {
			return fmt.Errorf("store: wal sync: %w", err)
		}
	}
	s.notify[notifyKey] = true
	return nil
}

// IsNotified 查询某条告警迁移是否已被确认。
func (s *Store) IsNotified(notifyKey string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify[notifyKey]
}

// Query 按时间升序（Timestamp 排序，同刻按接收序稳定）返回 [from, to] 内的事件。
func (s *Store) Query(from, to time.Time, kind model.Kind) []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []model.Event
	for _, it := range s.index {
		if it.ev.Timestamp.Before(from) || it.ev.Timestamp.After(to) {
			continue
		}
		if kind != "" && it.ev.Kind != kind {
			continue
		}
		out = append(out, it.ev)
	}
	return out
}

// ReplayEvents 按接收序号升序返回全部已接受事件，
// 供重启后按原始顺序重放告警引擎，保证迁移序列确定。
func (s *Store) ReplayEvents() []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := append([]indexedEvent(nil), s.index...)
	sort.SliceStable(items, func(i, j int) bool { return items[i].seq < items[j].seq })
	out := make([]model.Event, 0, len(items))
	for _, it := range items {
		out = append(out, it.ev)
	}
	return out
}

// Stats 返回运行统计。
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Close 关闭存储并落盘。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.wal.Close()
}
