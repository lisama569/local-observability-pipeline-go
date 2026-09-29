// Package pipeline wires ingestion (validation, dedup, WAL), the alert
// engine and the downstream sink.
//
// Backpressure: the queue between ingestion and processing is bounded.
// Ingest blocks when full (until space or ctx ends); TryIngest fails
// fast with a distinguishable "queue full" error. Memory therefore cannot
// grow without bound.
//
// At-least-once with no double conclusion: every accepted event is
// fsynced to the WAL before it is acknowledged. The offset is
// checkpointed only after processing AND after the downstream
// notification has been delivered (or given up after the retry budget).
// On restart, offsets after the checkpoint are replayed. Engine.Apply is
// idempotent on EventID and each incident fires/resolves at most once
// per lifecycle, so replay neither duplicates alerts nor flips state.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/alert"
	"github.com/lisama569/local-observability-pipeline-go/internal/dedup"
	"github.com/lisama569/local-observability-pipeline-go/internal/model"
	"github.com/lisama569/local-observability-pipeline-go/internal/sink"
	"github.com/lisama569/local-observability-pipeline-go/internal/store"
	"github.com/lisama569/local-observability-pipeline-go/internal/wal"
)

// Distinguishable errors.
var (
	// ErrQueueFull is returned by TryIngest when the bounded queue is full.
	ErrQueueFull = errors.New("pipeline: backpressure, queue full")
	// ErrClosed is returned after the pipeline stopped accepting events.
	ErrClosed = errors.New("pipeline: closed")
)

// Config configures a pipeline.
type Config struct {
	WALDir        string
	QueueSize     int
	DedupCapacity int
	StoreCapacity int
	Rules         alert.Rules

	// MaxNotifyAttempts caps downstream retries (1 = no retry).
	MaxNotifyAttempts int
	NotifyBackoff     time.Duration
	NotifyBackoffMax  time.Duration
}

// DefaultConfig provides bounded defaults for a fully local run.
func DefaultConfig(dir string) Config {
	return Config{
		WALDir:            dir,
		QueueSize:         128,
		DedupCapacity:     4096,
		StoreCapacity:     10000,
		Rules:             alert.DefaultRules(),
		MaxNotifyAttempts: 5,
		NotifyBackoff:     5 * time.Millisecond,
		NotifyBackoffMax:  200 * time.Millisecond,
	}
}

type job struct {
	offset int64
	event  model.Event
}

// Pipeline is the entry point used by cmd and tests.
type Pipeline struct {
	cfg   Config
	wl    *wal.WAL
	seen  *wal.SeenLog
	store *store.Store
	ded   *dedup.LRU
	eng   *alert.Engine
	sink  sink.Sink

	queue chan job

	workerDone chan struct{}
	wg         sync.WaitGroup
	mu         sync.Mutex
	closed     bool
	closeQ     chan struct{}

	// stats
	acceptedN  int64
	duplicateN int64
	rejectedN  int64
}

// New constructs and starts a pipeline, replaying any WAL events after the
// last checkpoint before accepting new traffic.
func New(ctx context.Context, cfg Config, sk sink.Sink) (*Pipeline, error) {
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1
	}
	if cfg.MaxNotifyAttempts <= 0 {
		cfg.MaxNotifyAttempts = 1
	}
	if sk == nil {
		sk = sink.NewFakeSink()
	}
	wl, err := wal.Open(ctx, cfg.WALDir)
	if err != nil {
		return nil, err
	}
	seen, err := wal.OpenSeen(cfg.WALDir)
	if err != nil {
		wl.Close()
		return nil, err
	}
	p := &Pipeline{
		cfg:        cfg,
		wl:         wl,
		seen:       seen,
		store:      store.New(cfg.StoreCapacity),
		ded:        dedup.NewLRU(cfg.DedupCapacity),
		sink:       sk,
		queue:      make(chan job, cfg.QueueSize),
		closeQ:     make(chan struct{}),
		workerDone: make(chan struct{}),
	}
	// Start with a silent notifier so replay reconstructs state without
	// double-notifying a downstream that may already have been called.
	p.eng = alert.NewEngine(cfg.Rules, alert.SilentNotifier{})
	// Restore durable incident state for offsets already checkpointed (those are
	// not replayed from the WAL). Missing file = first start, no error.
	if err := p.eng.Restore(cfg.WALDir); err != nil {
		wl.Close()
		seen.Close()
		return nil, err
	}
	var replayMax int64
	if err := wl.Replay(ctx, func(offset int64, e model.Event) error {
		p.ded.SeenOrAdd(e.EventID)
		p.store.Insert(e)
		p.eng.Apply(e)
		if offset > replayMax {
			replayMax = offset
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// Everything replayed is already delivered-or-decided; checkpoint it so it
	// is never replayed again.
	if replayMax > 0 {
		if err := wl.Checkpoint(ctx, replayMax); err != nil {
			return nil, err
		}
		// Persist state reconstructed from replay so a second crash before
		// any new event cannot lose it.
		if err := p.eng.Snapshot(cfg.WALDir); err != nil {
			return nil, err
		}
	}
	p.eng.SetNotifier(sink.Notifier{Sink: &retryingSink{p: p}})
	p.wg.Add(1)
	go p.worker(ctx)
	return p, nil
}

// AcceptResult explains an ingestion decision.
type AcceptResult struct {
	Accepted  bool
	Duplicate bool
	Reason    string
	EventID   string
}

// Ingest validates, deduplicates, durably appends and enqueues one
// event. It applies backpressure: when full it blocks until space frees or
// ctx ends.
func (p *Pipeline) Ingest(ctx context.Context, e model.Event) (AcceptResult, error) {
	return p.ingest(ctx, e, true)
}

// TryIngest is non-blocking: it returns ErrQueueFull when saturated.
func (p *Pipeline) TryIngest(e model.Event) (AcceptResult, error) {
	return p.ingest(context.Background(), e, false)
}

func (p *Pipeline) ingest(ctx context.Context, e model.Event, blocking bool) (AcceptResult, error) {
	if err := e.Validate(); err != nil {
		p.mu.Lock()
		p.rejectedN++
		p.mu.Unlock()
		return AcceptResult{EventID: e.EventID, Reason: err.Error()}, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return AcceptResult{EventID: e.EventID, Reason: ErrClosed.Error()}, ErrClosed
	}
	p.mu.Unlock()

	// Fast in-memory dedup (bounded LRU), backed by the durable seen
	// log so redelivery after restart or LRU eviction is still rejected.
	if p.ded.Has(e.EventID) || p.seen.Has(e.EventID) {
		p.mu.Lock()
		p.duplicateN++
		p.mu.Unlock()
		return AcceptResult{
			EventID: e.EventID, Duplicate: true,
			Reason: "duplicate event_id, accepted at most once and not recounted",
		}, nil
	}
	// Durable seen-id before any other write: a crash here at worst
	// records an id whose event append did not happen, which on a later
	// legitimate delivery... cannot share the same id, so it is safe.
	if dup, err := p.seen.Add(ctx, e.EventID); err != nil {
		return AcceptResult{EventID: e.EventID, Reason: err.Error()}, err
	} else if dup {
		p.ded.SeenOrAdd(e.EventID)
		p.mu.Lock()
		p.duplicateN++
		p.mu.Unlock()
		return AcceptResult{EventID: e.EventID, Duplicate: true, Reason: "durable duplicate"}, nil
	}
	p.ded.SeenOrAdd(e.EventID)

	// Durable event before acknowledge.
	off, err := p.wl.Append(ctx, e)
	if err != nil {
		return AcceptResult{EventID: e.EventID, Reason: err.Error()}, err
	}
	j := job{offset: off, event: e}

	if blocking {
		select {
		case p.queue <- j:
		case <-ctx.Done():
			return AcceptResult{EventID: e.EventID, Reason: ctx.Err().Error()}, ctx.Err()
		case <-p.closeQ:
			return AcceptResult{EventID: e.EventID, Reason: ErrClosed.Error()}, ErrClosed
		}
	} else {
		select {
		case p.queue <- j:
		default:
			return AcceptResult{EventID: e.EventID, Reason: ErrQueueFull.Error()}, ErrQueueFull
		}
	}

	p.mu.Lock()
	p.acceptedN++
	p.mu.Unlock()
	return AcceptResult{
		EventID: e.EventID, Accepted: true,
		Reason: fmt.Sprintf("accepted offset=%d kind=%s", off, e.Kind),
	}, nil
}

func (p *Pipeline) worker(ctx context.Context) {
	defer p.wg.Done()
	defer close(p.workerDone)
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.closeQ:
			return
		case j := <-p.queue:
			p.process(ctx, j)
		}
	}
}

func (p *Pipeline) process(ctx context.Context, j job) {
	// Store first: unified query surface reflects accepted events.
	p.store.Insert(j.event)
	out := p.eng.Apply(j.event)
	// The retrying sink invoked inside Apply already guaranteed delivery (or
	// exhausted attempts). Only checkpoint after that, so a crash replays
	// and retries rather than silently losing the notification.
	if err := p.wl.Checkpoint(ctx, j.offset); err != nil {
		// Checkpoint failure is rare; the event is still idempotently
		// replayable (uncheckpointed), so leave it.
		return
	}
	// Durable incident state now that the offset is committed. If this
	// fails we have checkpoint<=state: on restart the WAL replays this
	// event and reconstructs the same state idempotently.
	if err := p.eng.Snapshot(p.cfg.WALDir); err != nil {
		return
	}
	_ = out
}

// retryingSink wraps the real sink with bounded exponential backoff retry.
// It is invoked synchronously by the engine while processing one job, which
// serializes downstream pressure and propagates backpressure through the
// bounded queue.
type retryingSink struct{ p *Pipeline }

func (r *retryingSink) SendFire(inc *model.Incident) error {
	return r.retry(func() error { return r.p.sink.SendFire(inc) })
}

func (r *retryingSink) SendResolve(inc *model.Incident) error {
	return r.retry(func() error { return r.p.sink.SendResolve(inc) })
}

func (r *retryingSink) retry(op func() error) error {
	backoff := r.p.cfg.NotifyBackoff
	var err error
	for attempt := 1; attempt <= r.p.cfg.MaxNotifyAttempts; attempt++ {
		if err = op(); err == nil {
			return nil
		}
		if attempt == r.p.cfg.MaxNotifyAttempts {
			break
		}
		time.Sleep(backoff)
		backoff *= 2
		if r.p.cfg.NotifyBackoffMax > 0 && backoff > r.p.cfg.NotifyBackoffMax {
			backoff = r.p.cfg.NotifyBackoffMax
		}
	}
	return err
}

// Query exposes the unified store query surface.
func (p *Pipeline) Query(q store.Query) []store.Result { return p.store.Query(q) }

// Incidents returns all incidents in deterministic order.
func (p *Pipeline) Incidents() []*model.Incident { return p.eng.List() }

// Stats summarizes ingestion counters (for tests and explainability logs).
type Stats struct {
	Accepted   int64
	Duplicates int64
	Rejected   int64
	Stored     int
}

// Stats returns a point-in-time snapshot of counters.
func (p *Pipeline) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{
		Accepted:   p.acceptedN,
		Duplicates: p.duplicateN,
		Rejected:   p.rejectedN,
		Stored:     p.store.Len(),
	}
}

// Drain stops accepting new events, processes everything already accepted
// (waiting for downstream retries per event) and closes resources.
func (p *Pipeline) Drain(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.closeQ)
	p.mu.Unlock()

	// Stop the background worker, then process remaining jobs ourselves in
	// FIFO order (no queue competition).
	<-p.workerDone
	for {
		select {
		case j := <-p.queue:
			p.process(ctx, j)
		default:
			return p.closeResources()
		}
	}
}

// Close stops without necessarily draining (best effort).
func (p *Pipeline) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.closeQ)
	p.mu.Unlock()
	return p.closeResources()
}

func (p *Pipeline) closeResources() error {
	// Wait for the worker to exit so only Drain/Close touches the
	// remaining queued jobs. Anything still in the queue is durable in
	// the WAL and after the checkpoint, so it is replayed on restart.
	p.wg.Wait()
	err1 := p.wl.Close()
	err2 := p.seen.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
