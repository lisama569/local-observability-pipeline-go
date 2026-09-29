package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
	"github.com/lisama569/local-observability-pipeline-go/internal/sink"
	"github.com/lisama569/local-observability-pipeline-go/internal/store"
)

func evt(id, kind string, t time.Time) model.Event {
	e := model.Event{
		EventID: id, Source: "host", Service: "checkout",
		Fingerprint: "err", TraceID: "tr-1", Timestamp: t,
	}
	switch kind {
	case "m":
		e.Kind, e.MetricName, e.Fault = model.KindMetric, "errors", true
	case "l":
		e.Kind, e.Severity, e.Message = model.KindLog, model.SeverityError, "boom"
	case "tr":
		e.Kind, e.SpanName, e.IsError, e.DurationMs = model.KindTrace, "span", true, 10
	case "ok":
		e.Kind, e.MetricName, e.Resolved = model.KindMetric, "errors", true
	}
	return e
}

func cfg(dir string) Config {
	c := DefaultConfig(dir)
	c.QueueSize = 4
	c.NotifyBackoff = time.Millisecond
	return c
}

func fireKey() string { return "checkout|err" }

func TestDuplicateDeliveryNotNewFault(t *testing.T) {
	p, err := New(context.Background(), cfg(t.TempDir()), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	r1, _ := p.Ingest(ctx, evt("e1", "m", time.Unix(1, 0)))
	r2, _ := p.Ingest(ctx, evt("e1", "m", time.Unix(1, 0))) // redelivery
	r3, _ := p.Ingest(ctx, evt("e2", "l", time.Unix(2, 0)))
	t.Logf("input e1: %s | redelivered e1: duplicate=%v %s | e2: %s",
		r1.Reason, r2.Duplicate, r2.Reason, r3.Reason)
	if !r1.Accepted || !r2.Duplicate || !r3.Accepted {
		t.Fatal("first accept, redelivery duplicate, third accept")
	}
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	st := p.Stats()
	if st.Accepted != 2 || st.Duplicates != 1 || st.Stored != 2 {
		t.Fatalf("stats=%+v", st)
	}
	incs := p.Incidents()
	if len(incs) != 1 || incs[0].NotifyCount != 1 {
		t.Fatalf("duplicate must not create new fault/alert: %+v", incs)
	}
}

func TestValidationReasonsDistinguishable(t *testing.T) {
	p, _ := New(context.Background(), cfg(t.TempDir()), nil)
	defer p.Close()

	bad := model.Event{Kind: model.KindLog, Message: "x"} // missing ids
	_, err := p.Ingest(context.Background(), bad)
	var ve *model.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T %v", err, err)
	}
	t.Logf("missing-field rejection reason: %q", err.Error())

	// queue-full reason distinguishable: capacity 2 plus a slow sink means
	// one in-flight + two buffered = full for the duration of the
	// slow sink call, deterministically.
	c := cfg(t.TempDir())
	c.QueueSize = 2
	c.Rules.FireAfterKinds = 1 // first faulting metric hits the slow sink
	c.MaxNotifyAttempts = 1
	slow := sink.NewFakeSink()
	slow.SetLatency(300 * time.Millisecond)
	p2, _ := New(context.Background(), c, slow)

	if _, err := p2.Ingest(context.Background(), evt("z0", "m", time.Unix(0, 0))); err != nil {
		t.Fatal(err)
	}
	ingested := make(chan struct{})
	go func() {
		_, err := p2.Ingest(context.Background(), evt("z1", "m", time.Unix(1, 0)))
		if err == nil {
			close(ingested)
		}
	}()
	// Fill the second buffered slot as well.
	if _, err := p2.TryIngest(evt("z3", "m", time.Unix(3, 0))); err != nil {
		t.Fatalf("z3 should fit: %v", err)
	}
	select {
	case <-ingested:
	case <-time.After(time.Second):
		t.Fatal("second event should enter the 1-slot buffer")
	}
	// Sink still busy with z0 and z1 is buffered: full.
	_, e := p2.TryIngest(evt("z2", "l", time.Unix(2, 0)))
	if !errors.Is(e, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", e)
	}
	t.Logf("backpressure rejection reason: %q", e.Error())
	if err := p2.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Backpressure did not drop data: both accepted jobs processed.
	if p2.Stats().Stored != 3 {
		t.Fatalf("accepted data lost under backpressure: stored=%d", p2.Stats().Stored)
	}
}

func TestOutOfOrderNoFlap(t *testing.T) {
	fs := sink.NewFakeSink()
	p, _ := New(context.Background(), cfg(t.TempDir()), fs)
	ctx := context.Background()
	// fire, then deliver an old resolve and an old signal.
	p.Ingest(ctx, evt("f1", "m", time.Unix(100, 0)))
	p.Ingest(ctx, evt("f2", "l", time.Unix(101, 0)))
	p.Ingest(ctx, evt("old-ok", "ok", time.Unix(10, 0))) // late resolve
	p.Ingest(ctx, evt("old-l", "l", time.Unix(50, 0)))   // late evidence
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	inc := p.Incidents()[0]
	t.Logf("out-of-order inputs; decision: state=%s reason=%q fires=%v resolves=%v",
		inc.State, inc.Reason, fs.FireKeys(), fs.ResolveKeys())
	if inc.State != model.StateFiring {
		t.Fatalf("late resolve must not flap a firing incident, got %s", inc.State)
	}
	if len(fs.ResolveKeys()) != 0 {
		t.Fatal("late resolve must not be delivered downstream")
	}
}

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	fs := sink.NewFakeSink()

	// First process lifetime: fire an incident.
	p1, _ := New(ctx, cfg(dir), fs)
	p1.Ingest(ctx, evt("r1", "m", time.Unix(1, 0)))
	p1.Ingest(ctx, evt("r2", "l", time.Unix(2, 0)))
	if err := p1.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	firesAfterFirst := len(fs.FireKeys())
	if firesAfterFirst != 1 {
		t.Fatalf("expected 1 fire, got %d", firesAfterFirst)
	}

	// Restart with a NEW sink to detect duplicate downstream delivery.
	fs2 := sink.NewFakeSink()
	p2, err := New(ctx, cfg(dir), fs2)
	if err != nil {
		t.Fatal(err)
	}
	// Redelivery after restart of an already-accepted id: duplicate.
	res, _ := p2.Ingest(ctx, evt("r1", "m", time.Unix(1, 0)))
	t.Logf("post-restart redelivery r1: duplicate=%v %s", res.Duplicate, res.Reason)
	// New evidence after restart correlates onto the same recovered incident.
	p2.Ingest(ctx, evt("r3", "tr", time.Unix(3, 0)))
	p2.Ingest(ctx, evt("r4", "ok", time.Unix(4, 0))) // resolves
	if err := p2.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	inc := p2.Incidents()[0]
	t.Logf("after restart: state=%s events=%d fires_new_sink=%v resolves_new_sink=%v reason=%q",
		inc.State, len(inc.EventIDs), fs2.FireKeys(), fs2.ResolveKeys(), inc.Reason)
	if inc.State != model.StateResolved {
		t.Fatalf("restart replay+new resolve -> resolved, got %s", inc.State)
	}
	if len(fs2.FireKeys()) != 0 {
		t.Fatal("replay must not re-send historical fire notification")
	}
	if len(fs2.ResolveKeys()) != 1 {
		t.Fatal("new resolve after restart must be delivered exactly once")
	}
	if len(inc.EventIDs) != 4 {
		t.Fatalf("replayed + new events must correlate to one incident, got %d", len(inc.EventIDs))
	}
	if !res.Duplicate {
		t.Fatal("redelivery after restart must be recognized as duplicate")
	}
}

func TestDownstreamFailureRecoversExactlyOnce(t *testing.T) {
	fs := sink.NewFakeSink()
	c := cfg(t.TempDir())
	// QueueSize 1 keeps jobs buffered until the sink is armed: start with
	// the sink failing, then recover before drain.
	c.QueueSize = 1
	c.MaxNotifyAttempts = 10
	c.NotifyBackoff = 2 * time.Millisecond
	ctx := context.Background()
	// First event reaches the sink while it is down and stays pending via
	// in-engine attempt; to test pipeline-level retry budget we arm
	// failures then enqueue both events, then recover mid-flight.
	// Only fire attempts fail (resolve must succeed: it happens after the
	// downstream recovered). Use a failing-then-fixed wrapper.
	fl := &flakyFireSink{inner: fs, failUntil: 2, fired: make(chan struct{}, 8)}
	p0, _ := New(context.Background(), c, fl)
	p0.Ingest(ctx, evt("d1", "m", time.Unix(1, 0)))
	p0.Ingest(ctx, evt("d2", "l", time.Unix(2, 0)))
	// Wait until two failing fire attempts actually happened, then recover.
	<-fl.fired
	<-fl.fired
	fl.mu.Lock()
	fl.failUntil = 0
	fl.mu.Unlock()
	// now resolve event after recovery
	p0.Ingest(ctx, evt("d3", "ok", time.Unix(3, 0)))
	if err := p0.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	_, _, rok, rfail := fs.AttemptCounts()
	fl.mu.Lock()
	fok, ffail := fl.okN, fl.failN
	fl.mu.Unlock()
	t.Logf("attempts: fire ok=%d failed=%d | resolve ok=%d failed=%d", fok, ffail, rok, rfail)
	if ffail != 2 || fok != 1 {
		t.Fatalf("retry must succeed after recovery: fire ok=%d failed=%d", fok, ffail)
	}
	if rfail != 0 || rok != 1 {
		t.Fatalf("resolve after recovery must succeed once: ok=%d fail=%d", rok, rfail)
	}
	if p0.Incidents()[0].NotifyCount != 1 {
		t.Fatal("successful delivery must count exactly once despite retries")
	}
}

func TestConcurrentIngestion(t *testing.T) {
	fs := sink.NewFakeSink()
	c := cfg(t.TempDir())
	c.QueueSize = 64
	p, _ := New(context.Background(), c, fs)
	ctx, cancel := context.WithCancel(context.Background())

	const writers, perWriter = 8, 40
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("w%d-%d", w, i)
				// alternate metric/log, unique ids
				kind := "m"
				if i%2 == 0 {
					kind = "l"
				}
				if _, err := p.Ingest(ctx, evt(id, kind, time.Unix(int64(w*1000+i), 0))); err != nil {
					t.Errorf("ingest %s: %v", id, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	cancel()
	if err := p.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := p.Stats()
	t.Logf("concurrent: accepted=%d duplicates=%d stored=%d, sink fires=%d",
		st.Accepted, st.Duplicates, st.Stored, len(fs.FireKeys()))
	if st.Accepted != writers*perWriter {
		t.Fatalf("lost data: accepted=%d want=%d", st.Accepted, writers*perWriter)
	}
	if st.Duplicates != 0 {
		t.Fatalf("unique ids produced duplicates: %d", st.Duplicates)
	}
	if st.Stored != writers*perWriter {
		t.Fatalf("stored=%d want=%d", st.Stored, writers*perWriter)
	}
	// One incident, and it fired exactly once for the whole burst.
	if len(p.Incidents()) != 1 || p.Incidents()[0].NotifyCount != 1 {
		t.Fatalf("incident=%+v", p.Incidents())
	}
}

func TestUnifiedQueryAcrossKinds(t *testing.T) {
	p, _ := New(context.Background(), cfg(t.TempDir()), nil)
	defer p.Close()
	ctx := context.Background()
	p.Ingest(ctx, evt("q1", "m", time.Unix(1, 0)))
	p.Ingest(ctx, evt("q2", "l", time.Unix(2, 0)))
	p.Ingest(ctx, evt("q3", "tr", time.Unix(3, 0)))
	if err := p.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	rows := p.Query(store.Query{TraceID: "tr-1"})
	kinds := map[model.EventKind]bool{}
	for _, r := range rows {
		kinds[r.Event.Kind] = true
		t.Logf("unified row id=%s kind=%s %s", r.Event.EventID, r.Event.Kind, r.Reason)
	}
	if len(rows) != 3 || !kinds[model.KindMetric] || !kinds[model.KindLog] || !kinds[model.KindTrace] {
		t.Fatalf("one trace must unify all three kinds: %+v", rows)
	}
}

// flakyFireSink fails the first failUntil SendFire calls and records each
// attempt on the fired channel.
type flakyFireSink struct {
	mu        sync.Mutex
	inner     sink.Sink
	failUntil int
	okN       int
	failN     int
	fired     chan struct{}
}

func (f *flakyFireSink) SendFire(inc *model.Incident) error {
	f.fired <- struct{}{}
	f.mu.Lock()
	fail := f.failUntil > 0
	if fail {
		f.failUntil--
	}
	f.mu.Unlock()
	if fail {
		f.failN++
		return sink.ErrDownstream
	}
	if err := f.inner.SendFire(inc); err != nil {
		return err
	}
	f.okN++
	return nil
}

func (f *flakyFireSink) SendResolve(inc *model.Incident) error {
	return f.inner.SendResolve(inc)
}
