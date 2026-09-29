package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/alert"
	"github.com/lisama569/local-observability-pipeline-go/internal/model"
	"github.com/lisama569/local-observability-pipeline-go/internal/sink"
	"github.com/lisama569/local-observability-pipeline-go/internal/store"
)

var rule = alert.Rule{
	Name: "error-burst", Window: time.Minute, Threshold: 3,
	ResolveAfter: 2 * time.Minute, ReopenWindow: 5 * time.Minute,
}

func errEvent(id, fp string, ts time.Time) model.Event {
	return model.Event{
		ID: id, Kind: model.KindLog, Source: "svc", Fingerprint: fp,
		Timestamp: ts, Severity: model.SeverityError, Message: "boom",
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestDuplicateDeliveryNotCounted(t *testing.T) {
	dir := t.TempDir()
	ms := &sink.MemSink{}
	p, err := Open(Config{
		Store: store.Options{Dir: dir, SyncOnWrite: true},
		Rules: []alert.Rule{rule},
	}, ms)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	// 同一事件重复投递 3 次：只计 1 次，阈值不被重复数据触发。
	for i := 0; i < 3; i++ {
		res, err := p.Ingest(ctx, errEvent("dup-1", "fp-d", base))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("ingest dup-1 attempt %d: duplicate=%v seq=%d", i, res.Duplicate, res.Seq)
	}
	st := p.Stats()
	if st.Accepted != 1 || st.Duplicates != 2 {
		t.Fatalf("duplicate miscounted: %+v", st)
	}
	if got := p.Alerts()["error-burst|fp-d"]; got == alert.StateFiring {
		t.Fatal("duplicate deliveries must not trigger alert")
	}
}

func TestConcurrentIngest(t *testing.T) {
	dir := t.TempDir()
	ms := &sink.MemSink{}
	p, _ := Open(Config{
		Store: store.Options{Dir: dir, SyncOnWrite: true},
		Rules: []alert.Rule{rule},
	}, ms)
	defer p.Close()
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := p.Ingest(ctx, errEvent(fmt.Sprintf("c-%d", i), "fp-c",
				base.Add(time.Duration(i%50)*time.Second)))
			if err != nil {
				t.Errorf("ingest %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	st := p.Stats()
	t.Logf("concurrent ingest stats=%+v", st)
	if st.Accepted != n {
		t.Fatalf("want %d accepted, got %+v", n, st)
	}
	// 告警只触发一次（其余被抑制）。
	waitFor(t, "notification", func() bool { return ms.SentCount() >= 1 })
	fires := 0
	for _, tr := range ms.Sent {
		if tr.To == alert.StateFiring {
			fires++
		}
	}
	t.Logf("firing notifications=%d", fires)
	if fires != 1 {
		t.Fatalf("concurrent ingest produced %d firing notifications, want 1", fires)
	}
}

func TestRestartNoDuplicateNotification(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	// 第一次运行：触发告警并确认。
	ms1 := &sink.MemSink{}
	p1, _ := Open(Config{
		Store: store.Options{Dir: dir, SyncOnWrite: true},
		Rules: []alert.Rule{rule},
	}, ms1)
	for i, id := range []string{"k1", "k2", "k3"} {
		if _, err := p1.Ingest(ctx, errEvent(id, "fp-r", base.Add(time.Duration(i)*10*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "first notification", func() bool { return ms1.SentCount() == 1 })
	t.Logf("before restart: notified=%d key=%s", ms1.SentCount(), ms1.Sent[0].Key)
	p1.Close()

	// 重启：重放得到相同迁移，但已确认，不得重复通知。
	ms2 := &sink.MemSink{}
	p2, err := Open(Config{
		Store: store.Options{Dir: dir, SyncOnWrite: true},
		Rules: []alert.Rule{rule},
	}, ms2)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()
	// 给重放补发留出时间窗口，确认没有重发。
	time.Sleep(200 * time.Millisecond)
	t.Logf("after restart: notified=%d", ms2.SentCount())
	if ms2.SentCount() != 0 {
		t.Fatalf("restart re-notified acked transition: %+v", ms2.Sent)
	}
	// 状态恢复正确。
	if got := p2.Alerts()["error-burst|fp-r"]; got != alert.StateFiring {
		t.Fatalf("alert state lost after restart: %v", got)
	}
	// 重启后重复投递旧事件仍被去重。
	res, err := p2.Ingest(ctx, errEvent("k1", "fp-r", base))
	if err != nil || !res.Duplicate {
		t.Fatalf("dedup lost after restart: res=%+v err=%v", res, err)
	}
}

func TestDownstreamFailureAndRecovery(t *testing.T) {
	dir := t.TempDir()
	ms := &sink.MemSink{FailNext: 3} // 下游先失败 3 次
	p, _ := Open(Config{
		Store: store.Options{Dir: dir, SyncOnWrite: true},
		Rules: []alert.Rule{rule},
		Sink:  sink.Options{RetryBackoff: 5 * time.Millisecond},
	}, ms)
	defer p.Close()
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	for i, id := range []string{"f1", "f2", "f3"} {
		if _, err := p.Ingest(ctx, errEvent(id, "fp-f", base.Add(time.Duration(i)*10*time.Second))); err != nil {
			t.Fatal(err)
		}
	}
	// 下游恢复后，通知最终送达且只送达一次。
	waitFor(t, "delivery after retries", func() bool { return ms.SentCount() == 1 })
	t.Logf("delivered after injected failures: key=%s reason=%q", ms.Sent[0].Key, ms.Sent[0].Reason)
}

func TestBackpressureBoundedMemory(t *testing.T) {
	dir := t.TempDir()
	// 下游极慢 + 极小队列：Ingest 必须在 ctx 超时后以可区分错误返回，
	// 而不是无限缓冲。
	ms := &sink.MemSink{Delay: 200 * time.Millisecond}
	p, _ := Open(Config{
		Store: store.Options{Dir: dir, SyncOnWrite: true},
		Rules: []alert.Rule{rule},
		Sink:  sink.Options{QueueCapacity: 1, RetryBackoff: time.Millisecond},
	}, ms)
	defer p.Close()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	// 制造多个不同 fingerprint 的告警，快速填满队列。
	var lastErr error
	for g := 0; g < 5; g++ {
		fp := fmt.Sprintf("fp-bp-%d", g)
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := p.Ingest(ctx, errEvent(fmt.Sprintf("bp-%d-%d", g, i), fp,
				base.Add(time.Duration(i)*time.Second)))
			cancel()
			if err != nil {
				lastErr = err
			}
		}
	}
	t.Logf("backpressure error: %v", lastErr)
	if lastErr == nil {
		t.Fatal("expected backpressure error when downstream is slow")
	}
	// 已接受的事件不丢失：都在存储里可查。
	st := p.Stats()
	t.Logf("stats under backpressure: %+v", st)
	if st.Accepted == 0 {
		t.Fatal("accepted events lost under backpressure")
	}
}
