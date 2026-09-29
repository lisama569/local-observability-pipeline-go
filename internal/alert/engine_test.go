package alert

import (
	"testing"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

var testRule = Rule{
	Name: "error-burst", Window: time.Minute, Threshold: 3,
	ResolveAfter: 2 * time.Minute, ReopenWindow: 5 * time.Minute,
}

func errEv(id, fp string, ts time.Time) model.Event {
	return model.Event{
		ID: id, Kind: model.KindLog, Source: "svc", Fingerprint: fp,
		Timestamp: ts, Severity: model.SeverityError,
	}
}

func logTransitions(t *testing.T, label string, trs []Transition) {
	t.Helper()
	for _, tr := range trs {
		t.Logf("[%s] %s -> %s rule=%s fp=%s reason=%q triggers=%v reopened=%v key=%s",
			label, tr.From, tr.To, tr.Rule, tr.Fingerprint, tr.Reason, tr.TriggerIDs, tr.Reopened, tr.Key)
	}
}

func TestFireSuppressResolveReopen(t *testing.T) {
	eng := New(testRule)
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	// 3 个 Error 事件达到阈值 → firing。
	var all []Transition
	for i, id := range []string{"a1", "a2", "a3"} {
		trs := eng.Observe(errEv(id, "fp-1", base.Add(time.Duration(i)*10*time.Second)))
		all = append(all, trs...)
	}
	logTransitions(t, "fire", all)
	if len(all) != 1 || all[0].To != StateFiring || all[0].Reopened {
		t.Fatalf("want single firing transition, got %+v", all)
	}
	if len(all[0].TriggerIDs) != 3 {
		t.Fatalf("trigger ids should list evidence, got %v", all[0].TriggerIDs)
	}

	// firing 期间更多 Error：被抑制，无新迁移。
	trs := eng.Observe(errEv("a4", "fp-1", base.Add(40*time.Second)))
	if len(trs) != 0 {
		t.Fatalf("firing state must suppress duplicates, got %+v", trs)
	}

	// 水位线推进超过 ResolveAfter 且无新触发 → resolved。
	trs = eng.Observe(errEv("b1", "fp-1", base.Add(3*time.Minute)))
	logTransitions(t, "resolve", trs)
	// 注意：b1 本身也是 Error，窗口内仍有 a4/b1 等，需推进更远。
	trs2 := eng.Observe(errEv("b2", "fp-1", base.Add(4*time.Minute)))
	logTransitions(t, "resolve2", trs2)
	snap := eng.Snapshot()
	t.Logf("snapshot=%v", snap)
	if snap["error-burst|fp-1"] != StateResolved {
		t.Fatalf("want resolved, got %v", snap)
	}

	// 重开窗口内再次达到阈值 → reopen 而非新告警。
	var reopen []Transition
	for i, id := range []string{"c1", "c2", "c3"} {
		trs := eng.Observe(errEv(id, "fp-1", base.Add(4*time.Minute+time.Duration(i+1)*10*time.Second)))
		reopen = append(reopen, trs...)
	}
	logTransitions(t, "reopen", reopen)
	if len(reopen) != 1 || !reopen[0].Reopened || reopen[0].From != StateResolved {
		t.Fatalf("want single reopen transition, got %+v", reopen)
	}
}

func TestOutOfOrderNoFlapping(t *testing.T) {
	eng := New(testRule)
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	// 乱序：先到的两个事件时间更晚。
	seq := []model.Event{
		errEv("x2", "fp-2", base.Add(20*time.Second)),
		errEv("x3", "fp-2", base.Add(30*time.Second)),
		errEv("x1", "fp-2", base.Add(10*time.Second)), // 迟到事件
	}
	var all []Transition
	for _, e := range seq {
		all = append(all, eng.Observe(e)...)
	}
	logTransitions(t, "out-of-order", all)
	// 窗口内最终 3 个 Error → 恰好一次 firing，不因乱序反复。
	fires := 0
	for _, tr := range all {
		if tr.To == StateFiring {
			fires++
		}
	}
	if fires != 1 {
		t.Fatalf("out-of-order caused flapping: %d firing transitions", fires)
	}

	// 迟到的旧事件在水位线之后到达，不得把已 resolved 的状态拉回。
	eng2 := New(testRule)
	for i, id := range []string{"y1", "y2", "y3"} {
		eng2.Observe(errEv(id, "fp-3", base.Add(time.Duration(i)*10*time.Second)))
	}
	eng2.Observe(errEv("y4", "fp-3", base.Add(5*time.Minute))) // 推进水位线 → resolved
	before := eng2.Snapshot()["error-burst|fp-3"]
	trs := eng2.Observe(errEv("y0", "fp-3", base.Add(5*time.Second))) // 很旧的迟到事件
	after := eng2.Snapshot()["error-burst|fp-3"]
	t.Logf("late event: before=%s after=%s transitions=%v", before, after, trs)
	if before != StateResolved || after != StateResolved {
		t.Fatalf("late event flapped state: %s -> %s", before, after)
	}
}

func TestReplayDeterministic(t *testing.T) {
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	events := []model.Event{
		errEv("r1", "fp-4", base),
		errEv("r2", "fp-4", base.Add(10*time.Second)),
		errEv("r3", "fp-4", base.Add(20*time.Second)),
		errEv("r4", "fp-4", base.Add(3*time.Minute)),
	}
	run := func() []Transition {
		eng := New(testRule)
		var out []Transition
		for _, e := range events {
			out = append(out, eng.Observe(e)...)
		}
		return out
	}
	first, second := run(), run()
	if len(first) != len(second) {
		t.Fatalf("replay not deterministic: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].Key != second[i].Key {
			t.Fatalf("transition key mismatch at %d: %s vs %s", i, first[i].Key, second[i].Key)
		}
	}
	t.Logf("deterministic transitions: %d, keys=%v", len(first), []string{first[0].Key})
}

func TestCrossSignalCorrelation(t *testing.T) {
	eng := New(testRule)
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	// 同一 fingerprint 的 metric/log/trace 三类事件聚合到同一条告警。
	mk := func(id string, k model.Kind, ts time.Time) model.Event {
		return model.Event{ID: id, Kind: k, Source: "svc", Fingerprint: "fp-x",
			Timestamp: ts, Severity: model.SeverityError}
	}
	var all []Transition
	all = append(all, eng.Observe(mk("m1", model.KindMetric, base))...)
	all = append(all, eng.Observe(mk("l1", model.KindLog, base.Add(5*time.Second)))...)
	all = append(all, eng.Observe(mk("t1", model.KindTrace, base.Add(9*time.Second)))...)
	logTransitions(t, "cross-signal", all)
	if len(all) != 1 || all[0].To != StateFiring {
		t.Fatalf("cross-signal correlation failed: %+v", all)
	}
	if len(all[0].TriggerIDs) != 3 {
		t.Fatalf("transition should carry evidence from all 3 signals: %v", all[0].TriggerIDs)
	}
}
