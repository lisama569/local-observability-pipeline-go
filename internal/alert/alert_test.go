package alert

import (
	"errors"
	"testing"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

type recNotifier struct {
	fires    []string
	resolves []string
	failFire bool
}

func (n *recNotifier) Notify(i *model.Incident) error {
	if n.failFire {
		return errors.New("downstream boom")
	}
	n.fires = append(n.fires, i.Key)
	return nil
}
func (n *recNotifier) Resolve(i *model.Incident) error {
	n.resolves = append(n.resolves, i.Key)
	return nil
}

func ev(id, kind string, t time.Time) model.Event {
	e := model.Event{
		EventID: id, Source: "h", Service: "svc", Fingerprint: "fp",
		Timestamp: t,
	}
	switch kind {
	case "metric":
		e.Kind, e.MetricName, e.Fault = model.KindMetric, "err", true
	case "log":
		e.Kind, e.Severity, e.Message = model.KindLog, model.SeverityError, "boom"
	case "trace":
		e.Kind, e.SpanName, e.IsError = model.KindTrace, "span", true
	case "resolve":
		e.Kind, e.MetricName, e.Fault, e.Resolved = model.KindMetric, "err", false, true
	}
	return e
}

func TestAggregateFireOnceResolveReopen(t *testing.T) {
	n := &recNotifier{}
	en := NewEngine(Rules{FireAfterKinds: 2}, n)
	t0 := time.Unix(100, 0)

	steps := []struct {
		name string
		e    model.Event
		want model.IncidentState
	}{
		{"single signal pending", ev("e1", "metric", t0), model.StatePending},
		{"second kind fires", ev("e2", "log", t0.Add(time.Second)), model.StateFiring},
		{"third kind no renotify", ev("e3", "trace", t0.Add(2*time.Second)), model.StateFiring},
		{"repeat same kind", ev("e4", "log", t0.Add(3*time.Second)), model.StateFiring},
		{"resolve", ev("e5", "resolve", t0.Add(4*time.Second)), model.StateResolved},
		{"reopen metric pending", ev("e6", "metric", t0.Add(5*time.Second)), model.StatePending},
		{"refire log", ev("e7", "log", t0.Add(6*time.Second)), model.StateFiring},
	}
	for _, st := range steps {
		out := en.Apply(st.e)
		t.Logf("input=%-22s event_id=%s -> state=%s notified=%v basis=%q",
			st.name, st.e.EventID, out.State, out.Notified, out.Reason)
		if out.State != st.want {
			t.Fatalf("%s: state=%s want=%s", st.name, out.State, st.want)
		}
	}
	inc, _ := en.Get("svc|fp")
	// Exactly two fire notifications for two lifecycles, same trackable incident.
	if len(n.fires) != 2 || inc.NotifyCount != 2 {
		t.Fatalf("fires=%v notifyCount=%d", n.fires, inc.NotifyCount)
	}
	if len(n.resolves) != 1 {
		t.Fatalf("resolves=%v", n.resolves)
	}
	if inc.ReopenCount != 1 {
		t.Fatalf("reopenCount=%d", inc.ReopenCount)
	}
	if inc.SignalKinds.Len() != 2 {
		t.Fatalf("after reopen only the new lifecycle kinds count, got %d", inc.SignalKinds.Len())
	}
	if len(inc.EventIDs) != 7 {
		t.Fatalf("all distinct events stay correlated, got %d", len(inc.EventIDs))
	}
}

func TestReplayIdempotent(t *testing.T) {
	n := &recNotifier{}
	en := NewEngine(Rules{FireAfterKinds: 1}, n)
	t0 := time.Unix(100, 0)
	e1 := ev("e1", "metric", t0)
	e2 := ev("e2", "resolve", t0.Add(time.Second))
	en.Apply(e1)
	en.Apply(e2)
	// Simulated WAL replay of the same events after restart.
	out1 := en.Apply(e1)
	out2 := en.Apply(e2)
	t.Logf("replay e1 -> %s | %s", out1.State, out1.Reason)
	t.Logf("replay e2 -> %s | %s", out2.State, out2.Reason)
	if out1.State != model.StateResolved || out2.State != model.StateResolved {
		t.Fatal("replayed events must not change resolved state")
	}
	if len(n.fires) != 1 || len(n.resolves) != 1 {
		t.Fatalf("replay must not double-notify: fires=%v resolves=%v", n.fires, n.resolves)
	}
}

func TestLateResolveDoesNotFlap(t *testing.T) {
	n := &recNotifier{}
	en := NewEngine(Rules{FireAfterKinds: 1}, n)
	t0 := time.Unix(100, 0)
	en.Apply(ev("e1", "metric", t0.Add(10*time.Second))) // fires
	late := ev("old-resolve", "resolve", t0)             // older signal
	out := en.Apply(late)
	t.Logf("late resolve -> state=%s basis=%q", out.State, out.Reason)
	if out.State != model.StateFiring {
		t.Fatal("a resolve older than the newest event must not flap firing state")
	}
	if len(n.resolves) != 0 {
		t.Fatal("late resolve must not notify")
	}
}

func TestDownstreamFailureKeepsPending(t *testing.T) {
	n := &recNotifier{failFire: true}
	en := NewEngine(Rules{FireAfterKinds: 1}, n)
	out := en.Apply(ev("e1", "metric", time.Unix(1, 0)))
	t.Logf("downstream down -> state=%s basis=%q", out.State, out.Reason)
	if out.State != model.StatePending {
		t.Fatal("notification failure must leave incident pending for retry, not firing")
	}
	n.failFire = false
	out = en.Apply(ev("e2", "log", time.Unix(2, 0)))
	if out.State != model.StateFiring || !out.Notified {
		t.Fatalf("after recovery it should fire, state=%s notified=%v", out.State, out.Notified)
	}
	if len(n.fires) != 1 {
		t.Fatalf("fires=%v", n.fires)
	}
}

func TestSuppression(t *testing.T) {
	n := &recNotifier{}
	en := NewEngine(Rules{FireAfterKinds: 2}, n)
	t0 := time.Unix(100, 0)
	maintenance := ev("sup", "metric", t0)
	maintenance.Suppressed = true
	maintenance.Fault = false // benign suppressed marker
	en.Apply(maintenance)
	en.Apply(ev("e1", "metric", t0.Add(time.Second)))
	out := en.Apply(ev("e2", "log", t0.Add(2*time.Second)))
	t.Logf("suppressed fire -> state=%s basis=%q", out.State, out.Reason)
	if out.State != model.StateSuppressed {
		t.Fatalf("want suppressed, got %s", out.State)
	}
	if len(n.fires) != 0 {
		t.Fatal("suppressed incident must not notify")
	}
}
