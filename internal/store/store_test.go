package store

import (
	"testing"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

func evt(id string, kind model.EventKind, ts time.Time) model.Event {
	return model.Event{
		EventID: id, Kind: kind, Source: "h", Service: "svc",
		Fingerprint: "fp", TraceID: "tr1", Timestamp: ts,
		MetricName: "m", Message: "m", SpanName: "sp",
	}
}

func TestDuplicateIgnoredAndBounded(t *testing.T) {
	s := New(2)
	if !s.Insert(evt("a", model.KindLog, time.Unix(1, 0))) {
		t.Fatal("a should insert")
	}
	if s.Insert(evt("a", model.KindLog, time.Unix(1, 0))) {
		t.Fatal("duplicate EventID must be rejected, not counted twice")
	}
	s.Insert(evt("b", model.KindMetric, time.Unix(2, 0)))
	s.Insert(evt("c", model.KindTrace, time.Unix(3, 0))) // evicts a
	if s.Len() != 2 {
		t.Fatalf("len=%d must be bounded at 2", s.Len())
	}
	if s.Has("a") {
		t.Fatal("oldest should be evicted")
	}
	if !s.Has("b") || !s.Has("c") {
		t.Fatal("b,c should be retained")
	}
}

func TestQueryOrderIndependentOfArrival(t *testing.T) {
	s := New(10)
	// Arrive out of timestamp order.
	s.Insert(evt("late", model.KindLog, time.Unix(30, 0)))
	s.Insert(evt("early", model.KindMetric, time.Unix(10, 0)))
	s.Insert(evt("mid", model.KindTrace, time.Unix(20, 0)))

	rows := s.Query(Query{Service: "svc"})
	got := []string{rows[0].Event.EventID, rows[1].Event.EventID, rows[2].Event.EventID}
	want := []string{"early", "mid", "late"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("deterministic time-order expected %v got %v", want, got)
		}
	}
	t.Logf("decision basis: order by (timestamp,event_id), out-of-order arrival ids=%v", got)

	// Kind filter.
	logs := s.Query(Query{Kinds: []model.EventKind{model.KindLog}})
	if len(logs) != 1 || logs[0].Event.EventID != "late" {
		t.Fatalf("kind filter failed: %+v", logs)
	}
	tr := s.Query(Query{TraceID: "tr1"})
	if len(tr) != 3 {
		t.Fatalf("trace correlation expected 3, got %d", len(tr))
	}
}
