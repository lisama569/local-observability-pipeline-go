package model

import (
	"errors"
	"testing"
	"time"
)

func TestValidateDistinguishableErrors(t *testing.T) {
	base := Event{
		ID: "e1", Kind: KindLog, Source: "svc-a",
		Fingerprint: "fp-1", Timestamp: time.Now(),
	}
	cases := []struct {
		name   string
		mutate func(*Event)
		want   error
	}{
		{"missing id", func(e *Event) { e.ID = "" }, ErrMissingID},
		{"missing kind", func(e *Event) { e.Kind = "" }, ErrMissingKind},
		{"bad kind", func(e *Event) { e.Kind = "metricc" }, ErrMissingKind},
		{"missing source", func(e *Event) { e.Source = "" }, ErrMissingSource},
		{"missing fingerprint", func(e *Event) { e.Fingerprint = "" }, ErrMissingFingerprint},
		{"missing timestamp", func(e *Event) { e.Timestamp = time.Time{} }, ErrMissingTimestamp},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := base
			tc.mutate(&e)
			err := e.Validate()
			t.Logf("input=%+v want=%v got=%v", e, tc.want, err)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
	if err := base.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
}

func TestNormalizeDefaults(t *testing.T) {
	e := Event{ID: "t1", Kind: KindTrace, Source: "svc-b", Fingerprint: "fp-9", Timestamp: time.Now()}
	defaulted := e.Normalize()
	t.Logf("defaulted=%v trace_id=%q", defaulted, e.TraceID)
	if e.Attributes == nil {
		t.Fatal("attributes not defaulted")
	}
	if e.TraceID != "t1" {
		t.Fatalf("trace id not defaulted to event id: %q", e.TraceID)
	}
	if len(defaulted) != 2 {
		t.Fatalf("want 2 defaulted fields, got %v", defaulted)
	}
	// 再次归一化应为空（幂等）。
	if d := e.Normalize(); len(d) != 0 {
		t.Fatalf("normalize not idempotent: %v", d)
	}
}

func TestDedupKeyScopedBySource(t *testing.T) {
	a := Event{ID: "x", Source: "s1"}
	b := Event{ID: "x", Source: "s2"}
	if a.DedupKey() == b.DedupKey() {
		t.Fatal("dedup key must include source")
	}
}
