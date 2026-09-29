// Package model defines the unified observability event model and alert
// (incident) state model used across the local pipeline.
package model

import (
	"sort"
	"time"
)

// EventKind identifies one of the three telemetry sources.
type EventKind string

const (
	KindMetric EventKind = "metric"
	KindLog    EventKind = "log"
	KindTrace  EventKind = "trace"
)

// Severity is the normalized log severity.
type Severity string

const (
	SeverityDebug Severity = "debug"
	SeverityInfo  Severity = "info"
	SeverityWarn  Severity = "warn"
	SeverityError Severity = "error"
	SeverityFatal Severity = "fatal"
)

// IncidentState is the lifecycle state of an alert/incident.
type IncidentState string

const (
	// StatePending has collected evidence but has not met the firing rule.
	StatePending IncidentState = "pending"
	// StateFiring is an active alert that has been (or should be) notified.
	StateFiring IncidentState = "firing"
	// StateSuppressed is an active alert silenced by a suppression rule.
	StateSuppressed IncidentState = "suppressed"
	// StateResolved is an alert that received a recovery signal.
	StateResolved IncidentState = "resolved"
)

// Event is the normalized representation of a metric, log or trace record.
type Event struct {
	// EventID is the caller-provided globally unique id. It is required and
	// is the primary dedup key: a redelivery with the same EventID is
	// accepted once and processed at most once.
	EventID string    `json:"event_id"`
	Kind    EventKind `json:"kind"`

	// Source identifies the reporting instance (host/pod/process).
	Source string `json:"source"`
	// Service is the entity the incident is aggregated on.
	Service string `json:"service"`
	// Fingerprint normalizes the fault class (e.g. error type, rule name).
	Fingerprint string `json:"fingerprint"`
	// TraceID links metric/log/trace events belonging to the same request.
	TraceID string `json:"trace_id,omitempty"`

	// Timestamp is the event time assigned by the source. Events may
	// arrive out of order; processing is safe against late arrivals.
	Timestamp time.Time `json:"timestamp"`

	// Metric fields. Fault=true means a fault observation (e.g. error rate
	// high); Fault=false is a recovery observation (value back to normal).
	MetricName string  `json:"metric_name,omitempty"`
	Value      float64 `json:"value,omitempty"`
	Fault      bool    `json:"fault,omitempty"`

	// Log fields.
	Severity Severity `json:"severity,omitempty"`
	Message  string   `json:"message,omitempty"`

	// Trace fields.
	SpanName   string `json:"span_name,omitempty"`
	IsError    bool   `json:"is_error,omitempty"`
	DurationMs int64  `json:"duration_ms,omitempty"`

	// Resolved, when set on any kind, is an explicit recovery signal.
	Resolved bool `json:"resolved,omitempty"`

	// Suppressed marks an event whose source is under a maintenance /
	// suppression window.
	Suppressed bool `json:"suppressed,omitempty"`
}

// KindSet is a set of event kinds.
type KindSet map[EventKind]struct{}

// Add inserts a kind and reports whether it was newly added.
func (s KindSet) Add(k EventKind) bool {
	if _, ok := s[k]; ok {
		return false
	}
	s[k] = struct{}{}
	return true
}

func (s KindSet) Has(k EventKind) bool {
	_, ok := s[k]
	return ok
}

func (s KindSet) Len() int { return len(s) }

// Incident is the single trackable alert record that all related
// metric/log/trace events are correlated onto.
type Incident struct {
	Key         string        `json:"key"` // service|fingerprint correlation key
	Service     string        `json:"service"`
	Fingerprint string        `json:"fingerprint"`
	State       IncidentState `json:"state"`

	TraceIDs map[string]struct{} `json:"-"`

	EventIDs    []string `json:"event_ids"`
	SignalKinds KindSet  `json:"-"` // distinct fault-signal kinds observed
	// KindTimes records the newest timestamp at which each kind produced a
	// fault signal, used to evaluate the firing time window.
	KindTimes map[EventKind]time.Time `json:"-"`

	OpenedAt    time.Time `json:"opened_at"`
	LastEventAt time.Time `json:"last_event_at"`
	ResolvedAt  time.Time `json:"resolved_at,omitempty"`
	FiredAt     time.Time `json:"fired_at,omitempty"`

	NotifyCount int  `json:"notify_count"`
	ReopenCount int  `json:"reopen_count"`
	Suppressed  bool `json:"suppressed"`

	// Reason records the human-readable decision basis of the latest
	// transition (used in test logs and explainability output).
	Reason string `json:"reason"`
}

// ValidationError describes why an event was rejected. Distinct messages
// allow tests to differentiate failure causes.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return "validation error: " + e.Msg }

// Validate checks required fields. Each failure has a distinct, stable
// reason string so callers (and test logs) can tell rejection causes
// apart. Returns a *ValidationError on failure.
func (e *Event) Validate() error {
	switch {
	case e.EventID == "":
		return &ValidationError{Msg: "missing event_id"}
	case e.Kind != KindMetric && e.Kind != KindLog && e.Kind != KindTrace:
		return &ValidationError{Msg: "unknown kind: " + string(e.Kind)}
	case e.Source == "":
		return &ValidationError{Msg: "missing source"}
	case e.Service == "":
		return &ValidationError{Msg: "missing service"}
	case e.Fingerprint == "":
		return &ValidationError{Msg: "missing fingerprint"}
	case e.Timestamp.IsZero():
		return &ValidationError{Msg: "missing timestamp"}
	case e.Kind == KindLog && e.Message == "":
		return &ValidationError{Msg: "log event missing message"}
	case e.Kind == KindMetric && e.MetricName == "":
		return &ValidationError{Msg: "metric event missing metric_name"}
	case e.Kind == KindTrace && e.SpanName == "":
		return &ValidationError{Msg: "trace event missing span_name"}
	}
	return nil
}

// IsFaultSignal reports whether the event contributes fault evidence
// (rather than being a recovery heartbeat).
func (e *Event) IsFaultSignal() bool {
	if e.Resolved {
		return false
	}
	switch e.Kind {
	case KindMetric:
		return e.Fault
	case KindLog:
		return e.Severity == SeverityError || e.Severity == SeverityFatal
	case KindTrace:
		return e.IsError
	}
	return false
}

// TraceIDList returns the trace ids as a sorted, deterministic slice.
func (in *Incident) TraceIDList() []string {
	out := make([]string, 0, len(in.TraceIDs))
	for id := range in.TraceIDs {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// KindList returns the observed fault-signal kinds in a fixed order.
func (in *Incident) KindList() []EventKind {
	var out []EventKind
	for _, k := range []EventKind{KindMetric, KindLog, KindTrace} {
		if in.SignalKinds.Has(k) {
			out = append(out, k)
		}
	}
	return out
}
