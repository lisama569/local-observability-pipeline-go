// Package store provides a bounded, queryable in-memory view of accepted
// events. Insertion order (and therefore out-of-order timestamps) is
// handled explicitly: query results are deterministically sorted by
// (timestamp, event_id), so repeated queries over the same inputs always
// return the same rows in the same order.
//
// Memory is bounded: when full the oldest accepted event is evicted.
// Durable duplicate recognition across eviction/restart lives in the WAL
// replay and the pipeline's persistent seen-id file, not here.
package store

import (
	"sort"
	"sync"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

// Store is a bounded unified query surface over the three event kinds.
type Store struct {
	mu       sync.Mutex
	capacity int
	events   []model.Event
	seen     map[string]struct{}
}

// New creates a store retaining at most capacity events.
func New(capacity int) *Store {
	if capacity <= 0 {
		capacity = 1
	}
	return &Store{
		capacity: capacity,
		events:   make([]model.Event, 0, capacity),
		seen:     make(map[string]struct{}, capacity),
	}
}

// Insert adds an already-accepted event. A duplicate EventID is ignored and
// reports false; no data is silently overwritten. Returns true if stored.
func (s *Store) Insert(e model.Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[e.EventID]; ok {
		return false
	}
	if len(s.events) >= s.capacity {
		// Evict the oldest retained event to bound memory.
		evicted := s.events[0]
		s.events = s.events[1:]
		delete(s.seen, evicted.EventID)
	}
	s.events = append(s.events, e)
	s.seen[e.EventID] = struct{}{}
	return true
}

// Has reports whether an EventID is currently retained.
func (s *Store) Has(eventID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.seen[eventID]
	return ok
}

// Query filters events.
type Query struct {
	Kinds       []model.EventKind
	Service     string
	TraceID     string
	Source      string
	Fingerprint string
	From        time.Time
	To          time.Time
}

// Result is one query row with the reason it is returned.
type Result struct {
	Event  model.Event
	Reason string
}

// Query returns matching rows deterministically ordered by (timestamp, event_id),
// independent of arrival order. Result.Reason records which clauses matched.
func (s *Store) Query(q Query) []Result {
	s.mu.Lock()
	kindSet := make(map[model.EventKind]struct{}, len(q.Kinds))
	for _, k := range q.Kinds {
		kindSet[k] = struct{}{}
	}
	var out []Result
	for _, e := range s.events {
		reason, ok := matchEvent(e, q, kindSet)
		if ok {
			out = append(out, Result{Event: e, Reason: reason})
		}
	}
	s.mu.Unlock()

	sort.SliceStable(out, func(i, j int) bool {
		ti, tj := out[i].Event.Timestamp, out[j].Event.Timestamp
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return out[i].Event.EventID < out[j].Event.EventID
	})
	return out
}

func matchEvent(e model.Event, q Query, kinds map[model.EventKind]struct{}) (string, bool) {
	var reasons []string
	if len(kinds) > 0 {
		if _, ok := kinds[e.Kind]; !ok {
			return "", false
		}
		reasons = append(reasons, "kind="+string(e.Kind))
	}
	if q.Service != "" {
		if e.Service != q.Service {
			return "", false
		}
		reasons = append(reasons, "service="+q.Service)
	}
	if q.Source != "" {
		if e.Source != q.Source {
			return "", false
		}
		reasons = append(reasons, "source="+q.Source)
	}
	if q.Fingerprint != "" {
		if e.Fingerprint != q.Fingerprint {
			return "", false
		}
		reasons = append(reasons, "fingerprint="+q.Fingerprint)
	}
	if q.TraceID != "" {
		if e.TraceID != q.TraceID {
			return "", false
		}
		reasons = append(reasons, "trace_id="+q.TraceID)
	}
	if !q.From.IsZero() {
		if e.Timestamp.Before(q.From) {
			return "", false
		}
		reasons = append(reasons, "from>="+q.From.Format(time.RFC3339Nano))
	}
	if !q.To.IsZero() {
		if e.Timestamp.After(q.To) {
			return "", false
		}
		reasons = append(reasons, "to<="+q.To.Format(time.RFC3339Nano))
	}
	return joinReasons(reasons), true
}

func joinReasons(rs []string) string {
	out := "matched:"
	for i, r := range rs {
		if i > 0 {
			out += ","
		}
		out += r
	}
	return out
}

// Len returns the number of retained events.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.events)
}

// All returns a copy of retained events in deterministic order.
func (s *Store) All() []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Event, len(s.events))
	copy(out, s.events)
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].Timestamp.Equal(out[j].Timestamp) {
			return out[i].Timestamp.Before(out[j].Timestamp)
		}
		return out[i].EventID < out[j].EventID
	})
	return out
}
