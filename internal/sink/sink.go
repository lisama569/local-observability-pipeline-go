// Package sink defines the downstream notification interface and a controllable
// local fake used to simulate slowness and transient failures without any
// external service or real credentials.
package sink

import (
	"errors"
	"sync"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

// Sink receives fire/resolve notifications.
type Sink interface {
	SendFire(inc *model.Incident) error
	SendResolve(inc *model.Incident) error
}

// ErrDownstream is returned for scripted transient failures so tests can
// distinguish downstream failure from validation/backpressure errors.
var ErrDownstream = errors.New("downstream temporarily unavailable")

// call records one attempted delivery.
type call struct {
	key string
	at  time.Time
	ok  bool
}

// FakeSink is an in-memory sink with scripted failures and latency.
type FakeSink struct {
	mu        sync.Mutex
	failLeft  int
	latency   time.Duration
	fires     []call
	resolves  []call
	failFatal bool
}

// NewFakeSink creates an available sink.
func NewFakeSink() *FakeSink { return &FakeSink{} }

// FailNext arranges the next n Send calls to fail with ErrDownstream.
func (s *FakeSink) FailNext(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failLeft += n
}

// SetLatency makes every Send block for d (simulates a slow downstream).
func (s *FakeSink) SetLatency(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latency = d
}

// Recover clears scripted failures.
func (s *FakeSink) Recover() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failLeft = 0
}

func (s *FakeSink) send(key string) error {
	s.mu.Lock()
	lat := s.latency
	fail := s.failLeft > 0
	if fail {
		s.failLeft--
	}
	s.mu.Unlock()

	if lat > 0 {
		time.Sleep(lat)
	}
	if fail {
		s.record(false, key, false)
		return ErrDownstream
	}
	s.record(true, key, true)
	return nil
}

func (s *FakeSink) record(ok bool, key string, fire bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := call{key: key, at: time.Now().UTC(), ok: ok}
	if fire {
		s.fires = append(s.fires, c)
	} else {
		s.resolves = append(s.resolves, c)
	}
}

// SendFire delivers a firing notification.
func (s *FakeSink) SendFire(inc *model.Incident) error {
	return s.send(inc.Key)
}

// SendResolve delivers a resolution notification.
func (s *FakeSink) SendResolve(inc *model.Incident) error {
	// record() hardcodes fire=true in send(); use a dedicated branch.
	s.mu.Lock()
	lat := s.latency
	fail := s.failLeft > 0
	if fail {
		s.failLeft--
	}
	s.mu.Unlock()
	if lat > 0 {
		time.Sleep(lat)
	}
	s.mu.Lock()
	s.resolves = append(s.resolves, call{key: inc.Key, at: time.Now().UTC(), ok: !fail})
	s.mu.Unlock()
	if fail {
		return ErrDownstream
	}
	return nil
}

// FireKeys returns keys of successfully delivered fires, in delivery order.
func (s *FakeSink) FireKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, c := range s.fires {
		if c.ok {
			out = append(out, c.key)
		}
	}
	return out
}

// ResolveKeys returns keys of successfully delivered resolves.
func (s *FakeSink) ResolveKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, c := range s.resolves {
		if c.ok {
			out = append(out, c.key)
		}
	}
	return out
}

// AttemptCounts returns total (success, failure) attempt counts.
func (s *FakeSink) AttemptCounts() (fireOK, fireFail, resolveOK, resolveFail int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.fires {
		if c.ok {
			fireOK++
		} else {
			fireFail++
		}
	}
	for _, c := range s.resolves {
		if c.ok {
			resolveOK++
		} else {
			resolveFail++
		}
	}
	return
}

// Notifier adapts a Sink to the alert.Notifier interface.
type Notifier struct{ Sink Sink }

// Notify forwards a firing notification.
func (n Notifier) Notify(inc *model.Incident) error { return n.Sink.SendFire(inc) }

// Resolve forwards a resolution notification.
func (n Notifier) Resolve(inc *model.Incident) error { return n.Sink.SendResolve(inc) }
