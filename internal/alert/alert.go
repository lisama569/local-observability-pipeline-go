// Package alert implements the incident state machine.
//
// All metric/log/trace events for the same fault (service + fingerprint)
// are aggregated onto ONE trackable Incident. Firing requires N distinct
// fault-signal *kinds* within the firing window; repeat deliveries or
// repeat same-kind signals never count twice. Late (out-of-order) events
// are handled by a watermark: an event older than the newest one cannot
// undo a resolve or re-fire a resolved incident on its own.
//
// States: pending -> firing -> resolved, with suppressed as a sticky flag
// on firing. Transitions are idempotent, so WAL replay, restart and
// redelivery produce the same final state and each downstream Notify/Resolve
// is issued at most once per incident lifecycle.
package alert

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

// Rules configure the firing condition.
type Rules struct {
	// FireAfterKinds is the number of distinct fault-signal kinds required
	// to fire (clamped to 1..3). Repeated same-kind signals never
	// double count.
	FireAfterKinds int
	// FireWindow: only signals within this duration before the newest signal
	// count toward firing. Zero means unbounded.
	FireWindow time.Duration
}

// DefaultRules requires two distinct signal kinds (e.g. metric + log).
func DefaultRules() Rules { return Rules{FireAfterKinds: 2} }

// Notifier is the downstream side-effect interface.
type Notifier interface {
	Notify(inc *model.Incident) error
	Resolve(inc *model.Incident) error
}

// Engine owns all incidents keyed by service|fingerprint.
type Engine struct {
	mu    sync.Mutex
	rules Rules
	notif Notifier
	items map[string]*model.Incident
	order []string // deterministic key order
}

// NewEngine creates an engine.
func NewEngine(r Rules, n Notifier) *Engine {
	if r.FireAfterKinds < 1 {
		r.FireAfterKinds = 1
	}
	if r.FireAfterKinds > 3 {
		r.FireAfterKinds = 3
	}
	return &Engine{rules: r, notif: n, items: map[string]*model.Incident{}}
}

// CorrelationKey is the stable identity of a fault.
func CorrelationKey(e model.Event) string {
	return e.Service + "|" + e.Fingerprint
}

// Outcome describes what the engine did with one event.
type Outcome struct {
	Key      string
	State    model.IncidentState
	Incident *model.Incident
	Notified bool
	Resolved bool
	Reason   string
}

// Apply processes one accepted (already deduplicated) event idempotently.
// EventID-level dedup is performed in the pipeline; here, applying the
// same event content again (e.g. replay) must not change conclusions.
func (en *Engine) Apply(e model.Event) Outcome {
	en.mu.Lock()
	defer en.mu.Unlock()

	key := CorrelationKey(e)
	inc, exists := en.items[key]
	now := e.Timestamp

	if !exists {
		inc = &model.Incident{
			Key:         key,
			Service:     e.Service,
			Fingerprint: e.Fingerprint,
			State:       model.StatePending,
			TraceIDs:    map[string]struct{}{},
			SignalKinds: model.KindSet{},
			KindTimes:   map[model.EventKind]time.Time{},
			OpenedAt:    now,
			LastEventAt: now,
		}
		en.items[key] = inc
		en.order = append(en.order, key)
	}

	// Idempotency against WAL replay / reprocessing: if this exact event
	// is already attached to the incident, return current state unchanged.
	for _, id := range inc.EventIDs {
		if id == e.EventID {
			return Outcome{
				Key: key, State: inc.State, Incident: inc,
				Reason: "event already aggregated (idempotent replay)",
			}
		}
	}

	// Attach evidence.
	inc.EventIDs = append(inc.EventIDs, e.EventID)
	if e.TraceID != "" {
		inc.TraceIDs[e.TraceID] = struct{}{}
	}
	sort.Strings(inc.EventIDs)

	late := now.Before(inc.LastEventAt)
	if now.After(inc.LastEventAt) {
		inc.LastEventAt = now
	}

	out := Outcome{Key: key, Incident: inc}

	switch {
	case e.Resolved:
		// A recovery signal resolves. A late (older than newest) resolve
		// is recorded as evidence but cannot override a newer state
		// flip (prevents flapping under out-of-order delivery).
		if inc.State == model.StateResolved {
			out.State, out.Reason = inc.State, "already resolved; resolve signal ignored"
			break
		}
		if late && (inc.State == model.StateFiring || inc.State == model.StateSuppressed) {
			out.State, out.Reason = inc.State,
				"late resolve older than newest event; state held to avoid flap"
			break
		}
		inc.State = model.StateResolved
		inc.ResolvedAt = now
		inc.Reason = "resolved by recovery signal event=" + e.EventID
		if err := en.notif.Resolve(inc); err != nil {
			// Notification failure must not silently corrupt state; the
			// caller (pipeline) retries. Surface the error in reason
			// but keep the state transition durable.
			inc.Reason += "; resolve notification failed: " + err.Error()
		} else {
			out.Resolved = true
		}
		out.State = inc.State
		out.Reason = inc.Reason

	case e.IsFaultSignal():
		out = en.applyFault(inc, e, late, out)

	default:
		// Benign observation (healthy metric / info log / ok span):
		// attach evidence, no state change.
		out.State = inc.State
		out.Reason = "benign observation, no state change"
	}

	en.applySuppression(inc, e)
	out.State = inc.State
	return out
}

func (en *Engine) applyFault(inc *model.Incident, e model.Event, late bool, out Outcome) Outcome {
	// Reopen: a new fault after a previous resolve starts a new
	// lifecycle on the SAME trackable incident.
	if inc.State == model.StateResolved {
		inc.State = model.StatePending
		inc.ReopenCount++
		inc.ResolvedAt = time.Time{}
		inc.FiredAt = time.Time{}
		inc.Suppressed = false
		// Keep SignalKinds history? Reset the firing window counters so a
		// genuinely new fault must re-collect evidence.
		inc.SignalKinds = model.KindSet{}
		inc.KindTimes = map[model.EventKind]time.Time{}
		inc.Reason = fmt.Sprintf("reopened by event=%s (reopen#%d)", e.EventID, inc.ReopenCount)
	}

	// A late fault for a pending incident is still evidence, but it
	// cannot by itself fire: firing is evaluated relative to the newest
	// watermark below.
	newKind := inc.SignalKinds.Add(e.Kind)
	if prev, ok := inc.KindTimes[e.Kind]; !ok || e.Timestamp.After(prev) {
		inc.KindTimes[e.Kind] = e.Timestamp
	}
	count := en.distinctKindsInWindow(inc, e.Timestamp)

	switch inc.State {
	case model.StatePending:
		if count >= en.rules.FireAfterKinds && (!late || newKind) {
			if inc.Suppressed {
				inc.State = model.StateSuppressed
				inc.Reason = fmt.Sprintf(
					"suppressed: firing rule met (%d kinds) while under suppression (event=%s)",
					count, e.EventID)
			} else {
				inc.State = model.StateFiring
				inc.FiredAt = e.Timestamp
				if err := en.notif.Notify(inc); err != nil {
					inc.State = model.StatePending
					inc.FiredAt = time.Time{}
					inc.Reason = "fire withheld: downstream error: " + err.Error()
					out.State, out.Reason = model.StatePending, inc.Reason
					return out
				}
				inc.NotifyCount++
				inc.Reason = fmt.Sprintf(
					"fired: %d distinct signal kinds %s >= threshold %d (event=%s)",
					count, kindNames(inc.KindList()), en.rules.FireAfterKinds, e.EventID)
				out.Notified = true
			}
		} else {
			inc.Reason = fmt.Sprintf(
				"pending: %d/%d distinct kinds in window (new_kind=%v, late=%v, event=%s)",
				count, en.rules.FireAfterKinds, newKind, late, e.EventID)
		}
	case model.StateFiring, model.StateSuppressed:
		// Already firing: more evidence must NOT produce another alert.
		inc.Reason = fmt.Sprintf("already %s; extra evidence attached, no renotify (event=%s)",
			inc.State, e.EventID)
	}
	out.State = inc.State
	out.Reason = inc.Reason
	return out
}

// applySuppression handles the Suppressed flag. Suppression is sticky for the
// current firing lifecycle and never generates notifications; when the incident
// resolves, the next lifecycle starts unsuppressed.
func (en *Engine) applySuppression(inc *model.Incident, e model.Event) {
	if !e.Suppressed {
		return
	}
	if inc.State == model.StateFiring {
		inc.State = model.StateSuppressed
		inc.Suppressed = true
		inc.Reason = "suppressed by event=" + e.EventID
	} else if inc.State == model.StatePending {
		inc.Suppressed = true
		inc.Reason = "suppressed while pending (event=" + e.EventID + ")"
	}
}

// distinctKindsInWindow counts fault-signal kinds with a signal no older
// than FireWindow relative to at.
func (en *Engine) distinctKindsInWindow(inc *model.Incident, at time.Time) int {
	if en.rules.FireWindow <= 0 {
		return inc.SignalKinds.Len()
	}
	n := 0
	for _, ts := range inc.KindTimes {
		if !ts.Before(at.Add(-en.rules.FireWindow)) {
			n++
		}
	}
	return n
}

func kindNames(ks []model.EventKind) string {
	ss := make([]string, len(ks))
	for i, k := range ks {
		ss[i] = string(k)
	}
	return "[" + strings.Join(ss, ",") + "]"
}

// Get returns a copy-safe pointer to the incident (callers must not
// mutate outside the engine; tests run under the engine's own locking).
func (en *Engine) Get(key string) (*model.Incident, bool) {
	en.mu.Lock()
	defer en.mu.Unlock()
	inc, ok := en.items[key]
	return inc, ok
}

// List returns incidents in deterministic creation order.
func (en *Engine) List() []*model.Incident {
	en.mu.Lock()
	defer en.mu.Unlock()
	out := make([]*model.Incident, 0, len(en.order))
	for _, k := range en.order {
		out = append(out, en.items[k])
	}
	return out
}

// SetNotifier replaces the downstream notifier. It is used during restart:
// state is replayed with a SilentNotifier, then the real notifier is
// installed before live processing starts.
func (en *Engine) SetNotifier(n Notifier) {
	en.mu.Lock()
	defer en.mu.Unlock()
	en.notif = n
}
