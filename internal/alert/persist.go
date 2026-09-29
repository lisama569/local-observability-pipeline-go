package alert

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

// persistedIncident is the JSON form of an Incident.
type persistedIncident struct {
	Incident    model.Incident
	TraceIDs    []string
	KindTimes   map[model.EventKind]time.Time
	SignalKinds []model.EventKind
}

// snapshot is the on-disk form of the whole engine.
type snapshot struct {
	Order     []string
	Incidents map[string]persistedIncident
}

// Snapshot atomically writes all incidents to dir/incidents.json.
func (en *Engine) Snapshot(dir string) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	snap := snapshot{
		Order:     append([]string(nil), en.order...),
		Incidents: make(map[string]persistedIncident, len(en.items)),
	}
	for k, inc := range en.items {
		tr := make([]string, 0, len(inc.TraceIDs))
		for id := range inc.TraceIDs {
			tr = append(tr, id)
		}
		var kinds []model.EventKind
		for kk := range inc.SignalKinds {
			kinds = append(kinds, kk)
		}
		snap.Incidents[k] = persistedIncident{
			Incident:    *inc,
			TraceIDs:    tr,
			KindTimes:   inc.KindTimes,
			SignalKinds: kinds,
		}
	}
	b, err := json.MarshalIndent(snap, "", " ")
	if err != nil {
		return fmt.Errorf("alert snapshot marshal: %w", err)
	}
	final := filepath.Join(dir, "incidents.json")
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return fmt.Errorf("alert snapshot write: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("alert snapshot rename: %w", err)
	}
	return nil
}

// Restore loads incidents previously written by Snapshot. It replaces
// engine contents. Notifier and rules are retained.
func (en *Engine) Restore(dir string) error {
	en.mu.Lock()
	defer en.mu.Unlock()

	path := filepath.Join(dir, "incidents.json")
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("alert restore read: %w", err)
	}
	var snap snapshot
	if err := json.Unmarshal(b, &snap); err != nil {
		return fmt.Errorf("alert restore unmarshal: %w", err)
	}
	en.items = make(map[string]*model.Incident, len(snap.Incidents))
	en.order = snap.Order
	for k, pi := range snap.Incidents {
		inc := pi.Incident
		inc.TraceIDs = make(map[string]struct{}, len(pi.TraceIDs))
		for _, id := range pi.TraceIDs {
			inc.TraceIDs[id] = struct{}{}
		}
		inc.SignalKinds = model.KindSet{}
		for _, kk := range pi.SignalKinds {
			inc.SignalKinds[kk] = struct{}{}
		}
		inc.KindTimes = pi.KindTimes
		if inc.KindTimes == nil {
			inc.KindTimes = map[model.EventKind]time.Time{}
		}
		en.items[k] = &inc
	}
	return nil
}
