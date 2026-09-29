package alert

import "github.com/lisama569/local-observability-pipeline-go/internal/model"

// SilentNotifier never calls a downstream. It is used during restart
// replay: incident state is reconstructed from the WAL without re-sending
// notifications that may already have been delivered before the crash.
type SilentNotifier struct{}

// Notify reports success without any side effect.
func (SilentNotifier) Notify(*model.Incident) error { return nil }

// Resolve reports success without any side effect.
func (SilentNotifier) Resolve(*model.Incident) error { return nil }
