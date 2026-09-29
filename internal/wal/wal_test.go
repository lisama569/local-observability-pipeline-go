package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

func mkEvent(id string, ts int64) model.Event {
	return model.Event{
		EventID: id, Kind: model.KindLog, Source: "h", Service: "s",
		Fingerprint: "f", Timestamp: time.Unix(ts, 0),
		Message: "m", Severity: model.SeverityError,
	}
}

func TestAppendReplayCheckpoint(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	w, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	off1, err := w.Append(ctx, mkEvent("e1", 1))
	if err != nil || off1 != 1 {
		t.Fatalf("off1=%d err=%v", off1, err)
	}
	_, _ = w.Append(ctx, mkEvent("e2", 2))
	if err := w.Checkpoint(ctx, off1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: only offsets > checkpoint should replay.
	w2, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = w2.Replay(ctx, func(off int64, e model.Event) error {
		t.Logf("replay input offset=%d event_id=%s (decision: after checkpoint, replay)", off, e.EventID)
		got = append(got, e.EventID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "e2" {
		t.Fatalf("expected only e2 after checkpoint, got %v", got)
	}
	if w2.Committed() != 1 {
		t.Fatalf("committed=%d", w2.Committed())
	}
}

func TestTornTailTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, eventsFile)
	// Two valid lines + one partial line without trailing newline.
	good := `{"off":1,"event":{"event_id":"e1","kind":"log","source":"h","service":"s","fingerprint":"f","timestamp":"2026-01-01T00:00:00Z","severity":"error","message":"m"}}` + "\n"
	good += `{"off":2,"event":{"event_id":"e2","kind":"log","source":"h","service":"s","fingerprint":"f","timestamp":"2026-01-01T00:00:01Z","severity":"error","message":"m"}}` + "\n"
	torn := `{"off":3,"event":{"event_id":"e3"` // partial, no newline
	if err := os.WriteFile(path, []byte(good+torn), 0o644); err != nil {
		t.Fatal(err)
	}

	w, err := Open(context.Background(), dir)
	if err != nil {
		t.Fatalf("torn tail must be repaired, got %v", err)
	}
	var got []string
	err = w.Replay(context.Background(), func(off int64, e model.Event) error {
		t.Logf("after crash repair replay offset=%d id=%s", off, e.EventID)
		got = append(got, e.EventID)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "e1" || got[1] != "e2" {
		t.Fatalf("valid earlier data must survive torn tail, got %v", got)
	}
	// Appending after repair must not overwrite valid records.
	off, err := w.Append(context.Background(), mkEvent("e4", 4))
	if err != nil || off != 3 {
		t.Fatalf("after repair next offset must be 3, got %d err=%v", off, err)
	}
}
