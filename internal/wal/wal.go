// Package wal implements a small local write-ahead log backed by files in a
// temporary directory. It guarantees that accepted events survive process
// restarts and are replayed at least once; idempotent processing in the
// alert engine makes that effectively exactly-once for conclusions.
//
// On-disk layout inside dir:
//
//	events.log  - one JSON envelope per line: {"off":n,"event":{...}}
//	checkpoint - single integer: highest offset fully processed
//
// Durability: each Append is fsynced. A torn final line after a crash is
// detected on replay and truncated away rather than silently dropping later
// valid records.
package wal

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

const (
	eventsFile = "events.log"
	ckptFile   = "checkpoint"
)

// ErrTornTail signals that a partially written final record was found and
// repaired during Open.
var ErrTornTail = errors.New("wal: truncated tail record truncated")

type envelope struct {
	Off   int64       `json:"off"`
	Event model.Event `json:"event"`
}

// WAL is an append-only event log with a checkpoint of the last fully
// processed offset.
type WAL struct {
	mu         sync.Mutex
	dir        string
	f          *os.File
	nextOffset int64
	committed  int64
}

// Open opens (or creates) the log in dir and loads the checkpoint.
func Open(ctx context.Context, dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}
	path := filepath.Join(dir, eventsFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}
	w := &WAL{dir: dir, f: f, nextOffset: 1}

	if err := w.repairTornTail(); err != nil {
		f.Close()
		return nil, err
	}
	last, err := w.lastOffset()
	if err != nil {
		f.Close()
		return nil, err
	}
	w.nextOffset = last + 1
	w.committed, err = w.readCheckpoint()
	if err != nil {
		f.Close()
		return nil, err
	}
	if w.committed > last {
		w.committed = last
	}
	return w, nil
}

// Append durably writes an event and returns its monotonic offset (>= 1).
func (w *WAL) Append(ctx context.Context, e model.Event) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	line, err := json.Marshal(envelope{Off: w.nextOffset, Event: e})
	if err != nil {
		return 0, fmt.Errorf("wal: marshal: %w", err)
	}
	line = append(line, '\n')
	if _, err := w.f.Write(line); err != nil {
		return 0, fmt.Errorf("wal: write: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}
	off := w.nextOffset
	w.nextOffset++
	return off, nil
}

// Checkpoint records that every offset <= n is fully processed. The value
// only moves forward and is atomically replaced (tmp file + rename).
func (w *WAL) Checkpoint(ctx context.Context, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if n <= w.committed {
		return nil
	}
	tmp := filepath.Join(w.dir, ckptFile+".tmp")
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(n, 10)), 0o644); err != nil {
		return fmt.Errorf("wal: write checkpoint: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, ckptFile)); err != nil {
		return fmt.Errorf("wal: rename checkpoint: %w", err)
	}
	w.committed = n
	return nil
}

// Committed returns the highest checkpointed offset.
func (w *WAL) Committed() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.committed
}

// Replay invokes fn for every offset strictly greater than the checkpoint, in
// append order. fn stopping with an error aborts the replay.
func (w *WAL) Replay(ctx context.Context, fn func(offset int64, e model.Event) error) error {
	path := filepath.Join(w.dir, eventsFile)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("wal: replay open: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var env envelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			return fmt.Errorf("wal: corrupt record (run repair): %w", err)
		}
		if env.Off <= w.committed {
			continue
		}
		if err := fn(env.Off, env.Event); err != nil {
			return err
		}
	}
	return sc.Err()
}

// Close flushes and closes the log file.
func (w *WAL) Close() error { return w.f.Close() }

func (w *WAL) readCheckpoint() (int64, error) {
	b, err := os.ReadFile(filepath.Join(w.dir, ckptFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("wal: read checkpoint: %w", err)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wal: parse checkpoint %q: %w", string(b), err)
	}
	return n, nil
}

// lastOffset scans the log and returns the highest valid offset.
func (w *WAL) lastOffset() (int64, error) {
	f, err := os.Open(filepath.Join(w.dir, eventsFile))
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var last int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var env envelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			return 0, fmt.Errorf("wal: corrupt record: %w", err)
		}
		last = env.Off
	}
	return last, sc.Err()
}

// repairTornTail truncates a non-newline-terminated or unparseable final
// line, which is the only place a crash can leave a partial write.
func (w *WAL) repairTornTail() error {
	info, err := w.f.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	if size == 0 {
		return nil
	}
	if _, err := w.f.Seek(-1, io.SeekEnd); err != nil {
		return err
	}
	buf := make([]byte, 1)
	if _, err := w.f.Read(buf); err != nil {
		return err
	}
	if buf[0] == '\n' {
		// Last line is newline terminated; verify it parses.
		return w.verifyLastLine(size)
	}
	// Partial write: find last newline and truncate after it.
	return w.truncateAtLastNewline(size)
}

func (w *WAL) verifyLastLine(size int64) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(w.f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var last string
	for sc.Scan() {
		last = sc.Text()
	}
	if err := sc.Err(); err != nil {
		return err
	}
	var env envelope
	if json.Unmarshal([]byte(last), &env) == nil {
		return nil
	}
	return w.truncateAtLastNewline(size)
}

func (w *WAL) truncateAtLastNewline(size int64) error {
	buf := make([]byte, min(int(size), 1024*1024))
	start := size - int64(len(buf))
	if start < 0 {
		start = 0
		buf = buf[:size]
	}
	if _, err := w.f.ReadAt(buf, start); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	cut := int64(-1)
	for i := len(buf) - 1; i >= 0; i-- {
		if buf[i] == '\n' {
			cut = start + int64(i) + 1
			break
		}
	}
	if cut < 0 {
		cut = 0
	}
	if err := w.f.Truncate(cut); err != nil {
		return err
	}
	_, err := w.f.Seek(0, io.SeekEnd)
	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
