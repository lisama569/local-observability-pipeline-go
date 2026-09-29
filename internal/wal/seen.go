package wal

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const seenFile = "seen.log"

// SeenLog is a durable append-only set of accepted event ids. It makes
// duplicate detection survive restarts independent of the bounded in-memory
// LRU: every id acknowledged to a caller is fsynced here first.
//
// One raw base64-encoded id per line keeps membership an in-memory
// map while the file stays trivially re-readable; growth is one short
// line per distinct accepted event.
type SeenLog struct {
	mu   sync.Mutex
	f    *os.File
	seen map[string]struct{}
}

// OpenSeen opens or creates the seen-id log in dir and loads it.
func OpenSeen(dir string) (*SeenLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, seenFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s := &SeenLog{f: f, seen: map[string]struct{}{}}
	if err := s.load(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *SeenLog) load() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(s.f)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		b, err := base64.StdEncoding.DecodeString(line)
		if err != nil {
			return fmt.Errorf("seen log: corrupt id: %w", err)
		}
		s.seen[string(b)] = struct{}{}
	}
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return sc.Err()
}

// Has reports durable membership.
func (s *SeenLog) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.seen[id]
	return ok
}

// Add durably records id. Returns true if it was already present.
func (s *SeenLog) Add(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.seen[id]; ok {
		return true, nil
	}
	if _, err := s.f.WriteString(base64.StdEncoding.EncodeToString([]byte(id)) + "\n"); err != nil {
		return false, err
	}
	if err := s.f.Sync(); err != nil {
		return false, err
	}
	s.seen[id] = struct{}{}
	return false, nil
}

// Len returns the number of durable ids.
func (s *SeenLog) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// Close releases the file.
func (s *SeenLog) Close() error {
	if s == nil || s.f == nil {
		return nil
	}
	return s.f.Close()
}
