// Package dedup provides a bounded LRU set used to recognize redelivered
// events, including within a short redelivery window after restart.
//
// The capacity makes the memory footprint bounded: once full, the
// least-recently-used id is evicted. Durable duplicate protection across
// restarts is provided by the WAL + store; this cache is a fast path.
package dedup

import "sync"

// LRU is a thread-safe bounded set evicting least-recently-used ids.
type LRU struct {
	mu    sync.Mutex
	cap   int
	seen  map[string]struct{}
	order []string // order[0] is least recently used
}

// NewLRU creates a set holding at most capacity ids. A capacity <= 0 is
// normalized to 1.
func NewLRU(capacity int) *LRU {
	if capacity <= 0 {
		capacity = 1
	}
	return &LRU{
		cap:   capacity,
		seen:  make(map[string]struct{}, capacity),
		order: make([]string, 0, capacity),
	}
}

// SeenOrAdd reports whether id was already present (true = duplicate) and
// records/touches it otherwise.
func (l *LRU) SeenOrAdd(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[id]; ok {
		l.touch(id)
		return true
	}
	if len(l.order) >= l.cap {
		victim := l.order[0]
		l.order = l.order[1:]
		delete(l.seen, victim)
	}
	l.seen[id] = struct{}{}
	l.order = append(l.order, id)
	return false
}

// Has reports membership without changing recency.
func (l *LRU) Has(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.seen[id]
	return ok
}

// touch moves id to the most-recently-used end. Caller holds mu.
func (l *LRU) touch(id string) {
	for i, v := range l.order {
		if v == id {
			l.order = append(l.order[:i], l.order[i+1:]...)
			l.order = append(l.order, id)
			return
		}
	}
}

// Len returns the number of retained ids.
func (l *LRU) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.seen)
}
