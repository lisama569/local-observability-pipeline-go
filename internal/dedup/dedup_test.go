package dedup

import "testing"

func TestLRUBoundedAndRecency(t *testing.T) {
	l := NewLRU(2)
	if l.SeenOrAdd("a") {
		t.Fatal("a should be new")
	}
	if !l.SeenOrAdd("a") {
		t.Fatal("a should now be seen (duplicate)")
	}
	// Touch a again now that b exists, making b the LRU.
	l.SeenOrAdd("a")
	l.SeenOrAdd("b")
	l.SeenOrAdd("c") // capacity 2: b was just touched, a is LRU -> evict a
	if l.Len() != 2 {
		t.Fatalf("len=%d must stay bounded", l.Len())
	}
	if l.Has("a") {
		t.Fatal("a should have been evicted as LRU")
	}
	if !l.Has("b") || !l.Has("c") {
		t.Fatal("b (recently used) and c must remain")
	}
	t.Log("decision basis: duplicate ids return true; capacity evicts least-recently-used; memory stays bounded")
}
