package wal

import (
	"context"
	"testing"
)

func TestSeenLogDurableAcrossOpen(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s, err := OpenSeen(dir)
	if err != nil {
		t.Fatal(err)
	}
	if dup, err := s.Add(ctx, "id-1"); err != nil || dup {
		t.Fatalf("first add dup=%v err=%v", dup, err)
	}
	if dup, _ := s.Add(ctx, "id-1"); !dup {
		t.Fatal("second add in same process must be duplicate")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenSeen(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.Has("id-1") {
		t.Fatal("id must survive restart (durable duplicate protection)")
	}
	if dup, _ := s2.Add(ctx, "id-1"); !dup {
		t.Fatal("add after restart must report duplicate")
	}
	t.Logf("decision basis: seen log survived restart, len=%d", s2.Len())
}
