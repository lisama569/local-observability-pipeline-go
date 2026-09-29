package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
)

func ev(id, source string, ts time.Time) model.Event {
	return model.Event{
		ID: id, Kind: model.KindLog, Source: source,
		Fingerprint: "fp", Timestamp: ts, Severity: model.SeverityError,
	}
}

func TestAppendDedupAndStats(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir, SyncOnWrite: true})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	if _, err := s.Append(ctx, ev("e1", "svc", base)); err != nil {
		t.Fatal(err)
	}
	// 同一来源同一 ID 重复投递：去重，不算新数据。
	_, err = s.Append(ctx, ev("e1", "svc", base))
	t.Logf("duplicate append: err=%v", err)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	// 不同来源同一 ID：不是重复。
	if _, err := s.Append(ctx, ev("e1", "svc2", base)); err != nil {
		t.Fatal(err)
	}
	// 缺字段：拒绝且原因可区分。
	_, err = s.Append(ctx, model.Event{ID: "bad"})
	t.Logf("invalid append: err=%v", err)
	if !errors.Is(err, model.ErrMissingKind) {
		t.Fatalf("want ErrMissingKind, got %v", err)
	}
	st := s.Stats()
	t.Logf("stats=%+v", st)
	if st.Accepted != 2 || st.Duplicates != 1 || st.Rejected != 1 {
		t.Fatalf("unexpected stats: %+v", st)
	}
}

func TestQueryOutOfOrderDeterministic(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(Options{Dir: dir})
	defer s.Close()
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	// 乱序到达：e3 的时间最早但最后投递。
	events := []model.Event{
		ev("e1", "svc", base.Add(2*time.Minute)),
		ev("e2", "svc", base.Add(5*time.Minute)),
		ev("e3", "svc", base.Add(1*time.Minute)),
	}
	for _, e := range events {
		if _, err := s.Append(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	got := s.Query(base, base.Add(10*time.Minute), "")
	var ids []string
	for _, e := range got {
		ids = append(ids, e.ID)
	}
	t.Logf("query order=%v (input order was e1,e2,e3)", ids)
	want := []string{"e3", "e1", "e2"} // 按事件时间排序，与到达顺序无关
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("want %v, got %v", want, ids)
		}
	}
	// kind 过滤。
	if n := len(s.Query(base, base.Add(10*time.Minute), model.KindMetric)); n != 0 {
		t.Fatalf("kind filter broken: %d", n)
	}
}

func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

	s1, _ := Open(Options{Dir: dir, SyncOnWrite: true})
	s1.Append(ctx, ev("e1", "svc", base))
	s1.Append(ctx, ev("e2", "svc", base.Add(time.Minute)))
	s1.MarkNotified(ctx, "rule|fp|firing|e1")
	st1 := s1.Stats()
	s1.Close()

	// 重启：重放 WAL，状态必须一致。
	s2, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	st2 := s2.Stats()
	t.Logf("before restart stats=%+v after=%+v", st1, st2)
	if st2.Accepted != st1.Accepted {
		t.Fatalf("accepted mismatch after restart: %+v vs %+v", st1, st2)
	}
	// 重启后重复投递仍被去重（不重复计入）。
	if _, err := s2.Append(ctx, ev("e1", "svc", base)); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("dedup lost after restart: %v", err)
	}
	// 已确认标记在重启后保留。
	if !s2.IsNotified("rule|fp|firing|e1") {
		t.Fatal("notified mark lost after restart")
	}
	// 重放顺序按接收序。
	replay := s2.ReplayEvents()
	if len(replay) != 2 || replay[0].ID != "e1" || replay[1].ID != "e2" {
		t.Fatalf("replay order wrong: %+v", replay)
	}
}
