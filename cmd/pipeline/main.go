// pipeline 是本地可运行的观测数据管道演示入口：
// 接入指标/日志/追踪三类事件，对同一故障关联出可解释的告警，
// 并演示重启后不丢、不重。所有数据落在临时目录，不依赖外部服务。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/alert"
	"github.com/lisama569/local-observability-pipeline-go/internal/model"
	"github.com/lisama569/local-observability-pipeline-go/internal/pipeline"
	"github.com/lisama569/local-observability-pipeline-go/internal/sink"
	"github.com/lisama569/local-observability-pipeline-go/internal/store"
)

// printingSink 把告警迁移打印到标准输出，模拟本地下游。
type printingSink struct{ inner *sink.MemSink }

func (p *printingSink) Send(ctx context.Context, t alert.Transition) error {
	fmt.Printf("[NOTIFY] %s -> %s rule=%s fp=%s reopened=%v\n  reason: %s\n  evidence: %v\n",
		t.From, t.To, t.Rule, t.Fingerprint, t.Reopened, t.Reason, t.TriggerIDs)
	return p.inner.Send(ctx, t)
}

func main() {
	dir := flag.String("dir", "", "WAL 目录（默认临时目录）")
	flag.Parse()
	if *dir == "" {
		d, err := os.MkdirTemp("", "obspipe-*")
		if err != nil {
			log.Fatal(err)
		}
		*dir = d
	}
	fmt.Println("WAL dir:", *dir)

	rule := alert.Rule{
		Name: "error-burst", Window: time.Minute, Threshold: 3,
		ResolveAfter: 2 * time.Minute, ReopenWindow: 5 * time.Minute,
	}
	open := func() (*pipeline.Pipeline, *sink.MemSink) {
		ms := &sink.MemSink{}
		p, err := pipeline.Open(pipeline.Config{
			Store: store.Options{Dir: *dir, SyncOnWrite: true},
			Rules: []alert.Rule{rule},
		}, &printingSink{inner: ms})
		if err != nil {
			log.Fatal(err)
		}
		return p, ms
	}

	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Minute)
	fp := "checkout-db-latency" // 同一故障的关联键

	p1, ms1 := open()
	events := []model.Event{
		{ID: "m-1", Kind: model.KindMetric, Source: "checkout", Fingerprint: fp,
			Timestamp: base, Severity: model.SeverityError, Message: "p99 latency > 2s", Value: 2.3},
		{ID: "l-1", Kind: model.KindLog, Source: "checkout", Fingerprint: fp,
			Timestamp: base.Add(5 * time.Second), Severity: model.SeverityError, Message: "db timeout"},
		{ID: "t-1", Kind: model.KindTrace, Source: "checkout", Fingerprint: fp,
			Timestamp: base.Add(9 * time.Second), Severity: model.SeverityError,
			Message: "span checkout->db failed", TraceID: "trace-001"},
	}
	for _, e := range events {
		res, err := p1.Ingest(ctx, e)
		fmt.Printf("ingest %-6s id=%-4s -> seq=%d dup=%v defaulted=%v err=%v\n",
			e.Kind, e.ID, res.Seq, res.Duplicate, res.Defaulted, err)
	}
	// 演示重复投递：同一事件再发一次，被去重，不产生新告警。
	res, _ := p1.Ingest(ctx, events[0])
	fmt.Printf("redeliver metric id=m-1 -> duplicate=%v (不计入告警)\n", res.Duplicate)

	deadline := time.Now().Add(2 * time.Second)
	for ms1.SentCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Println("alerts:", p1.Alerts())
	p1.Close()

	// 模拟重启：同一目录重开，已确认的告警不重复通知，状态完整恢复。
	fmt.Println("--- restart ---")
	p2, ms2 := open()
	time.Sleep(300 * time.Millisecond)
	fmt.Printf("after restart: re-notified=%d (want 0), alerts=%v, stats=%+v\n",
		ms2.SentCount(), p2.Alerts(), p2.Stats())
	p2.Close()
}
