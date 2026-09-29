// Command pipeline runs a fully local demonstration of the observability
// pipeline: it ingests metric/log/trace events around one fault, prints the
// unified query result and the resulting trackable alert state, then resolves
// and reopens it. No external services or credentials are used; data
// lives in a temporary directory.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/lisama569/local-observability-pipeline-go/internal/model"
	"github.com/lisama569/local-observability-pipeline-go/internal/pipeline"
	"github.com/lisama569/local-observability-pipeline-go/internal/store"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "obs-pipeline-demo-")
	must(err)
	defer os.RemoveAll(dir)

	cfg := pipeline.DefaultConfig(dir)
	p, err := pipeline.New(ctx, cfg, nil)
	must(err)

	base := time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
	svc, fp := "checkout", "error_rate_high"

	events := []model.Event{
		// 1) single signal -> pending
		{EventID: "m-1", Kind: model.KindMetric, Source: "host-1", Service: svc,
			Fingerprint: fp, Timestamp: base, MetricName: "errors", Value: 0.42, Fault: true},
		// 2) related log on the same trace -> second distinct kind -> firing
		{EventID: "l-1", Kind: model.KindLog, Source: "host-1", Service: svc,
			Fingerprint: fp, TraceID: "t-abc", Timestamp: base.Add(2 * time.Second),
			Severity: model.SeverityError, Message: "payment downstream 500"},
		// 3) related error trace (third kind) -> evidence only, no duplicate alert
		{EventID: "tr-1", Kind: model.KindTrace, Source: "host-1", Service: svc,
			Fingerprint: fp, TraceID: "t-abc", Timestamp: base.Add(3 * time.Second),
			SpanName: "charge", IsError: true, DurationMs: 812},
		// 4) redelivery of m-1: must not create a new fault / alert
		{EventID: "m-1", Kind: model.KindMetric, Source: "host-1", Service: svc,
			Fingerprint: fp, Timestamp: base, MetricName: "errors", Value: 0.42, Fault: true},
		// 5) out-of-order late signal: must not flap state
		{EventID: "l-2", Kind: model.KindLog, Source: "host-1", Service: svc,
			Fingerprint: fp, TraceID: "t-abc", Timestamp: base.Add(-1 * time.Minute),
			Severity: model.SeverityError, Message: "late error log"},
		// 6) recovery -> resolved
		{EventID: "m-ok", Kind: model.KindMetric, Source: "host-1", Service: svc,
			Fingerprint: fp, Timestamp: base.Add(10 * time.Second),
			MetricName: "errors", Value: 0.0, Fault: false, Resolved: true},
		// 7) new fault after resolve -> reopen on the same trackable incident
		{EventID: "m-2", Kind: model.KindMetric, Source: "host-1", Service: svc,
			Fingerprint: fp, Timestamp: base.Add(20 * time.Second),
			MetricName: "errors", Value: 0.55, Fault: true},
		{EventID: "l-3", Kind: model.KindLog, Source: "host-1", Service: svc,
			Fingerprint: fp, Timestamp: base.Add(21 * time.Second),
			Severity: model.SeverityError, Message: "errors again"},
	}

	for _, e := range events {
		res, err := p.Ingest(ctx, e)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ingest %s error: %v\n", e.EventID, err)
			os.Exit(1)
		}
		fmt.Printf("[ingest] event=%-5s -> %s\n", e.EventID, res.Reason)
	}
	must(p.Drain(ctx))

	fmt.Println("\n--- unified query: all events for the fault (deterministic order) ---")
	rows := p.Query(store.Query{Service: svc, Fingerprint: fp})
	for _, r := range rows {
		fmt.Printf("  %s  %-6s  %-8s  %s\n",
			r.Event.Timestamp.Format("15:04:05"), r.Event.EventID, r.Event.Kind, r.Reason)
	}

	fmt.Println("\n--- trackable incident ---")
	for _, inc := range p.Incidents() {
		fmt.Printf("  key=%s state=%s events=%d kinds=%v traces=%v\n",
			inc.Key, inc.State, len(inc.EventIDs),
			incidentsKinds(inc), inc.TraceIDList())
		fmt.Printf("  notifies=%d reopens=%d suppressed=%v\n",
			inc.NotifyCount, inc.ReopenCount, inc.Suppressed)
		fmt.Printf("  reason: %s\n", inc.Reason)
	}
	st := p.Stats()
	fmt.Printf("\nstats: accepted=%d duplicates=%d rejected=%d stored=%d\n",
		st.Accepted, st.Duplicates, st.Rejected, st.Stored)
	fmt.Printf("wal dir: %s\n", dir)
}

func incidentsKinds(inc *model.Incident) []string {
	ks := inc.KindList()
	out := make([]string, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
