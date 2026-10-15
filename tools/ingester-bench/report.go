// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

// sampler records each ingester's resource use over the measurement window.
type sampler struct {
	ingesters [2]*ingesterProcess
	feed      *feed
	mu        sync.Mutex
	start     [2]map[string]float64
	end       [2]map[string]float64
	startAt   time.Time
	endAt     time.Time
	gauges    [2]map[string][]float64
	lag       [2][]float64
}

// The gauges sampled over the window, and the counters taken at its ends.
var (
	sampledGauges  = []string{"go_memstats_heap_inuse_bytes", "process_resident_memory_bytes", "go_memstats_heap_alloc_bytes", "go_goroutines"}
	windowCounters = []string{"process_cpu_seconds_total", "go_memstats_alloc_bytes_total", "go_memstats_mallocs_total", "go_cpu_classes_gc_total_cpu_seconds_total", "go_gc_duration_seconds_count"}
)

func newSampler(ingesters [2]*ingesterProcess, f *feed) *sampler {
	return &sampler{ingesters: ingesters, feed: f, gauges: [2]map[string][]float64{{}, {}}}
}

func (s *sampler) run(ctx context.Context, every time.Duration) {
	s.snapshot(&s.start)
	s.startAt = time.Now()
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.snapshot(&s.end)
			s.endAt = time.Now()
			return
		case <-ticker.C:
		}
		produced := float64(s.feed.lastOffset.Load())
		for i, ing := range s.ingesters {
			m, err := ing.metrics()
			if err != nil {
				continue
			}
			s.mu.Lock()
			for _, g := range sampledGauges {
				s.gauges[i][g] = append(s.gauges[i][g], m[g])
			}
			if consumed, ok := m["cortex_ingest_storage_reader_last_consumed_offset{partition=0}"]; ok {
				// The offset produced is read before the ingester's: it can be a record ahead.
				s.lag[i] = append(s.lag[i], max(0, produced-consumed))
			}
			s.mu.Unlock()
		}
	}
}

func (s *sampler) snapshot(into *[2]map[string]float64) {
	for i, ing := range s.ingesters {
		m, err := ing.metrics()
		if err != nil {
			m = map[string]float64{}
		}
		into[i] = m
	}
}

// Report is what a bench run measured, per ingester (A and B) where it applies.
type Report struct {
	Ingesters   [2]string            `json:"ingesters"`
	Series      int                  `json:"series"`
	Tenants     int                  `json:"tenants"`
	SamplesPerS float64              `json:"samples_per_second"`
	Replay      [2]float64           `json:"replay_seconds"`
	WindowS     float64              `json:"window_seconds"`
	Resources   [2]Resources         `json:"resources"`
	APIs        map[string]APIReport `json:"apis"`
	Mismatches  []string             `json:"mismatch_examples,omitempty"`
	// What the run was, when each of its phases was, and what it recorded over time.
	Config    map[string]string `json:"config"`
	Started   int64             `json:"started"`
	Phases    []Phase           `json:"phases"`
	Timeline  Timeline          `json:"timeline"`
	SlowReads []SlowRead        `json:"slow_reads,omitempty"`
}

type Resources struct {
	CPUCores       float64 `json:"cpu_cores"`
	GCCPUCores     float64 `json:"gc_cpu_cores"`
	AllocBytesPerS float64 `json:"alloc_bytes_per_second"`
	AllocsPerS     float64 `json:"allocs_per_second"`
	GCCyclesPerMin float64 `json:"gc_cycles_per_minute"`
	HeapInuseAvg   float64 `json:"heap_inuse_avg_bytes"`
	HeapInuseMax   float64 `json:"heap_inuse_max_bytes"`
	HeapLiveAvg    float64 `json:"heap_alloc_avg_bytes"`
	RSSAvg         float64 `json:"rss_avg_bytes"`
	RSSMax         float64 `json:"rss_max_bytes"`
	IngestLagAvg   float64 `json:"ingest_lag_avg_records"`
	IngestLagMax   float64 `json:"ingest_lag_max_records"`
}

type APIReport struct {
	Sent            int        `json:"sent"`
	Dropped         int        `json:"dropped"`
	Compared        int        `json:"compared"`
	Match           int        `json:"match"`
	MatchAfterRetry int        `json:"match_after_retry"`
	Mismatch        int        `json:"mismatch"`
	TooLarge        int        `json:"too_large"`
	Errors          [2]int     `json:"errors"`
	FirstErrors     [2]string  `json:"first_errors,omitempty"`
	ItemsPerRead    float64    `json:"items_per_read"`
	LatencyMs       [2]Latency `json:"latency_ms"`
}

type Latency struct {
	P50, P90, P99, P999, Max float64
	Count                    int
}

func latency(ds []time.Duration) Latency {
	if len(ds) == 0 {
		return Latency{}
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
	at := func(q float64) float64 {
		return float64(sorted[min(len(sorted)-1, int(float64(len(sorted))*q))].Microseconds()) / 1000
	}
	return Latency{P50: at(0.5), P90: at(0.9), P99: at(0.99), P999: at(0.999), Max: at(1), Count: len(sorted)}
}

func (s *sampler) resources(i int) Resources {
	window := s.endAt.Sub(s.startAt).Seconds()
	delta := func(name string) float64 { return (s.end[i][name] - s.start[i][name]) / window }
	avgMax := func(name string) (float64, float64) {
		vs := s.gauges[i][name]
		if len(vs) == 0 {
			return 0, 0
		}
		sum, peak := 0.0, 0.0
		for _, v := range vs {
			sum += v
			peak = max(peak, v)
		}
		return sum / float64(len(vs)), peak
	}
	r := Resources{
		CPUCores:       delta("process_cpu_seconds_total"),
		GCCPUCores:     delta("go_cpu_classes_gc_total_cpu_seconds_total"),
		AllocBytesPerS: delta("go_memstats_alloc_bytes_total"),
		AllocsPerS:     delta("go_memstats_mallocs_total"),
		GCCyclesPerMin: delta("go_gc_duration_seconds_count") * 60,
	}
	r.HeapInuseAvg, r.HeapInuseMax = avgMax("go_memstats_heap_inuse_bytes")
	r.HeapLiveAvg, _ = avgMax("go_memstats_heap_alloc_bytes")
	r.RSSAvg, r.RSSMax = avgMax("process_resident_memory_bytes")
	if lags := s.lag[i]; len(lags) > 0 {
		sum := 0.0
		for _, l := range lags {
			sum += l
			r.IngestLagMax = max(r.IngestLagMax, l)
		}
		r.IngestLagAvg = sum / float64(len(lags))
	}
	return r
}

func (d *driver) apiReports() map[string]APIReport {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string]APIReport{}
	for api, s := range d.stats {
		r := APIReport{Sent: s.sent, Dropped: s.dropped, Compared: s.compared, Match: s.match, MatchAfterRetry: s.matchAfterRetry,
			Mismatch: s.mismatch, TooLarge: s.tooLarge, Errors: s.errors, FirstErrors: s.firstErrors}
		if s.compared > 0 {
			r.ItemsPerRead = float64(s.items) / float64(s.compared)
		}
		for i := range 2 {
			r.LatencyMs[i] = latency(s.latencies[i])
		}
		out[api] = r
	}
	return out
}

func (r Report) print(w io.Writer) {
	fmt.Fprintf(w, "\n%d series in %d tenants, %.0f samples/s, %.0fs window\n", r.Series, r.Tenants, r.SamplesPerS, r.WindowS)
	fmt.Fprintf(w, "A = %s, B = %s\n\n", r.Ingesters[0], r.Ingesters[1])
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	gib := func(b float64) string { return fmt.Sprintf("%.2f GiB", b/(1<<30)) }
	mib := func(b float64) string { return fmt.Sprintf("%.1f MiB/s", b/(1<<20)) }
	ratio := func(a, b float64) string {
		if a == 0 {
			return "-"
		}
		return fmt.Sprintf("%.2f", b/a)
	}
	a, b := r.Resources[0], r.Resources[1]
	fmt.Fprintln(tw, "\tA\tB\tB/A\t")
	rows := []struct {
		name string
		a, b float64
		f    func(float64) string
	}{
		{"replay", r.Replay[0], r.Replay[1], func(v float64) string { return fmt.Sprintf("%.0fs", v) }},
		{"CPU", a.CPUCores, b.CPUCores, func(v float64) string { return fmt.Sprintf("%.2f cores", v) }},
		{"GC CPU", a.GCCPUCores, b.GCCPUCores, func(v float64) string { return fmt.Sprintf("%.2f cores", v) }},
		{"allocated", a.AllocBytesPerS, b.AllocBytesPerS, mib},
		{"allocations", a.AllocsPerS, b.AllocsPerS, func(v float64) string { return fmt.Sprintf("%.0f/s", v) }},
		{"GC cycles", a.GCCyclesPerMin, b.GCCyclesPerMin, func(v float64) string { return fmt.Sprintf("%.1f/min", v) }},
		{"heap in use avg", a.HeapInuseAvg, b.HeapInuseAvg, gib},
		{"heap in use max", a.HeapInuseMax, b.HeapInuseMax, gib},
		{"heap live avg", a.HeapLiveAvg, b.HeapLiveAvg, gib},
		{"RSS avg", a.RSSAvg, b.RSSAvg, gib},
		{"RSS max", a.RSSMax, b.RSSMax, gib},
		{"ingest lag avg", a.IngestLagAvg, b.IngestLagAvg, func(v float64) string { return fmt.Sprintf("%.0f records", v) }},
	}
	for _, row := range rows {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t\n", row.name, row.f(row.a), row.f(row.b), ratio(row.a, row.b))
	}
	tw.Flush()
	fmt.Fprintln(w)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "API\tsent\tcompared\tmismatch\tafter retry\terrors A/B\tp50 A/B ms\tp99 A/B ms\tp99.9 A/B ms\titems/read\t")
	apis := make([]string, 0, len(r.APIs))
	for api := range r.APIs {
		apis = append(apis, api)
	}
	sort.Strings(apis)
	for _, api := range apis {
		x := r.APIs[api]
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d/%d\t%.2f/%.2f\t%.2f/%.2f\t%.1f/%.1f\t%.0f\t\n", api, x.Sent, x.Compared, x.Mismatch, x.MatchAfterRetry,
			x.Errors[0], x.Errors[1], x.LatencyMs[0].P50, x.LatencyMs[1].P50, x.LatencyMs[0].P99, x.LatencyMs[1].P99, x.LatencyMs[0].P999, x.LatencyMs[1].P999, x.ItemsPerRead)
	}
	tw.Flush()
	for api, x := range r.APIs {
		for i, e := range x.FirstErrors {
			if e != "" {
				fmt.Fprintf(w, "first %s error of %c: %s\n", api, 'A'+i, e)
			}
		}
	}
	if len(r.Mismatches) > 0 {
		fmt.Fprintf(w, "\n%d mismatch examples, first:\n%s\n", len(r.Mismatches), strings.Join(r.Mismatches[:min(3, len(r.Mismatches))], "\n"))
	}
}

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// watchIngesters logs the ingesters' memory every 30s and saves their heap profiles every two minutes, until ctx is
// done, and says when one stops answering.
func watchIngesters(ctx context.Context, ingesters [2]*ingesterProcess, out string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	down := [2]bool{}
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		var parts []string
		for i, ing := range ingesters {
			m, err := ing.metrics()
			if err != nil {
				if !down[i] {
					log.Printf("%s stopped answering: %v", ing.name, err)
					down[i] = true
				}
				parts = append(parts, ing.name+" down")
				continue
			}
			parts = append(parts, fmt.Sprintf("%s heap %.2f GiB rss %.2f GiB goroutines %.0f", ing.name, m["go_memstats_heap_inuse_bytes"]/(1<<30), m["process_resident_memory_bytes"]/(1<<30), m["go_goroutines"]))
			if n%4 == 3 {
				_ = ing.profile("heap", filepath.Join(out, fmt.Sprintf("heap-%c-%03d.pb.gz", 'a'+i, n/4)), 0)
			}
		}
		log.Print(strings.Join(parts, "; "))
	}
}
