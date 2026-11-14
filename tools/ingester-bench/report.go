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
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

// sampler records each ingester's resource use over the measurement window.
type sampler struct {
	ingesters []*ingesterProcess
	feed      *feed
	mu        sync.Mutex
	start     []map[string]float64
	end       []map[string]float64
	startAt   time.Time
	endAt     time.Time
	gauges    []map[string][]float64
	lag       [][]float64
}

// The gauges sampled over the window.
var sampledGauges = []string{"go_memstats_heap_inuse_bytes", "process_resident_memory_bytes", "go_memstats_heap_alloc_bytes", "go_goroutines"}

func newSampler(ingesters []*ingesterProcess, f *feed) *sampler {
	s := &sampler{ingesters: ingesters, feed: f, gauges: make([]map[string][]float64, len(ingesters)), lag: make([][]float64, len(ingesters))}
	for i := range s.gauges {
		s.gauges[i] = map[string][]float64{}
	}
	return s
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

func (s *sampler) snapshot(into *[]map[string]float64) {
	*into = make([]map[string]float64, len(s.ingesters))
	for i, ing := range s.ingesters {
		m, err := ing.metrics()
		if err != nil {
			m = map[string]float64{}
		}
		(*into)[i] = m
	}
}

// Report is what a bench run measured, per ingester (A, B, and C and D if they ran) where it applies.
type Report struct {
	Ingesters   []string             `json:"ingesters"`
	Series      int                  `json:"series"`
	Tenants     int                  `json:"tenants"`
	SamplesPerS float64              `json:"samples_per_second"`
	Replay      []float64            `json:"replay_seconds"`
	WindowS     float64              `json:"window_seconds"`
	Resources   []Resources          `json:"resources"`
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
	Sent            int `json:"sent"`
	Dropped         int `json:"dropped"`
	Compared        int `json:"compared"`
	Match           int `json:"match"`
	MatchAfterRetry int `json:"match_after_retry"`
	// Reads where an ingester persistently differed from A, and per ingester, of which A's is always 0.
	Mismatch     int       `json:"mismatch"`
	MismatchOf   []int     `json:"mismatch_of,omitempty"`
	TooLarge     int       `json:"too_large"`
	Errors       []int     `json:"errors"`
	FirstErrors  []string  `json:"first_errors,omitempty"`
	ItemsPerRead float64   `json:"items_per_read"`
	LatencyMs    []Latency `json:"latency_ms"`
	// The slowest reads of the window on each ingester, with how long the others took, to find which reads are slow.
	Slowest [][]TimedRead `json:"slowest,omitempty"`
}

// TimedRead is a read of the window, with how long each ingester took, in milliseconds.
type TimedRead struct {
	Ms      []float64 `json:"ms"`
	Request string    `json:"request"`
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
		n := len(d.ingesters)
		r := APIReport{Sent: s.sent, Dropped: s.dropped, Compared: s.compared, Match: s.match, MatchAfterRetry: s.matchAfterRetry,
			Mismatch: s.mismatch, MismatchOf: slices.Clone(s.mismatchOf), TooLarge: s.tooLarge, Errors: slices.Clone(s.errors), FirstErrors: slices.Clone(s.firstErrors),
			LatencyMs: make([]Latency, n), Slowest: make([][]TimedRead, n)}
		if s.compared > 0 {
			r.ItemsPerRead = float64(s.items) / float64(s.compared)
		}
		for i := range n {
			r.LatencyMs[i] = latency(s.latencies[i])
			for _, read := range s.slowest[i] {
				ms := make([]float64, n)
				for j, took := range read.took {
					ms[j] = float64(took.Microseconds()) / 1000
				}
				r.Slowest[i] = append(r.Slowest[i], TimedRead{Ms: ms, Request: read.request})
			}
		}
		out[api] = r
	}
	return out
}

// letters are the ingesters' letters, A first.
func letters(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = string(rune('A' + i))
	}
	return out
}

func (r Report) print(w io.Writer) {
	n := len(r.Ingesters)
	ls := letters(n)
	fmt.Fprintf(w, "\n%d series in %d tenants, %.0f samples/s, %.0fs window\n", r.Series, r.Tenants, r.SamplesPerS, r.WindowS)
	named := make([]string, n)
	for i, name := range r.Ingesters {
		named[i] = ls[i] + " = " + name
	}
	fmt.Fprintf(w, "%s\n\n", strings.Join(named, ", "))
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	gib := func(b float64) string { return fmt.Sprintf("%.2f GiB", b/(1<<30)) }
	mib := func(b float64) string { return fmt.Sprintf("%.1f MiB/s", b/(1<<20)) }
	ratio := func(a, b float64) string {
		if a == 0 {
			return "-"
		}
		return fmt.Sprintf("%.2f", b/a)
	}
	header := "\t" + strings.Join(ls, "\t") + "\t"
	for _, l := range ls[1:] {
		header += l + "/A\t"
	}
	fmt.Fprintln(tw, header)
	of := func(key func(Resources) float64) []float64 {
		vs := make([]float64, n)
		for i, res := range r.Resources {
			vs[i] = key(res)
		}
		return vs
	}
	rows := []struct {
		name string
		vs   []float64
		f    func(float64) string
	}{
		{"replay", r.Replay, func(v float64) string { return fmt.Sprintf("%.0fs", v) }},
		{"CPU", of(func(x Resources) float64 { return x.CPUCores }), func(v float64) string { return fmt.Sprintf("%.2f cores", v) }},
		{"GC CPU", of(func(x Resources) float64 { return x.GCCPUCores }), func(v float64) string { return fmt.Sprintf("%.2f cores", v) }},
		{"allocated", of(func(x Resources) float64 { return x.AllocBytesPerS }), mib},
		{"allocations", of(func(x Resources) float64 { return x.AllocsPerS }), func(v float64) string { return fmt.Sprintf("%.0f/s", v) }},
		{"GC cycles", of(func(x Resources) float64 { return x.GCCyclesPerMin }), func(v float64) string { return fmt.Sprintf("%.1f/min", v) }},
		{"heap in use avg", of(func(x Resources) float64 { return x.HeapInuseAvg }), gib},
		{"heap in use max", of(func(x Resources) float64 { return x.HeapInuseMax }), gib},
		{"heap live avg", of(func(x Resources) float64 { return x.HeapLiveAvg }), gib},
		{"RSS avg", of(func(x Resources) float64 { return x.RSSAvg }), gib},
		{"RSS max", of(func(x Resources) float64 { return x.RSSMax }), gib},
		{"ingest lag avg", of(func(x Resources) float64 { return x.IngestLagAvg }), func(v float64) string { return fmt.Sprintf("%.0f records", v) }},
	}
	for _, row := range rows {
		line := row.name + "\t"
		for _, v := range row.vs {
			line += row.f(v) + "\t"
		}
		for _, v := range row.vs[1:] {
			line += ratio(row.vs[0], v) + "\t"
		}
		fmt.Fprintln(tw, line)
	}
	tw.Flush()
	fmt.Fprintln(w)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	all := strings.Join(ls, "/")
	fmt.Fprintf(tw, "API\tsent\tcompared\tmismatch\tafter retry\terrors %s\tp50 %s ms\tp99 %s ms\tp99.9 %s ms\titems/read\t\n", all, all, all, all)
	apis := make([]string, 0, len(r.APIs))
	for api := range r.APIs {
		apis = append(apis, api)
	}
	sort.Strings(apis)
	join := func(vs []string) string { return strings.Join(vs, "/") }
	perIngester := func(f func(i int) string) string {
		vs := make([]string, n)
		for i := range vs {
			vs[i] = f(i)
		}
		return join(vs)
	}
	for _, api := range apis {
		x := r.APIs[api]
		at := func(i int) Latency {
			if i < len(x.LatencyMs) {
				return x.LatencyMs[i]
			}
			return Latency{}
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%.0f\t\n", api, x.Sent, x.Compared, x.Mismatch, x.MatchAfterRetry,
			perIngester(func(i int) string { return fmt.Sprint(x.Errors[i]) }),
			perIngester(func(i int) string { return fmt.Sprintf("%.2f", at(i).P50) }),
			perIngester(func(i int) string { return fmt.Sprintf("%.2f", at(i).P99) }),
			perIngester(func(i int) string { return fmt.Sprintf("%.1f", at(i).P999) }), x.ItemsPerRead)
	}
	tw.Flush()
	for _, api := range apis {
		x := r.APIs[api]
		for i, e := range x.FirstErrors {
			if e != "" {
				fmt.Fprintf(w, "first %s error of %s: %s\n", api, ls[i], e)
			}
		}
		for i, m := range x.MismatchOf {
			if m > 0 {
				fmt.Fprintf(w, "%s: %d reads where %s persistently differed from A\n", api, m, ls[i])
			}
		}
	}
	for _, api := range apis {
		x := r.APIs[api]
		for i := 1; i < len(x.Slowest); i++ {
			if len(x.Slowest[i]) == 0 {
				continue
			}
			fmt.Fprintf(w, "\nslowest %s reads of %s (%s ms):\n", api, ls[i], all)
			for _, read := range x.Slowest[i][:min(3, len(x.Slowest[i]))] {
				ms := make([]string, len(read.Ms))
				for j, v := range read.Ms {
					ms[j] = fmt.Sprintf("%.1f", v)
				}
				fmt.Fprintf(w, "  %s %s\n", join(ms), read.Request[:min(len(read.Request), 400)])
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
func watchIngesters(ctx context.Context, ingesters []*ingesterProcess, out string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	down := make([]bool, len(ingesters))
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
		// The bench's own, which holds the in-process Kafka's log.
		var self runtime.MemStats
		runtime.ReadMemStats(&self)
		parts = append(parts, fmt.Sprintf("bench heap %.2f GiB", float64(self.HeapInuse)/(1<<30)))
		log.Print(strings.Join(parts, "; "))
	}
}
