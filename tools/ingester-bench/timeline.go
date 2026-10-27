// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Timeline is what the run recorded over time, for graphs: each ingester's resources, the feed, and each API's reads,
// in buckets.
type Timeline struct {
	IntervalS float64               `json:"interval_seconds"`
	BucketS   float64               `json:"bucket_seconds"`
	Ingesters [2][]IngesterPoint    `json:"ingesters"`
	Feed      []FeedPoint           `json:"feed"`
	APIs      map[string][]APIPoint `json:"apis"`
}

type IngesterPoint struct {
	T              int64   `json:"t"`
	Up             bool    `json:"up"`
	CPUCores       float64 `json:"cpu_cores"`
	GCCPUCores     float64 `json:"gc_cpu_cores"`
	AllocBytesPerS float64 `json:"alloc_bytes_per_second"`
	AllocsPerS     float64 `json:"allocs_per_second"`
	GCPerMin       float64 `json:"gc_per_minute"`
	HeapInuse      float64 `json:"heap_inuse_bytes"`
	HeapAlloc      float64 `json:"heap_alloc_bytes"`
	RSS            float64 `json:"rss_bytes"`
	Goroutines     float64 `json:"goroutines"`
	MemorySeries   float64 `json:"memory_series"`
	ActiveSeries   float64 `json:"active_series"`
	SamplesPerS    float64 `json:"ingested_samples_per_second"`
	LagRecords     float64 `json:"lag_records"`
}

type FeedPoint struct {
	T           int64   `json:"t"`
	SamplesPerS float64 `json:"samples_per_second"`
	Offset      int64   `json:"offset"`
}

// APIPoint is an API's reads that ended in a bucket, per ingester where it applies.
type APIPoint struct {
	T          int64      `json:"t"`
	Reads      int        `json:"reads"`
	Errors     [2]int     `json:"errors"`
	P50        [2]float64 `json:"p50_ms"`
	P90        [2]float64 `json:"p90_ms"`
	P99        [2]float64 `json:"p99_ms"`
	Max        [2]float64 `json:"max_ms"`
	Mismatch   int        `json:"mismatch"`
	AfterRetry int        `json:"match_after_retry"`
}

// Phase is a span of the run.
type Phase struct {
	Name  string `json:"name"`
	Start int64  `json:"start"`
	End   int64  `json:"end"`
}

// recorder builds the timeline as the run goes.
type recorder struct {
	mu       sync.Mutex
	timeline Timeline
	bucket   time.Duration
	// The reads of the bucket being filled, per API.
	open      map[string]*openBucket
	openStart int64
	phases    []Phase
}

type openBucket struct {
	latencies            [2][]time.Duration
	errors               [2]int
	mismatch, afterRetry int
}

func newRecorder(interval, bucket time.Duration) *recorder {
	return &recorder{timeline: Timeline{IntervalS: interval.Seconds(), BucketS: bucket.Seconds(), APIs: map[string][]APIPoint{}}, bucket: bucket, open: map[string]*openBucket{}}
}

func (r *recorder) phase(name string, start, end time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases = append(r.phases, Phase{Name: name, Start: start.Unix(), End: end.Unix()})
}

func (r *recorder) bucketOf(api string) *openBucket {
	now := time.Now().Truncate(r.bucket).Unix()
	if r.openStart != 0 && now != r.openStart {
		r.closeLocked()
	}
	r.openStart = now
	b, ok := r.open[api]
	if !ok {
		b = &openBucket{}
		r.open[api] = b
	}
	return b
}

// read records a read's latencies on both ingesters, or their errors.
func (r *recorder) read(api string, latencies [2]time.Duration, errs [2]error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.bucketOf(api)
	for i := range 2 {
		if errs[i] != nil {
			b.errors[i]++
		} else {
			b.latencies[i] = append(b.latencies[i], latencies[i])
		}
	}
}

func (r *recorder) outcome(api string, mismatch bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	b := r.bucketOf(api)
	if mismatch {
		b.mismatch++
	} else {
		b.afterRetry++
	}
}

func (r *recorder) closeLocked() {
	for api, b := range r.open {
		p := APIPoint{T: r.openStart, Errors: b.errors, Mismatch: b.mismatch, AfterRetry: b.afterRetry}
		for i := range 2 {
			l := latency(b.latencies[i])
			p.P50[i], p.P90[i], p.P99[i], p.Max[i] = l.P50, l.P90, l.P99, l.Max
			p.Reads = max(p.Reads, l.Count+b.errors[i])
		}
		r.timeline.APIs[api] = append(r.timeline.APIs[api], p)
	}
	r.open = map[string]*openBucket{}
}

// sample records the ingesters and the feed every interval until ctx is done.
func (r *recorder) sample(ctx context.Context, ingesters [2]*ingesterProcess, f *feed, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var previous [2]map[string]float64
	var previousAt [2]time.Time
	previousSamples, previousFeedAt := f.samples.Load(), time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now()
		produced := f.lastOffset.Load()
		for i, ing := range ingesters {
			p := IngesterPoint{T: now.Unix()}
			m, err := ing.metrics()
			if err == nil {
				p.Up = true
				if previous[i] != nil {
					dt := now.Sub(previousAt[i]).Seconds()
					rate := func(name string) float64 { return max(0, m[name]-previous[i][name]) / dt }
					p.CPUCores = rate("process_cpu_seconds_total")
					p.GCCPUCores = rate("go_cpu_classes_gc_total_cpu_seconds_total")
					p.AllocBytesPerS = rate("go_memstats_alloc_bytes_total")
					p.AllocsPerS = rate("go_memstats_mallocs_total")
					p.GCPerMin = rate("go_gc_duration_seconds_count") * 60
					p.SamplesPerS = rate("cortex_ingester_ingested_samples_total")
				}
				p.HeapInuse, p.HeapAlloc = m["go_memstats_heap_inuse_bytes"], m["go_memstats_heap_alloc_bytes"]
				p.RSS, p.Goroutines = m["process_resident_memory_bytes"], m["go_goroutines"]
				p.MemorySeries, p.ActiveSeries = m["cortex_ingester_memory_series"], m["cortex_ingester_active_series"]
				if consumed, ok := m["cortex_ingest_storage_reader_last_consumed_offset{partition=0}"]; ok {
					p.LagRecords = max(0, float64(produced)-consumed)
				}
				previous[i], previousAt[i] = m, now
			}
			r.mu.Lock()
			r.timeline.Ingesters[i] = append(r.timeline.Ingesters[i], p)
			r.mu.Unlock()
		}
		samples := f.samples.Load()
		r.mu.Lock()
		r.timeline.Feed = append(r.timeline.Feed, FeedPoint{T: now.Unix(), Offset: produced, SamplesPerS: float64(samples-previousSamples) / now.Sub(previousFeedAt).Seconds()})
		r.mu.Unlock()
		previousSamples, previousFeedAt = samples, now
	}
}

// finish closes the last bucket and returns the timeline and phases.
func (r *recorder) finish() (Timeline, []Phase) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closeLocked()
	for api := range r.timeline.APIs {
		sort.Slice(r.timeline.APIs[api], func(a, b int) bool { return r.timeline.APIs[api][a].T < r.timeline.APIs[api][b].T })
	}
	return r.timeline, append([]Phase(nil), r.phases...)
}
