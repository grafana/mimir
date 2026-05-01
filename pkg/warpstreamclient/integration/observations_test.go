// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// observation is one produce call's outcome.
type observation struct {
	issued  time.Time
	latency time.Duration
	err     error
}

// observations is a thread-safe collector.
type observations struct {
	mu   sync.Mutex
	list []observation
}

func (o *observations) record(issued time.Time, latency time.Duration, err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.list = append(o.list, observation{issued: issued, latency: latency, err: err})
}

func (o *observations) snapshot() []observation {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]observation, len(o.list))
	copy(out, o.list)
	return out
}

// summary aggregates a slice of observations.
type summary struct {
	total       int
	successes   int
	failures    int
	meanLatency time.Duration
	p50Latency  time.Duration
	p99Latency  time.Duration
}

func summarize(obs []observation) summary {
	s := summary{total: len(obs)}
	if len(obs) == 0 {
		return s
	}
	latencies := make([]time.Duration, 0, len(obs))
	var sum time.Duration
	for _, o := range obs {
		if o.err == nil {
			s.successes++
		} else {
			s.failures++
		}
		latencies = append(latencies, o.latency)
		sum += o.latency
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	s.meanLatency = sum / time.Duration(len(latencies))
	s.p50Latency = latencies[len(latencies)*50/100]
	if idx := len(latencies) * 99 / 100; idx < len(latencies) {
		s.p99Latency = latencies[idx]
	} else {
		s.p99Latency = latencies[len(latencies)-1]
	}
	return s
}

// bucketStats holds per-window aggregates derived from a slice of
// observations.
type bucketStats struct {
	total       int
	success     int
	meanLatency time.Duration
	p99Latency  time.Duration
}

// timeBucketStats splits obs into windowSize-wide buckets keyed by issue
// time and returns one bucketStats per bucket up to maxIdx. The first
// bucket is anchored at the earliest issued time.
func timeBucketStats(obs []observation, windowSize time.Duration) (out []bucketStats, start time.Time) {
	if len(obs) == 0 {
		return nil, time.Time{}
	}
	for _, o := range obs {
		if start.IsZero() || o.issued.Before(start) {
			start = o.issued
		}
	}
	byIdx := map[int][]observation{}
	maxIdx := 0
	for _, o := range obs {
		idx := int(o.issued.Sub(start) / windowSize)
		byIdx[idx] = append(byIdx[idx], o)
		if idx > maxIdx {
			maxIdx = idx
		}
	}
	out = make([]bucketStats, maxIdx+1)
	for i := 0; i <= maxIdx; i++ {
		s := summarize(byIdx[i])
		out[i] = bucketStats{
			total:       s.total,
			success:     s.successes,
			meanLatency: s.meanLatency,
			p99Latency:  s.p99Latency,
		}
	}
	return out, start
}

func TestObservations_RecordIsThreadSafe(t *testing.T) {
	t.Parallel()
	obs := &observations{}
	const goroutines = 16
	const perGoroutine = 200
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				obs.record(time.Now(), 10*time.Millisecond, nil)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, goroutines*perGoroutine, len(obs.snapshot()))
}

func TestSummarize_EmptyReturnsZero(t *testing.T) {
	t.Parallel()
	s := summarize(nil)
	assert.Equal(t, summary{}, s)
}

func TestSummarize_CountsAndPercentiles(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	obs := []observation{
		{latency: 100 * time.Millisecond, err: nil},
		{latency: 200 * time.Millisecond, err: nil},
		{latency: 300 * time.Millisecond, err: boom},
		{latency: 400 * time.Millisecond, err: nil},
	}
	s := summarize(obs)
	assert.Equal(t, 4, s.total)
	assert.Equal(t, 3, s.successes)
	assert.Equal(t, 1, s.failures)
	assert.Equal(t, 250*time.Millisecond, s.meanLatency)
	// p50: index = 4*50/100 = 2 → 300ms.
	assert.Equal(t, 300*time.Millisecond, s.p50Latency)
	// p99: index = 4*99/100 = 3 → 400ms.
	assert.Equal(t, 400*time.Millisecond, s.p99Latency)
}

func TestTimeBucketStats_GroupsByIssueTime(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 0)
	obs := []observation{
		{issued: start, latency: 100 * time.Millisecond},
		{issued: start.Add(2 * time.Second), latency: 100 * time.Millisecond},
		{issued: start.Add(11 * time.Second), latency: 200 * time.Millisecond},
		{issued: start.Add(31 * time.Second), latency: 300 * time.Millisecond},
	}
	buckets, gotStart := timeBucketStats(obs, 10*time.Second)
	require.Len(t, buckets, 4) // 0-10, 10-20, 20-30, 30-40
	assert.Equal(t, start, gotStart)
	assert.Equal(t, 2, buckets[0].total)
	assert.Equal(t, 1, buckets[1].total)
	assert.Equal(t, 0, buckets[2].total) // gap
	assert.Equal(t, 1, buckets[3].total)
}

func TestTimeBucketStats_EmptyReturnsNil(t *testing.T) {
	t.Parallel()
	got, start := timeBucketStats(nil, 10*time.Second)
	assert.Nil(t, got)
	assert.True(t, start.IsZero())
}
