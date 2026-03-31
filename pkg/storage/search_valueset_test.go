// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectAll drains the value set and returns all results, the first error, and all warnings.
func collectAll(t *testing.T, vs *SearchValueSet) ([]SearchResult, annotations.Annotations, error) {
	t.Helper()
	var results []SearchResult
	for vs.Next() {
		results = append(results, vs.At())
	}
	return results, vs.Warnings(), vs.Err()
}

// producerOf returns a producer that sends the given values and returns no error/warnings.
func producerOf(values ...string) func(ch chan<- SearchResult) (annotations.Annotations, error) {
	return func(ch chan<- SearchResult) (annotations.Annotations, error) {
		for _, v := range values {
			ch <- SearchResult{Value: v}
		}
		return nil, nil
	}
}

const (
	sortNone  = 0
	sortAlpha = 1
	sortScore = 2
	sortAsc   = 0
	sortDesc  = 1
)

func TestSearchValueSet_NoResults(t *testing.T) {
	vs := NewSearchValueSet(producerOf(), sortNone, sortAsc, 0, 0, nil)
	results, warns, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Empty(t, warns)
	assert.Empty(t, results)
}

func TestSearchValueSet_ProducerError(t *testing.T) {
	producerErr := errors.New("index read error")
	produce := func(ch chan<- SearchResult) (annotations.Annotations, error) {
		ch <- SearchResult{Value: "aaa"}
		return nil, producerErr
	}

	t.Run("unsorted", func(t *testing.T) {
		vs := NewSearchValueSet(produce, sortNone, sortAsc, 0, 0, nil)
		// The one value sent before the error is still returned.
		results, _, err := collectAll(t, vs)
		assert.Equal(t, producerErr, err)
		assert.Equal(t, []SearchResult{{Value: "aaa"}}, results)
	})

	t.Run("sorted", func(t *testing.T) {
		vs := NewSearchValueSet(produce, sortAlpha, sortAsc, 0, 0, nil)
		// Producer error means Next() must return false immediately (no partial results).
		results, _, err := collectAll(t, vs)
		assert.Equal(t, producerErr, err)
		assert.Empty(t, results)
	})
}

func TestSearchValueSet_ProducerWarning(t *testing.T) {
	warn := errors.New("truncated results")
	produce := func(ch chan<- SearchResult) (annotations.Annotations, error) {
		ch <- SearchResult{Value: "aaa"}
		ch <- SearchResult{Value: "bbb"}
		a := annotations.New()
		a.Add(warn)
		return *a, nil
	}

	for _, sorted := range []bool{false, true} {
		name := "unsorted"
		sortBy := sortNone
		if sorted {
			name = "sorted"
			sortBy = sortAlpha
		}
		t.Run(name, func(t *testing.T) {
			vs := NewSearchValueSet(produce, sortBy, sortAsc, 0, 0, nil)
			results, warns, err := collectAll(t, vs)
			require.NoError(t, err)
			assert.Len(t, results, 2)
			_, hasWarn := warns[warn.Error()]
			assert.True(t, hasWarn, "expected warning to be propagated")
		})
	}
}

func TestSearchValueSet_Deduplication(t *testing.T) {
	vs := NewSearchValueSet(producerOf("aaa", "bbb", "aaa", "ccc", "bbb"), sortNone, sortAsc, 0, 0, nil)
	results, _, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []SearchResult{
		{Value: "aaa"},
		{Value: "bbb"},
		{Value: "ccc"},
	}, results)
}

func TestSearchValueSet_LimitUnsorted(t *testing.T) {
	vs := NewSearchValueSet(producerOf("aaa", "bbb", "ccc", "ddd"), sortNone, sortAsc, 2, 0, nil)
	results, _, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []SearchResult{
		{Value: "aaa"},
		{Value: "bbb"},
	}, results)
}

func TestSearchValueSet_LimitSortedAlphaAsc(t *testing.T) {
	// Producer sends in reverse order; sort+limit should give us the first 2 alphabetically.
	vs := NewSearchValueSet(producerOf("ddd", "aaa", "ccc", "bbb"), sortAlpha, sortAsc, 2, 0, nil)
	results, _, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []SearchResult{
		{Value: "aaa"},
		{Value: "bbb"},
	}, results)
}

func TestSearchValueSet_LimitSortedAlphaDesc(t *testing.T) {
	vs := NewSearchValueSet(producerOf("aaa", "bbb", "ccc", "ddd"), sortAlpha, sortDesc, 2, 0, nil)
	results, _, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []SearchResult{
		{Value: "ddd"},
		{Value: "ccc"},
	}, results)
}

func TestSearchValueSet_MaxBytesSorted(t *testing.T) {
	// "aaa" = 3 bytes, "bbb" = 3 bytes → 6 bytes total. Limit to 5 bytes so second value exceeds.
	vs := NewSearchValueSet(producerOf("aaa", "bbb"), sortAlpha, sortAsc, 0, 5, nil)
	results, _, err := collectAll(t, vs)
	assert.ErrorContains(t, err, "buffer exceeded max size")
	assert.Empty(t, results)
}

func TestSearchValueSet_MaxBytesNotExceeded(t *testing.T) {
	// Exactly at the limit should not error.
	vs := NewSearchValueSet(producerOf("aaa", "bbb"), sortAlpha, sortAsc, 0, 6, nil)
	results, _, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestSearchValueSet_SortedDeduplication(t *testing.T) {
	// Duplicates across the input should appear only once in sorted output.
	vs := NewSearchValueSet(producerOf("bbb", "aaa", "bbb", "ccc", "aaa"), sortAlpha, sortAsc, 0, 0, nil)
	results, _, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []SearchResult{
		{Value: "aaa"},
		{Value: "bbb"},
		{Value: "ccc"},
	}, results)
}

func TestSearchValueSet_SortedDeduplicationDesc(t *testing.T) {
	vs := NewSearchValueSet(producerOf("bbb", "aaa", "bbb", "ccc", "aaa"), sortAlpha, sortDesc, 0, 0, nil)
	results, _, err := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []SearchResult{
		{Value: "ccc"},
		{Value: "bbb"},
		{Value: "aaa"},
	}, results)
}

func TestSearchValueSet_ContextCancelledUnsorted(t *testing.T) {
	// Simulate context cancellation: producer sends a few values then returns context.Canceled.
	produce := func(ch chan<- SearchResult) (annotations.Annotations, error) {
		ch <- SearchResult{Value: "aaa"}
		ch <- SearchResult{Value: "bbb"}
		return nil, context.Canceled
	}
	vs := NewSearchValueSet(produce, sortNone, sortAsc, 0, 0, nil)
	results, _, err := collectAll(t, vs)
	assert.ErrorIs(t, err, context.Canceled)
	// Partial results sent before cancellation are still returned.
	assert.Equal(t, []SearchResult{{Value: "aaa"}, {Value: "bbb"}}, results)
}

func TestSearchValueSet_ContextCancelledSorted(t *testing.T) {
	// Simulate context cancellation mid-drain: producer sends a few values then returns context.Canceled.
	produce := func(ch chan<- SearchResult) (annotations.Annotations, error) {
		ch <- SearchResult{Value: "bbb"}
		ch <- SearchResult{Value: "aaa"}
		return nil, context.Canceled
	}
	vs := NewSearchValueSet(produce, sortAlpha, sortAsc, 0, 0, nil)
	// Producer error must suppress all results on the sorted path.
	results, _, err := collectAll(t, vs)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, results)
}

func TestSearchValueSet_CloseUnblocksProducer(t *testing.T) {
	// Producer writes more values than the channel buffer; Close must drain so it doesn't block.
	produce := func(ch chan<- SearchResult) (annotations.Annotations, error) {
		for i := range SearchValueSetChannelSize * 3 {
			ch <- SearchResult{Value: string(rune('a' + i%26))}
		}
		return nil, nil
	}
	vs := NewSearchValueSet(produce, sortNone, sortAsc, 0, 0, nil)
	// Read just one value, then close without exhausting.
	require.True(t, vs.Next())
	vs.Close()
	// Err/Warnings must not deadlock.
	assert.NoError(t, vs.Err())
}

// generateValues returns n unique label-value strings of the form "label_XXXXXXXX".
func generateValues(n int) []string {
	vals := make([]string, n)
	for i := range n {
		vals[i] = fmt.Sprintf("label_%08d", i)
	}
	return vals
}

// BenchmarkSearchValueSet_Unsorted measures the unsorted streaming path (no sort, no limit).
func BenchmarkSearchValueSet_Unsorted(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		vals := generateValues(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				vs := NewSearchValueSet(producerOf(vals...), sortNone, sortAsc, 0, 0, nil)
				for vs.Next() {
				}
				vs.Close()
			}
		})
	}
}

// BenchmarkSearchValueSet_Sorted measures the sorted path (drain-all then sort).
func BenchmarkSearchValueSet_Sorted(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		vals := generateValues(n)
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				vs := NewSearchValueSet(producerOf(vals...), sortAlpha, sortAsc, 0, 0, nil)
				for vs.Next() {
				}
				vs.Close()
			}
		})
	}
}

// BenchmarkSearchValueSet_UnsortedWithLimit measures the unsorted path with an early-exit limit.
func BenchmarkSearchValueSet_UnsortedWithLimit(b *testing.B) {
	const n = 100_000
	const limit = 100
	vals := generateValues(n)
	b.ReportAllocs()
	for range b.N {
		vs := NewSearchValueSet(producerOf(vals...), sortNone, sortAsc, limit, 0, nil)
		for vs.Next() {
		}
		vs.Close()
	}
}

// BenchmarkSearchValueSet_UnsortedHighDuplication measures dedup overhead when most values are duplicates.
func BenchmarkSearchValueSet_UnsortedHighDuplication(b *testing.B) {
	// 10k unique values each repeated 10 times = 100k total sent.
	unique := generateValues(10_000)
	vals := make([]string, 0, 100_000)
	for range 10 {
		vals = append(vals, unique...)
	}
	b.ReportAllocs()
	for range b.N {
		vs := NewSearchValueSet(producerOf(vals...), sortNone, sortAsc, 0, 0, nil)
		for vs.Next() {
		}
		vs.Close()
	}
}
