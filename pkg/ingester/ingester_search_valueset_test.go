// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// collectAll drains the value set and returns all results, the first error, and all warnings.
func collectAll(t *testing.T, vs *ingesterSearcherValueSet) ([]client.SearchLabelValuesResult, error, annotations.Annotations) {
	t.Helper()
	var results []client.SearchLabelValuesResult
	for vs.Next() {
		results = append(results, vs.At())
	}
	return results, vs.Err(), vs.Warnings()
}

// producerOf returns a producer that sends the given values and returns no error/warnings.
func producerOf(values ...string) func(ch chan<- string) (annotations.Annotations, error) {
	return func(ch chan<- string) (annotations.Annotations, error) {
		for _, v := range values {
			ch <- v
		}
		return nil, nil
	}
}

func TestIngesterSearcherValueSet_NoResults(t *testing.T) {
	vs := newingesterSearcherValueSet(producerOf(), nil, 0, 0)
	results, err, warns := collectAll(t, vs)
	require.NoError(t, err)
	assert.Empty(t, warns)
	assert.Empty(t, results)
}

func TestIngesterSearcherValueSet_ProducerError(t *testing.T) {
	producerErr := errors.New("index read error")
	produce := func(ch chan<- string) (annotations.Annotations, error) {
		ch <- "aaa"
		return nil, producerErr
	}

	t.Run("unsorted", func(t *testing.T) {
		vs := newingesterSearcherValueSet(produce, nil, 0, 0)
		// The one value sent before the error is still returned.
		results, err, _ := collectAll(t, vs)
		assert.Equal(t, producerErr, err)
		assert.Equal(t, []client.SearchLabelValuesResult{{Value: "aaa"}}, results)
	})

	t.Run("sorted", func(t *testing.T) {
		sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC}
		vs := newingesterSearcherValueSet(produce, sf, 0, 0)
		// Producer error means Next() must return false immediately (no partial results).
		results, err, _ := collectAll(t, vs)
		assert.Equal(t, producerErr, err)
		assert.Empty(t, results)
	})
}

func TestIngesterSearcherValueSet_ProducerWarning(t *testing.T) {
	warn := errors.New("truncated results")
	produce := func(ch chan<- string) (annotations.Annotations, error) {
		ch <- "aaa"
		ch <- "bbb"
		a := annotations.New()
		a.Add(warn)
		return *a, nil
	}

	for _, sorted := range []bool{false, true} {
		name := "unsorted"
		var sf *client.SearchLabelValuesFilter
		if sorted {
			name = "sorted"
			sf = &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC}
		}
		t.Run(name, func(t *testing.T) {
			vs := newingesterSearcherValueSet(produce, sf, 0, 0)
			results, err, warns := collectAll(t, vs)
			require.NoError(t, err)
			assert.Len(t, results, 2)
			_, hasWarn := warns[warn.Error()]
			assert.True(t, hasWarn, "expected warning to be propagated")
		})
	}
}

func TestIngesterSearcherValueSet_Deduplication(t *testing.T) {
	vs := newingesterSearcherValueSet(producerOf("aaa", "bbb", "aaa", "ccc", "bbb"), nil, 0, 0)
	results, err, _ := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []client.SearchLabelValuesResult{
		{Value: "aaa"},
		{Value: "bbb"},
		{Value: "ccc"},
	}, results)
}

func TestIngesterSearcherValueSet_LimitUnsorted(t *testing.T) {
	vs := newingesterSearcherValueSet(producerOf("aaa", "bbb", "ccc", "ddd"), nil, 2, 0)
	results, err, _ := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []client.SearchLabelValuesResult{
		{Value: "aaa"},
		{Value: "bbb"},
	}, results)
}

func TestIngesterSearcherValueSet_LimitSortedAlphaAsc(t *testing.T) {
	// Producer sends in reverse order; sort+limit should give us the first 2 alphabetically.
	sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC}
	vs := newingesterSearcherValueSet(producerOf("ddd", "aaa", "ccc", "bbb"), sf, 2, 0)
	results, err, _ := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []client.SearchLabelValuesResult{
		{Value: "aaa"},
		{Value: "bbb"},
	}, results)
}

func TestIngesterSearcherValueSet_LimitSortedAlphaDesc(t *testing.T) {
	sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_DESC}
	vs := newingesterSearcherValueSet(producerOf("aaa", "bbb", "ccc", "ddd"), sf, 2, 0)
	results, err, _ := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []client.SearchLabelValuesResult{
		{Value: "ddd"},
		{Value: "ccc"},
	}, results)
}

func TestIngesterSearcherValueSet_MaxBytesSorted(t *testing.T) {
	// "aaa" = 3 bytes, "bbb" = 3 bytes → 6 bytes total. Limit to 5 bytes so second value exceeds.
	sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC}
	vs := newingesterSearcherValueSet(producerOf("aaa", "bbb"), sf, 0, 5)
	results, err, _ := collectAll(t, vs)
	assert.ErrorContains(t, err, "buffer exceeded max size")
	assert.Empty(t, results)
}

func TestIngesterSearcherValueSet_MaxBytesNotExceeded(t *testing.T) {
	// Exactly at the limit should not error.
	sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC}
	vs := newingesterSearcherValueSet(producerOf("aaa", "bbb"), sf, 0, 6)
	results, err, _ := collectAll(t, vs)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

func TestIngesterSearcherValueSet_SortedDeduplication(t *testing.T) {
	// Duplicates across the input should appear only once in sorted output.
	sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC}
	vs := newingesterSearcherValueSet(producerOf("bbb", "aaa", "bbb", "ccc", "aaa"), sf, 0, 0)
	results, err, _ := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []client.SearchLabelValuesResult{
		{Value: "aaa"},
		{Value: "bbb"},
		{Value: "ccc"},
	}, results)
}

func TestIngesterSearcherValueSet_SortedDeduplicationDesc(t *testing.T) {
	sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_DESC}
	vs := newingesterSearcherValueSet(producerOf("bbb", "aaa", "bbb", "ccc", "aaa"), sf, 0, 0)
	results, err, _ := collectAll(t, vs)
	require.NoError(t, err)
	assert.Equal(t, []client.SearchLabelValuesResult{
		{Value: "ccc"},
		{Value: "bbb"},
		{Value: "aaa"},
	}, results)
}

func TestIngesterSearcherValueSet_ContextCancelledUnsorted(t *testing.T) {
	// Simulate context cancellation: producer sends a few values then returns context.Canceled.
	produce := func(ch chan<- string) (annotations.Annotations, error) {
		ch <- "aaa"
		ch <- "bbb"
		return nil, context.Canceled
	}
	vs := newingesterSearcherValueSet(produce, nil, 0, 0)
	results, err, _ := collectAll(t, vs)
	assert.ErrorIs(t, err, context.Canceled)
	// Partial results sent before cancellation are still returned.
	assert.Equal(t, []client.SearchLabelValuesResult{{Value: "aaa"}, {Value: "bbb"}}, results)
}

func TestIngesterSearcherValueSet_ContextCancelledSorted(t *testing.T) {
	// Simulate context cancellation mid-drain: producer sends a few values then returns context.Canceled.
	produce := func(ch chan<- string) (annotations.Annotations, error) {
		ch <- "bbb"
		ch <- "aaa"
		return nil, context.Canceled
	}
	sf := &client.SearchLabelValuesFilter{SortBy: client.SORT_BY_ALPHA, SortOrder: client.SORT_ORDER_ASC}
	vs := newingesterSearcherValueSet(produce, sf, 0, 0)
	// Producer error must suppress all results on the sorted path.
	results, err, _ := collectAll(t, vs)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, results)
}

func TestIngesterSearcherValueSet_CloseUnblocksProducer(t *testing.T) {
	// Producer writes more values than the channel buffer; Close must drain so it doesn't block.
	produce := func(ch chan<- string) (annotations.Annotations, error) {
		for i := range internalChannelSize * 3 {
			ch <- string(rune('a' + i%26))
		}
		return nil, nil
	}
	vs := newingesterSearcherValueSet(produce, nil, 0, 0)
	// Read just one value, then close without exhausting.
	require.True(t, vs.Next())
	vs.Close()
	// Err/Warnings must not deadlock.
	assert.NoError(t, vs.Err())
}
