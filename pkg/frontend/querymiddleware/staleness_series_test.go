// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"errors"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// mockSeriesSet implements storage.SeriesSet for testing
type mockSeriesSet struct { // TODO dimitarvdimitrov unify with seriesIteratorMock
	series []storage.Series
	next   int
	err    error
	warn   annotations.Annotations
}

func (m *mockSeriesSet) Next() bool {
	m.next++
	return m.next <= len(m.series)
}

func (m *mockSeriesSet) At() storage.Series {
	if m.next <= 0 || m.next > len(m.series) {
		return nil
	}
	return m.series[m.next-1]
}

func (m *mockSeriesSet) Err() error {
	return m.err
}

func (m *mockSeriesSet) Warnings() annotations.Annotations {
	return m.warn
}

// mockSeries implements storage.Series for testing
type mockSeries struct {
	lbs labels.Labels
}

func (m mockSeries) Labels() labels.Labels {
	return m.lbs
}

func (m mockSeries) Iterator(chunkenc.Iterator) chunkenc.Iterator {
	return nil
}

func TestSeriesSetsHeap_SeriesSetInterface(t *testing.T) {
	t.Run("next without sets", func(t *testing.T) {
		h := concatSeriesSets(nil)

		assert.False(t, h.Next(), "Next should return false for empty heap")
		assert.Nil(t, h.At(), "At should return nil for empty heap")
	})

	t.Run("next with single set", func(t *testing.T) {
		series1 := &mockSeries{lbs: labels.FromStrings("a", "1")}
		series2 := &mockSeries{lbs: labels.FromStrings("b", "2")}

		set := &mockSeriesSet{series: []storage.Series{series1, series2}}

		h := concatSeriesSets([]storage.SeriesSet{set})

		// First Next should return true and point to first series
		assert.True(t, h.Next())
		assert.Equal(t, series1, h.At())

		// Second Next should return true and advance to second series
		assert.True(t, h.Next())
		assert.Equal(t, series2, h.At())

		// Third Next should return false (no more series)
		assert.False(t, h.Next())
	})

	t.Run("next with multiple sets", func(t *testing.T) {
		// Create series with different labels
		seriesA1 := &mockSeries{lbs: labels.FromStrings("a", "1")}
		seriesA2 := &mockSeries{lbs: labels.FromStrings("a", "2")}
		seriesB1 := &mockSeries{lbs: labels.FromStrings("b", "1")}
		seriesB2 := &mockSeries{lbs: labels.FromStrings("b", "2")}
		seriesC1 := &mockSeries{lbs: labels.FromStrings("c", "1")}

		// Create series sets
		set1 := &mockSeriesSet{series: []storage.Series{seriesA1, seriesB1}}
		set2 := &mockSeriesSet{series: []storage.Series{seriesA2, seriesC1}}
		set3 := &mockSeriesSet{series: []storage.Series{seriesB2}}

		// Create heap and add sets
		h := concatSeriesSets([]storage.SeriesSet{set1, set2, set3})

		// Expected order by labels should be:
		// a:1, a:2, b:1, b:2, c:1

		assert.True(t, h.Next())
		assert.Equal(t, seriesA1.Labels(), h.At().Labels())

		assert.True(t, h.Next())
		assert.Equal(t, seriesA2.Labels(), h.At().Labels())

		assert.True(t, h.Next())
		assert.Equal(t, seriesB1.Labels(), h.At().Labels())

		assert.True(t, h.Next())
		assert.Equal(t, seriesB2.Labels(), h.At().Labels())

		assert.True(t, h.Next())
		assert.Equal(t, seriesC1.Labels(), h.At().Labels())

		assert.False(t, h.Next())
	})
}

func TestSeriesSetsHeap_ErrorHandling(t *testing.T) {
	t.Run("empty heap returns no errors", func(t *testing.T) {
		h := concatSeriesSets(nil)
		assert.NoError(t, h.Err())
		assert.Empty(t, h.Warnings())
	})

	t.Run("collects errors from sets", func(t *testing.T) {
		set1 := &mockSeriesSet{
			series: []storage.Series{&mockSeries{lbs: labels.FromStrings("a", "1")}},
			err:    errors.New("error from set 1"),
		}
		set2 := &mockSeriesSet{
			series: []storage.Series{&mockSeries{lbs: labels.FromStrings("b", "1")}},
			err:    errors.New("error from set 2"),
		}

		h := concatSeriesSets([]storage.SeriesSet{set1, set2})

		// Next exhausts all sets, collecting errors
		for h.Next() {
			// Consume all series
		}

		// Check that errors were collected
		err := h.Err()
		assert.Error(t, err)

		errString := err.Error()
		assert.Contains(t, errString, "error from set 1")
		assert.Contains(t, errString, "error from set 2")
	})

	t.Run("collects warnings from sets", func(t *testing.T) {
		warning1 := annotations.New().Add(errors.New("warning 1"))
		warning2 := annotations.New().Add(errors.New("warning 2"))

		set1 := &mockSeriesSet{
			series: []storage.Series{&mockSeries{lbs: labels.FromStrings("a", "1")}},
			warn:   warning1,
		}
		set2 := &mockSeriesSet{
			series: []storage.Series{&mockSeries{lbs: labels.FromStrings("b", "1")}},
			warn:   warning2,
		}

		h := concatSeriesSets([]storage.SeriesSet{set1, set2})

		// Consume all series to collect warnings
		for h.Next() {
			// Just consume the series
		}

		// Get warnings
		warnings := h.Warnings()

		// Check that warnings were collected (by converting to strings)
		warningStrings := make([]string, 0, len(warnings))
		for _, w := range warnings {
			warningStrings = append(warningStrings, w.Error())
		}

		assert.Contains(t, warningStrings, "warning 1")
		assert.Contains(t, warningStrings, "warning 2")
	})
}

func TestConcatSeriesSets(t *testing.T) {
	t.Run("empty sets array", func(t *testing.T) {
		h := concatSeriesSets(nil)
		assert.False(t, h.Next())
		assert.NoError(t, h.Err())
	})

	t.Run("sets with errors before first element", func(t *testing.T) {
		// Create a set that returns an error without any series
		errorSet := &mockSeriesSet{
			err:  errors.New("set failed"),
			warn: annotations.New().Add(errors.New("set warning")),
		}

		h := concatSeriesSets([]storage.SeriesSet{errorSet})

		// The error should be collected during initialization
		assert.False(t, h.Next())
		assert.Error(t, h.Err())
		assert.Contains(t, h.Err().Error(), "set failed")

		// The warning should also be collected
		warnings := h.Warnings()
		assert.Len(t, warnings, 1)
		assert.Contains(t, warnings, "set warning")
	})

	t.Run("sets with mixed success and errors", func(t *testing.T) {
		// Create a set with series but no error
		successSet := &mockSeriesSet{
			series: []storage.Series{&mockSeries{lbs: labels.FromStrings("a", "1")}},
		}

		// Create a set that fails before first element
		errorSet := &mockSeriesSet{
			err: errors.New("error before any series"),
		}

		h := concatSeriesSets([]storage.SeriesSet{successSet, errorSet})

		// Should have the one series from successSet
		assert.True(t, h.Next())
		assert.Equal(t, labels.FromStrings("a", "1"), h.At().Labels())

		// No more series
		assert.False(t, h.Next())

		// Should have collected the error
		assert.Error(t, h.Err())
		assert.Contains(t, h.Err().Error(), "error before any series")
	})

	t.Run("concat multiple series with same label set", func(t *testing.T) {
		// Create sets with the same labels but different values
		set1 := &mockSeriesSet{
			series: []storage.Series{&mockSeries{lbs: labels.FromStrings("a", "1")}},
		}
		set2 := &mockSeriesSet{
			series: []storage.Series{&mockSeries{lbs: labels.FromStrings("a", "1")}},
		}

		h := concatSeriesSets([]storage.SeriesSet{set1, set2})

		// Should see both series since heap doesn't deduplicate
		assert.True(t, h.Next())
		assert.Equal(t, labels.FromStrings("a", "1"), h.At().Labels())

		assert.True(t, h.Next())
		assert.Equal(t, labels.FromStrings("a", "1"), h.At().Labels())

		assert.False(t, h.Next())
	})

	t.Run("same labels from multiple sets aren't merged", func(t *testing.T) {
		// Create multiple series with identical labels across different sets
		series1a := &mockSeries{lbs: labels.FromStrings("a", "1")}
		series1b := &mockSeries{lbs: labels.FromStrings("a", "1")} // Same labels as 1a
		series1c := &mockSeries{lbs: labels.FromStrings("a", "1")} // Same labels as 1a and 1b

		// Create series sets with those series
		set1 := &mockSeriesSet{series: []storage.Series{series1a}}
		set2 := &mockSeriesSet{series: []storage.Series{series1b}}
		set3 := &mockSeriesSet{series: []storage.Series{series1c}}

		h := concatSeriesSets([]storage.SeriesSet{set1, set2, set3})

		// We should get all three series in sequence, despite having the same labels
		assert.True(t, h.Next())
		firstSeries := h.At()
		assert.Equal(t, labels.FromStrings("a", "1"), firstSeries.Labels())

		assert.True(t, h.Next())
		secondSeries := h.At()
		assert.Equal(t, labels.FromStrings("a", "1"), secondSeries.Labels())

		assert.True(t, h.Next())
		thirdSeries := h.At()
		assert.Equal(t, labels.FromStrings("a", "1"), thirdSeries.Labels())

		// No more series
		assert.False(t, h.Next())

		// Verify these are actually different series objects
		// This is an implementation detail, but important to test that they aren't merged
		assert.NotSame(t, firstSeries, secondSeries)
		assert.NotSame(t, thirdSeries, secondSeries)
		assert.NotSame(t, firstSeries, thirdSeries)
	})

	t.Run("interleaved series from multiple sets", func(t *testing.T) {
		// Create series with interleaved label ordering across different sets
		seriesA := &mockSeries{lbs: labels.FromStrings("a", "1")}
		seriesC := &mockSeries{lbs: labels.FromStrings("c", "1")}
		seriesE := &mockSeries{lbs: labels.FromStrings("e", "1")}

		seriesB := &mockSeries{lbs: labels.FromStrings("b", "1")}
		seriesD := &mockSeries{lbs: labels.FromStrings("d", "1")}
		seriesF := &mockSeries{lbs: labels.FromStrings("f", "1")}

		// Create sets with non-sequential series
		set1 := &mockSeriesSet{series: []storage.Series{seriesA, seriesC, seriesE}}
		set2 := &mockSeriesSet{series: []storage.Series{seriesB, seriesD, seriesF}}

		h := concatSeriesSets([]storage.SeriesSet{set1, set2})

		// We should get all series in alphabetical order by label
		expectedOrder := []labels.Labels{
			labels.FromStrings("a", "1"),
			labels.FromStrings("b", "1"),
			labels.FromStrings("c", "1"),
			labels.FromStrings("d", "1"),
			labels.FromStrings("e", "1"),
			labels.FromStrings("f", "1"),
		}

		for i, expected := range expectedOrder {
			assert.True(t, h.Next(), "Expected to get series %d", i)
			assert.Equal(t, expected, h.At().Labels(), "Series %d has wrong labels", i)
		}

		// No more series
		assert.False(t, h.Next())
	})
}

func TestStalenessMarkerIterator(t *testing.T) {
	testCases := []struct {
		name     string
		samples  []mimirpb.Sample
		step     int64
		testFunc func(t *testing.T, iter chunkenc.Iterator) // TODO dimitarvdimitrov remote *testing.T arg because its unused
	}{
		{
			name: "seek forward with normal data",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 30, Value: 3.0},
				{TimestampMs: 40, Value: 4.0},
			},
			step: 5,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Seek(25)
			},
		},
		{
			name: "next forward with normal data",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 30, Value: 3.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Next()
				iter.Next()
				iter.Next()
				iter.Next()
				iter.Next()
			},
		},
		{
			name: "seek forward with gap larger than step",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 40, Value: 4.0},
				{TimestampMs: 50, Value: 5.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				// First get to time 20
				iter.Next()
				iter.Next()

				// Seek to time 30 - should insert staleness marker
				iter.Seek(30)

				// Next should return 40
				iter.Next()
			},
		},
		{
			name: "seek to end of series",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 30, Value: 3.0},
			},
			step: 5,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				// Seek past the end at time 40
				iter.Seek(40)

				// Next should be ValNone
				iter.Next()
			},
		},
		{
			name: "seek to same position doesn't move cursor",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 30, Value: 3.0},
			},
			step: 5,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Seek(20)

				// Seek to same position again
				iter.Seek(20)
			},
		},
		{
			name: "seek backward doesn't move cursor",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 30, Value: 3.0},
			},
			step: 5,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				// Seek to 25
				iter.Seek(25)

				// Try to seek backward to 15
				iter.Seek(15)
			},
		},
		{
			name: "seek to exact boundary with gap",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 30, Value: 3.0}, // Gap of 20 units
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				// Seek to 20 (exactly in the gap)
				iter.Seek(20)

				// Next should bring us to 30
				iter.Next()
			},
		},
		{
			name: "seek to before start",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				// Seek to before the start
				iter.Seek(1)

				// Next should bring us to 20
				iter.Next()

				// Next should return false
				iter.Next()
			},
		},
		{
			name: "seek to negative timestamp",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				// Seek to negative timestamp
				iter.Seek(-10)

				// Next should bring us to 20
				iter.Next()

				// Next should return false
				iter.Next()
			},
		},

		{
			name:    "iterating over no samples",
			samples: []mimirpb.Sample{},
			step:    10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Seek(10)
			},
		},
		{
			name:    "iterating over no samples",
			samples: []mimirpb.Sample{},
			step:    10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Next()
				iter.Seek(10)
			},
		},
		{
			name: "seeking to beyond the last sample",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 30, Value: 3.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Seek(31)
			},
		},
		{
			name: "a very large gap between samples",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 100, Value: 10.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Next()
				iter.Seek(70)
				iter.Next()
			},
		},
		{
			name: "a very large gap between Seeks",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 69, Value: 6.9},
				{TimestampMs: 100, Value: 10.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Next()
				iter.Seek(78) // This should give us the staleness marker at 79
				iter.Next()
			},
		},
		{
			name: "seeking to right after a staleness marker after a read",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 69, Value: 6.9},
				{TimestampMs: 100, Value: 10.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Next()
				iter.Seek(80) // There was a staleness marker at 79
				iter.Next()
			},
		},
		{
			name: "seeking to right after a staleness marker",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 69, Value: 6.9},
				{TimestampMs: 100, Value: 10.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Seek(80) // There was a staleness marker at 79
				iter.Next()
			},
		},
		{
			name: "seeking much after the last sample",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 20, Value: 2.0},
				{TimestampMs: 69, Value: 6.9},
				{TimestampMs: 100, Value: 10.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Seek(1000)
				iter.Next()
			},
		},
		{
			name: "samples with smaller than step intervals",
			samples: []mimirpb.Sample{
				{TimestampMs: 10, Value: 1.0},
				{TimestampMs: 12, Value: 1.2},
				{TimestampMs: 14, Value: 1.4},
				{TimestampMs: 16, Value: 1.6},
				{TimestampMs: 18, Value: 1.8},
				{TimestampMs: 30, Value: 3.0},
				{TimestampMs: 100, Value: 10.0},
			},
			step: 10,
			testFunc: func(t *testing.T, iter chunkenc.Iterator) {
				iter.Seek(14)
				iter.Next()
				iter.Next()
				iter.Next()
				iter.Next()
				iter.Next()
				iter.Next()
				iter.Next()
				iter.Next()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create the mock iterator implementation
			mockIter := newMockIterator(tc.samples)
			actual := newStalenessMarkerIterator(mockIter, tc.step)

			// Create the SeriesSet implementation
			ss := newSeriesSetFromEmbeddedQueriesResults([][]SampleStream{{{Samples: tc.samples}}}, &storage.SelectHints{Step: tc.step})
			require.True(t, ss.Next())
			expected := ss.At().Iterator(nil)

			// Create a comparing iterator
			compIter := newComparingIterator(t, expected, actual)

			// Run the test
			tc.testFunc(t, compIter)
		})
	}
}

func TestNewSeriesSetFromEmbeddedQueriesResults(t *testing.T) {
	tests := map[string]struct {
		input    []SampleStream
		hints    *storage.SelectHints
		expected []SampleStream
	}{
		"should add a stale marker at the end even if if input samples have no gaps": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 10},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}},
		},
		"should add stale markers at the beginning of each gap and one at the end of the series": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 10},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 20, Value: math.Float64frombits(value.StaleNaN)}, {TimestampMs: 40, Value: 4}, {TimestampMs: 50, Value: math.Float64frombits(value.StaleNaN)}, {TimestampMs: 90, Value: 9}, {TimestampMs: 100, Value: math.Float64frombits(value.StaleNaN)}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}, {TimestampMs: 40, Value: math.Float64frombits(value.StaleNaN)}},
			}},
		},
		"should not add stale markers even if points have gaps if hints is not passed": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: nil,
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
		},
		"should not add stale markers even if points have gaps if step == 0": {
			input: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
			hints: &storage.SelectHints{Step: 0},
			expected: []SampleStream{{
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "1"}},
				Samples: []mimirpb.Sample{{TimestampMs: 10, Value: 1}, {TimestampMs: 40, Value: 4}, {TimestampMs: 90, Value: 9}},
			}, {
				Labels:  []mimirpb.LabelAdapter{{Name: "a", Value: "b"}},
				Samples: []mimirpb.Sample{{TimestampMs: 20, Value: 2}, {TimestampMs: 30, Value: 3}},
			}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			step := int64(0)
			if testData.hints != nil {
				step = testData.hints.Step
			}
			set := newStalenessMarkerSeriesSet(sampleStreamToSeriesSet(testData.input), step)
			actual, err := seriesSetToSampleStreams(set)
			require.NoError(t, err)
			assertEqualSampleStream(t, testData.expected, actual)
		})
	}
}

// comparingIterator wraps two iterators and compares their behavior
type comparingIterator struct {
	t        *testing.T
	expected chunkenc.Iterator
	actual   chunkenc.Iterator
}

func newComparingIterator(t *testing.T, expected, actual chunkenc.Iterator) *comparingIterator {
	return &comparingIterator{
		t:        t,
		expected: expected,
		actual:   actual,
	}
}

func (c *comparingIterator) Next() chunkenc.ValueType {
	vtExpected := c.expected.Next()
	vtActual := c.actual.Next()

	c.t.Logf("Next() returned value types: %v and %v", vtExpected, vtActual)
	require.Equal(c.t, vtExpected.String(), vtActual.String(), "Next() should return the same value type")

	if vtExpected != chunkenc.ValNone {
		c.compareValues()
	}

	return vtExpected
}

func (c *comparingIterator) Seek(t int64) chunkenc.ValueType {
	vtExpected := c.expected.Seek(t)
	vtActual := c.actual.Seek(t)

	require.Equal(c.t, vtExpected.String(), vtActual.String(), "Seek() should return the same value type")

	if vtExpected != chunkenc.ValNone {
		c.compareValues()
	}

	return vtExpected
}

func (c *comparingIterator) At() (int64, float64) {
	tsExpected, vExpected := c.expected.At()
	tsActual, vActual := c.actual.At()
	assert.Equal(c.t, tsExpected, tsActual, "At() should return the same timestamp")
	assert.Equal(c.t, vExpected, vActual, "At() should return the same value")
	return tsExpected, vExpected
}

func (c *comparingIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	tsExpected, hExpected := c.expected.AtHistogram(h)
	tsActual, hActual := c.actual.AtHistogram(h)
	require.Equal(c.t, tsExpected, tsActual, "AtHistogram() should return the same timestamp")
	require.Equal(c.t, hExpected, hActual, "AtHistogram() should return the same histogram")
	return tsExpected, hExpected
}

func (c *comparingIterator) AtFloatHistogram(h *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	tsExpected, hExpected := c.expected.AtFloatHistogram(h)
	tsActual, hActual := c.actual.AtFloatHistogram(h)
	require.Equal(c.t, tsExpected, tsActual, "AtFloatHistogram() should return the same timestamp")
	require.Equal(c.t, hExpected, hActual, "AtFloatHistogram() should return the same histogram")
	return tsExpected, hExpected
}

func (c *comparingIterator) AtT() int64 {
	tExpected := c.expected.AtT()
	tActual := c.actual.AtT()

	require.Equal(c.t, tExpected, tActual, "AtT() should return the same timestamp")

	return tExpected
}

func (c *comparingIterator) Err() error {
	errExpected := c.expected.Err()
	errActual := c.actual.Err()

	require.Equal(c.t, errExpected, errActual, "Err() should return the same error")
	return errExpected
}

// compareValues compares the current values of both iterators
func (c *comparingIterator) compareValues() {
	errExpected := c.expected.Err()
	errActual := c.actual.Err()

	assert.Equal(c.t, errExpected, errActual, "Err() should return the same error")

	tsExpected, vExpected := c.expected.At()
	tsActual, vActual := c.actual.At()

	c.t.Logf("At() returned timestamps: expected %d got %d", tsExpected, tsActual)
	c.t.Logf("At() returned values: expected %v got %v", vExpected, vActual)

	assert.Equal(c.t, tsExpected, tsActual, "Timestamps should match")

	// Special handling for NaN values
	if math.IsNaN(vExpected) && math.IsNaN(vActual) {
		isStaleExpected := value.IsStaleNaN(vExpected)
		isStaleActual := value.IsStaleNaN(vActual)
		assert.Equal(c.t, isStaleExpected, isStaleActual, "NaN types should match (stale vs. regular)")
	} else {
		assert.Equal(c.t, vExpected, vActual, "Values should match")
	}
}

// mockIterator implements chunkenc.Iterator for testing
type mockIterator struct {
	samples []mimirpb.Sample
	pos     int
}

func newMockIterator(samples []mimirpb.Sample) *mockIterator {
	return &mockIterator{
		samples: samples,
		pos:     -1,
	}
}

func (it *mockIterator) Next() chunkenc.ValueType {
	if it.pos >= len(it.samples)-1 {
		return chunkenc.ValNone
	}
	it.pos++
	return chunkenc.ValFloat
}

func (it *mockIterator) Seek(t int64) chunkenc.ValueType {
	for i, s := range it.samples {
		if s.TimestampMs >= t {
			it.pos = i
			return chunkenc.ValFloat
		}
	}
	it.pos = len(it.samples)
	return chunkenc.ValNone
}

func (it *mockIterator) At() (int64, float64) {
	if it.pos < 0 || it.pos >= len(it.samples) {
		return 0, 0
	}
	return it.samples[it.pos].TimestampMs, it.samples[it.pos].Value
}

func (it *mockIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

func (it *mockIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (it *mockIterator) AtT() int64 {
	if it.pos < 0 || it.pos >= len(it.samples) {
		return 0
	}
	return it.samples[it.pos].TimestampMs
}

func (it *mockIterator) Err() error {
	return nil
}
