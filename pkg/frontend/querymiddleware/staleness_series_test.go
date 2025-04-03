// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
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
