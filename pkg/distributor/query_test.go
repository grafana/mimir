// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/query_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestMergeSamplesIntoFirstDuplicates(t *testing.T) {
	a := []mimirpb.Sample{
		{Value: 1.084537996, TimestampMs: 1583946732744},
		{Value: 1.086111723, TimestampMs: 1583946750366},
		{Value: 1.086111723, TimestampMs: 1583946768623},
		{Value: 1.087776094, TimestampMs: 1583946795182},
		{Value: 1.089301187, TimestampMs: 1583946810018},
		{Value: 1.089301187, TimestampMs: 1583946825064},
		{Value: 1.089301187, TimestampMs: 1583946835547},
		{Value: 1.090722985, TimestampMs: 1583946846629},
		{Value: 1.090722985, TimestampMs: 1583946857608},
		{Value: 1.092038719, TimestampMs: 1583946882302},
	}

	b := []mimirpb.Sample{
		{Value: 1.084537996, TimestampMs: 1583946732744},
		{Value: 1.086111723, TimestampMs: 1583946750366},
		{Value: 1.086111723, TimestampMs: 1583946768623},
		{Value: 1.087776094, TimestampMs: 1583946795182},
		{Value: 1.089301187, TimestampMs: 1583946810018},
		{Value: 1.089301187, TimestampMs: 1583946825064},
		{Value: 1.089301187, TimestampMs: 1583946835547},
		{Value: 1.090722985, TimestampMs: 1583946846629},
		{Value: 1.090722985, TimestampMs: 1583946857608},
		{Value: 1.092038719, TimestampMs: 1583946882302},
	}

	a = mergeSamples(a, b)

	// should be the same
	require.Equal(t, a, b)
}

func TestMergeSamplesIntoFirst(t *testing.T) {
	a := []mimirpb.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 45},
		{Value: 5, TimestampMs: 50},
	}

	b := []mimirpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 2, TimestampMs: 15},
		{Value: 3, TimestampMs: 25},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 35},
		{Value: 5, TimestampMs: 45},
		{Value: 6, TimestampMs: 55},
	}

	a = mergeSamples(a, b)

	require.Equal(t, []mimirpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 15},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 25},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 35},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 45},
		{Value: 5, TimestampMs: 50},
		{Value: 6, TimestampMs: 55},
	}, a)
}

func TestMergeSamplesIntoFirstNilA(t *testing.T) {
	b := []mimirpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 2, TimestampMs: 15},
		{Value: 3, TimestampMs: 25},
		{Value: 4, TimestampMs: 35},
		{Value: 5, TimestampMs: 45},
		{Value: 6, TimestampMs: 55},
	}

	a := mergeSamples(nil, b)

	require.Equal(t, b, a)
}

func TestMergeSamplesIntoFirstNilB(t *testing.T) {
	a := []mimirpb.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 50},
	}

	b := mergeSamples(a, nil)

	require.Equal(t, b, a)
}

func TestMergeExemplars(t *testing.T) {
	now := timestamp.FromTime(time.Now())
	exemplar1 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-1")), TimestampMs: now, Value: 1}
	exemplar2 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-2")), TimestampMs: now + 1, Value: 2}
	exemplar3 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-3")), TimestampMs: now + 4, Value: 3}
	exemplar4 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now + 8, Value: 7}
	exemplar5 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now, Value: 7}
	labels1 := []mimirpb.LabelAdapter{{Name: "label1", Value: "foo1"}}
	labels2 := []mimirpb.LabelAdapter{{Name: "label1", Value: "foo2"}}

	for i, c := range []struct {
		seriesA       []mimirpb.TimeSeries
		seriesB       []mimirpb.TimeSeries
		expected      []mimirpb.TimeSeries
		nonReversible bool
	}{
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
		},
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
		},
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
		},
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar3, exemplar4}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4}}},
		},
		{ // Ensure that when there are exemplars with duplicate timestamps, the first one wins.
			seriesA:       []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3}}},
			seriesB:       []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar5, exemplar3, exemplar4}}},
			expected:      []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4}}},
			nonReversible: true,
		},
		{ // Disjoint exemplars on two different series.
			seriesA: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}}},
			seriesB: []mimirpb.TimeSeries{{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3, exemplar4}}},
			expected: []mimirpb.TimeSeries{
				{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3, exemplar4}}},
		},
		{ // Second input adds to first on one series.
			seriesA: []mimirpb.TimeSeries{
				{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3}}},
			seriesB: []mimirpb.TimeSeries{{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar4}}},
			expected: []mimirpb.TimeSeries{
				{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3, exemplar4}}},
		},
	} {
		t.Run(fmt.Sprint("test", i), func(t *testing.T) {
			rA := &ingester_client.ExemplarQueryResponse{Timeseries: c.seriesA}
			rB := &ingester_client.ExemplarQueryResponse{Timeseries: c.seriesB}
			e := mergeExemplarQueryResponses([]interface{}{rA, rB})
			require.Equal(t, c.expected, e.Timeseries)
			if !c.nonReversible {
				// Check the other way round too
				e = mergeExemplarQueryResponses([]interface{}{rB, rA})
				require.Equal(t, c.expected, e.Timeseries)
			}
		})
	}
}

func makeExemplarQueryResponse(numSeries int) *ingester_client.ExemplarQueryResponse {
	now := time.Now()
	ts := make([]mimirpb.TimeSeries, numSeries)
	for i := 0; i < numSeries; i++ {
		lbls := labels.NewBuilder(labels.EmptyLabels())
		lbls.Set(model.MetricNameLabel, "foo")
		for i := 0; i < 10; i++ {
			lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d", i))
		}
		ts[i].Labels = mimirpb.FromLabelsToLabelAdapters(lbls.Labels(nil))
		ts[i].Exemplars = []mimirpb.Exemplar{{
			Labels:      []mimirpb.LabelAdapter{{Name: "traceid", Value: "trace1"}},
			Value:       float64(i),
			TimestampMs: now.Add(time.Hour).UnixNano() / int64(time.Millisecond),
		}}
	}

	return &ingester_client.ExemplarQueryResponse{Timeseries: ts}
}

func BenchmarkMergeExemplars(b *testing.B) {
	input := makeExemplarQueryResponse(1000)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Merge input with itself three times
		mergeExemplarQueryResponses([]interface{}{input, input, input})
	}
}
