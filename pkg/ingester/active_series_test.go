// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_ActiveSeries(t *testing.T) {
	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}

	seriesWithLabelsOfSize := func(size, index int) mimirpb.PreallocTimeseries {
		// 24 bytes of static strings and slice overhead, the remaining bytes are used to
		// pad the value of the "lbl" label.
		require.Greater(t, size, 24, "minimum message size is 24 bytes")
		tpl := fmt.Sprintf("%%0%dd", size-24)
		return mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(
					labels.FromStrings(model.MetricNameLabel, "test", "lbl", fmt.Sprintf(tpl, index))),
				Samples: samples,
			},
		}
	}

	totalMessageCount := 4
	totalSeriesSize := totalMessageCount * activeSeriesMaxSizeBytes

	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	currentSize := 0
	for i := 0; currentSize < totalSeriesSize; i++ {
		s := seriesWithLabelsOfSize(1024, i)
		writeReq.Timeseries = append(writeReq.Timeseries, s)
		currentSize += s.Size()
	}

	// Write the series.
	ingesterClient := prepareHealthyIngester(t, nil)
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ingesterClient.Push(ctx, writeReq)
	require.NoError(t, err)

	testCases := []struct {
		matchers             []*labels.Matcher
		expectedMessageCount int
		expectedSeriesCount  int
	}{
		{ // Match all series by name.
			matchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test")},
			expectedMessageCount: totalMessageCount,
			expectedSeriesCount:  len(writeReq.Timeseries),
		},
		{ // Match all series by blanket regex, but sharded.
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, sharding.ShardLabel, "1_of_4"),
				labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".*"),
			},
			expectedMessageCount: totalMessageCount / 4,
			expectedSeriesCount:  1012, // Note this number should change if the sample data changes.
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.matchers), func(t *testing.T) {
			// Get active series
			req, err := client.ToActiveSeriesRequest(tc.matchers)
			require.NoError(t, err)

			server := &mockActiveSeriesServer{ctx: ctx}
			err = ingesterClient.ActiveSeries(req, server)
			require.NoError(t, err)

			// Check that all series were returned.
			returnedSeriesCount := 0
			for _, res := range server.responses {
				returnedSeriesCount += len(res.Metric)
				// Check that all series have the expected number of labels.
				for _, m := range res.Metric {
					assert.Equal(t, 2, len(m.Labels))
				}
			}
			assert.Equal(t, tc.expectedSeriesCount, returnedSeriesCount)
			assert.Equal(t, tc.expectedMessageCount, len(server.responses))
		})
	}
}

func TestIngester_ActiveNativeHistogramSeries(t *testing.T) {
	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	histograms := []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1_000, util_test.GenerateTestHistogram(1))}

	seriesWithLabelsOfSize := func(size, index int, isHistogram bool) mimirpb.PreallocTimeseries {
		// 24 bytes of static strings and slice overhead, the remaining bytes are used to
		// pad the value of the "lbl" label.
		require.Greater(t, size, 24, "minimum message size is 24 bytes")
		tpl := fmt.Sprintf("%%0%dd", size-24)
		ts := &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "test", "lbl", fmt.Sprintf(tpl, index))),
		}
		if isHistogram {
			ts.Histograms = histograms
		} else {
			ts.Samples = samples
		}
		return mimirpb.PreallocTimeseries{TimeSeries: ts}
	}

	expectedMessageCount := 4
	totalSeriesSize := expectedMessageCount * activeSeriesMaxSizeBytes

	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	currentSize := 0
	for i := 0; currentSize < totalSeriesSize; i++ {
		isHistogram := i%2 != 0 // Half of the series will be float and the other half will be native histograms.
		s := seriesWithLabelsOfSize(1024, i, isHistogram)
		writeReq.Timeseries = append(writeReq.Timeseries, s)
		if isHistogram {
			currentSize += s.Size()
		}
	}

	// Write the series.
	ingesterClient := prepareHealthyIngester(t, func(limits *validation.Limits) { limits.NativeHistogramsIngestionEnabled = true })
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ingesterClient.Push(ctx, writeReq)
	require.NoError(t, err)

	// Get active series
	req, err := client.ToActiveSeriesRequest([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test"),
	})
	req.Type = client.NATIVE_HISTOGRAM_SERIES
	require.NoError(t, err)

	server := &mockActiveSeriesServer{ctx: ctx}
	err = ingesterClient.ActiveSeries(req, server)
	require.NoError(t, err)

	// Check that all series were returned.
	returnedSeriesCount := 0
	for _, res := range server.responses {
		returnedSeriesCount += len(res.Metric)
		// Check that all series have a corresponding bucket count.
		assert.Equal(t, len(res.Metric), len(res.BucketCount), "All series should have a bucket count.")
		for _, bc := range res.BucketCount {
			assert.Equal(t, uint64(8), bc)
		}
	}
	assert.Equal(t, len(writeReq.Timeseries)/2, returnedSeriesCount)

	// Check that we got the correct number of messages.
	assert.Equal(t, expectedMessageCount, len(server.responses))
}

func BenchmarkIngester_ActiveSeries(b *testing.B) {
	const (
		userID     = "test"
		numSeries  = 2e6
		metricName = "metric_name"
	)

	in := prepareHealthyIngester(b, nil)
	ctx := user.InjectOrgID(context.Background(), userID)

	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	for s := 0; s < numSeries; s++ {
		writeReq.Timeseries = append(writeReq.Timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
					model.MetricNameLabel, metricName,
					// Use mod prime to make label values repeat every n series
					"mod_10", strconv.Itoa(s%(2*5)),
					"mod_4199", strconv.Itoa(s%(13*17*19)))),
				Samples: samples,
			},
		})
	}
	_, err := in.Push(ctx, writeReq)
	require.NoError(b, err)

	for _, bc := range []struct {
		name     string
		matchers []*client.LabelMatcher
	}{
		{
			name:     "few series",
			matchers: []*client.LabelMatcher{{Name: "mod_4199", Value: "0", Type: client.EQUAL}},
		},
		{
			name:     "~10% of series",
			matchers: []*client.LabelMatcher{{Name: "mod_10", Value: "0", Type: client.EQUAL}},
		},
	} {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				req := &client.ActiveSeriesRequest{Matchers: bc.matchers}
				server := &mockActiveSeriesServer{ctx: ctx}
				require.NoError(b, in.ActiveSeries(req, server))
			}
		})
	}
}

type mockActiveSeriesServer struct {
	client.Ingester_ActiveSeriesServer
	responses []*client.ActiveSeriesResponse
	ctx       context.Context
}

func (s *mockActiveSeriesServer) Send(resp *client.ActiveSeriesResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func (s *mockActiveSeriesServer) Context() context.Context {
	return s.ctx
}

func TestMatchAllSeries(t *testing.T) {
	tests := []struct {
		name     string
		matchers []*labels.Matcher
		expected bool
	}{
		{
			name:     "empty matchers",
			matchers: []*labels.Matcher{},
			expected: false,
		},
		{
			name: "single matcher with regexp .*",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			},
			expected: true,
		},
		{
			name: "single matcher on __name__ with regexp .+",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
			},
			expected: true,
		},
		{
			name: "single matcher on __name__ with not-equal to empty string",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, model.MetricNameLabel, ""),
			},
			expected: true,
		},
		{
			name: "single matcher with different regexp pattern",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar"),
			},
			expected: false,
		},
		{
			name: "multiple matchers with regexp .*",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "baz"),
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchAllSeries(tc.matchers)
			assert.Equal(t, tc.expected, result)
		})
	}
}
