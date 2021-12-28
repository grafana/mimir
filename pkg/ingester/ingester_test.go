// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
)

func buildTestMatrix(numSeries int, samplesPerSeries int, offset int) model.Matrix {
	m := make(model.Matrix, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		ss := model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: model.LabelValue(fmt.Sprintf("testmetric_%d", i)),
				model.JobLabel:        model.LabelValue(fmt.Sprintf("testjob%d", i%2)),
			},
			Values: make([]model.SamplePair, 0, samplesPerSeries),
		}
		for j := 0; j < samplesPerSeries; j++ {
			ss.Values = append(ss.Values, model.SamplePair{
				Timestamp: model.Time(i + j + offset),
				Value:     model.SampleValue(i + j + offset),
			})
		}
		m = append(m, &ss)
	}
	sort.Sort(m)
	return m
}

func matrixToSamples(m model.Matrix) []mimirpb.Sample {
	var samples []mimirpb.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, mimirpb.Sample{
				TimestampMs: int64(sp.Timestamp),
				Value:       float64(sp.Value),
			})
		}
	}
	return samples
}

// Return one copy of the labels per sample
func matrixToLables(m model.Matrix) []labels.Labels {
	var labels []labels.Labels
	for _, ss := range m {
		for range ss.Values {
			labels = append(labels, mimirpb.FromLabelAdaptersToLabels(mimirpb.FromMetricsToLabelAdapters(ss.Metric)))
		}
	}
	return labels
}

func runTestQuery(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string) (model.Matrix, *client.QueryRequest, error) {
	return runTestQueryTimes(ctx, t, ing, ty, n, v, model.Earliest, model.Latest)
}

func runTestQueryTimes(ctx context.Context, t *testing.T, ing *Ingester, ty labels.MatchType, n, v string, start, end model.Time) (model.Matrix, *client.QueryRequest, error) {
	matcher, err := labels.NewMatcher(ty, n, v)
	if err != nil {
		return nil, nil, err
	}
	req, err := client.ToQueryRequest(start, end, []*labels.Matcher{matcher})
	if err != nil {
		return nil, nil, err
	}
	s := stream{ctx: ctx}
	err = ing.QueryStream(req, &s)
	require.NoError(t, err)

	res, err := chunkcompat.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
	require.NoError(t, err)
	sort.Sort(res)
	return res, req, nil
}

func pushTestMetadata(t *testing.T, ing *Ingester, numMetadata, metadataPerMetric int) ([]string, map[string][]*mimirpb.MetricMetadata) {
	userIDs := []string{"1", "2", "3"}

	// Create test metadata.
	// Map of userIDs, to map of metric => metadataSet
	testData := map[string][]*mimirpb.MetricMetadata{}
	for _, userID := range userIDs {
		metadata := make([]*mimirpb.MetricMetadata, 0, metadataPerMetric)
		for i := 0; i < numMetadata; i++ {
			metricName := fmt.Sprintf("testmetric_%d", i)
			for j := 0; j < metadataPerMetric; j++ {
				m := &mimirpb.MetricMetadata{MetricFamilyName: metricName, Help: fmt.Sprintf("a help for %d", j), Unit: "", Type: mimirpb.COUNTER}
				metadata = append(metadata, m)
			}
		}
		testData[userID] = metadata
	}

	// Append metadata.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, testData[userID], mimirpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func pushTestSamples(t testing.TB, ing *Ingester, numSeries, samplesPerSeries, offset int) ([]string, map[string]model.Matrix) {
	userIDs := []string{"1", "2", "3"}

	// Create test samples.
	testData := map[string]model.Matrix{}
	for i, userID := range userIDs {
		testData[userID] = buildTestMatrix(numSeries, samplesPerSeries, i+offset)
	}

	// Append samples.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(matrixToLables(testData[userID]), matrixToSamples(testData[userID]), nil, mimirpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

func TestIngesterPurgeMetadata(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	userIDs, _ := pushTestMetadata(t, ing, 10, 3)

	time.Sleep(40 * time.Millisecond)
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		ing.purgeUserMetricsMetadata()

		resp, err := ing.MetricsMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, len(resp.GetMetadata()))
	}
}

func TestIngesterMetadataMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.MetadataRetainPeriod = 20 * time.Millisecond

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), "", reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	_, _ = pushTestMetadata(t, ing, 10, 3)

	pushTestMetadata(t, ing, 10, 3)
	pushTestMetadata(t, ing, 10, 3) // We push the _exact_ same metrics again to ensure idempotency. Metadata is kept as a set so there shouldn't be a change of metrics.

	metricNames := []string{
		"cortex_ingester_memory_metadata_created_total",
		"cortex_ingester_memory_metadata_removed_total",
		"cortex_ingester_memory_metadata",
	}

	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 90
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
	`), metricNames...))

	time.Sleep(40 * time.Millisecond)
	ing.purgeUserMetricsMetadata()
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 30
		cortex_ingester_memory_metadata_created_total{user="2"} 30
		cortex_ingester_memory_metadata_created_total{user="3"} 30
		# HELP cortex_ingester_memory_metadata_removed_total The total number of metadata that were removed per user.
		# TYPE cortex_ingester_memory_metadata_removed_total counter
		cortex_ingester_memory_metadata_removed_total{user="1"} 30
		cortex_ingester_memory_metadata_removed_total{user="2"} 30
		cortex_ingester_memory_metadata_removed_total{user="3"} 30
	`), metricNames...))

}

func TestIngesterSendsOnlySeriesWithData(t *testing.T) {
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), defaultLimitsTestConfig(), "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	userIDs, _ := pushTestSamples(t, ing, 10, 1000, 0)

	// Read samples back via ingester queries.
	for _, userID := range userIDs {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, req, err := runTestQueryTimes(ctx, t, ing, labels.MatchRegexp, model.JobLabel, ".+", model.Latest.Add(-15*time.Second), model.Latest)
		require.NoError(t, err)

		s := stream{
			ctx: ctx,
		}
		err = ing.QueryStream(req, &s)
		require.NoError(t, err)

		// Nothing should be selected.
		require.Equal(t, 0, len(s.responses))
	}

	// Read samples back via chunk store.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ing))
}

type stream struct {
	grpc.ServerStream
	ctx       context.Context
	responses []*client.QueryStreamResponse
}

func (s *stream) Context() context.Context {
	return s.ctx
}

func (s *stream) Send(response *client.QueryStreamResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

// Test that blank labels are removed by the ingester
func TestIngester_Push_SeriesWithBlankLabel(t *testing.T) {
	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), defaultLimitsTestConfig(), "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	lbls := []labels.Labels{{
		{Name: model.MetricNameLabel, Value: "testmetric"},
		{Name: "foo", Value: ""},
		{Name: "bar", Value: ""},
	}}

	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = ing.Push(ctx, mimirpb.ToWriteRequest(
		lbls,
		[]mimirpb.Sample{{TimestampMs: 1, Value: 0}},
		nil,
		mimirpb.API,
	))
	require.NoError(t, err)

	res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, labels.MetricName, "testmetric")
	require.NoError(t, err)

	expected := model.Matrix{
		{
			Metric: model.Metric{labels.MetricName: "testmetric"},
			Values: []model.SamplePair{
				{Timestamp: 1, Value: 0},
			},
		},
	}

	assert.Equal(t, expected, res)
}

func TestIngesterUserLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerUser = 1
	limits.MaxLocalMetricsWithMetadataPerUser = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()

	newIngester := func() *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		// Wait until it's healthy
		test.Poll(t, time.Second, 1, func() interface{} {
			return ing.lifecycler.HealthyInstancesCount()
		})

		return ing
	}

	ing := newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	userID := "1"
	// Series
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := mimirpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}
	// Metadata
	metadata1 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.COUNTER}
	metadata2 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric2", Help: "a help for testmetric2", Type: mimirpb.COUNTER}

	// Append only one series and one metadata first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1}, []mimirpb.Sample{sample1}, []*mimirpb.MetricMetadata{metadata1}, mimirpb.API))
	require.NoError(t, err)

	testLimits := func() {
		// Append to two series, expect series-exceeded error.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []mimirpb.Sample{sample2, sample3}, nil, mimirpb.API))
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "returned error is not an httpgrpc response")
		assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
		assert.Equal(t, wrapWithUser(makeLimitError(perUserSeriesLimit, ing.limiter.FormatError(userID, errMaxSeriesPerUserLimitExceeded)), userID).Error(), string(httpResp.Body))

		// Append two metadata, expect no error since metadata is a best effort approach.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, []*mimirpb.MetricMetadata{metadata1, metadata2}, mimirpb.API))
		require.NoError(t, err)

		// Read samples back via ingester queries.
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
		require.NoError(t, err)

		expected := model.Matrix{
			{
				Metric: mimirpb.FromLabelAdaptersToMetric(mimirpb.FromLabelsToLabelAdapters(labels1)),
				Values: []model.SamplePair{
					{
						Timestamp: model.Time(sample1.TimestampMs),
						Value:     model.SampleValue(sample1.Value),
					},
					{
						Timestamp: model.Time(sample2.TimestampMs),
						Value:     model.SampleValue(sample2.Value),
					},
				},
			},
		}

		require.Equal(t, expected, res)

		// Verify metadata
		m, err := ing.MetricsMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []*mimirpb.MetricMetadata{metadata1}, m.Metadata)
	}

	testLimits()

	// Limits should hold after restart.
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	ing = newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	testLimits()

}

func TestIngesterMetricLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerMetric = 1
	limits.MaxLocalMetadataPerMetric = 1

	// create a data dir that survives an ingester restart
	dataDir := t.TempDir()

	newIngester := func() *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, dataDir, nil)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))

		// Wait until it's healthy
		test.Poll(t, time.Second, 1, func() interface{} {
			return ing.lifecycler.HealthyInstancesCount()
		})

		return ing
	}

	ing := newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	userID := "1"
	labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
	sample1 := mimirpb.Sample{
		TimestampMs: 0,
		Value:       1,
	}
	sample2 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       2,
	}
	labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
	sample3 := mimirpb.Sample{
		TimestampMs: 1,
		Value:       3,
	}

	// Metadata
	metadata1 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: mimirpb.COUNTER}
	metadata2 := &mimirpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric2", Type: mimirpb.COUNTER}

	// Append only one series and one metadata first, expect no error.
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1}, []mimirpb.Sample{sample1}, []*mimirpb.MetricMetadata{metadata1}, mimirpb.API))
	require.NoError(t, err)

	testLimits := func() {
		// Append two series, expect series-exceeded error.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest([]labels.Labels{labels1, labels3}, []mimirpb.Sample{sample2, sample3}, nil, mimirpb.API))
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "returned error is not an httpgrpc response")
		assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
		assert.Equal(t, wrapWithUser(makeMetricLimitError(perMetricSeriesLimit, labels3, ing.limiter.FormatError(userID, errMaxSeriesPerMetricLimitExceeded)), userID).Error(), string(httpResp.Body))

		// Append two metadata for the same metric. Drop the second one, and expect no error since metadata is a best effort approach.
		_, err = ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, []*mimirpb.MetricMetadata{metadata1, metadata2}, mimirpb.API))
		require.NoError(t, err)

		// Read samples back via ingester queries.
		res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
		require.NoError(t, err)

		// Verify Series
		expected := model.Matrix{
			{
				Metric: mimirpb.FromLabelAdaptersToMetric(mimirpb.FromLabelsToLabelAdapters(labels1)),
				Values: []model.SamplePair{
					{
						Timestamp: model.Time(sample1.TimestampMs),
						Value:     model.SampleValue(sample1.Value),
					},
					{
						Timestamp: model.Time(sample2.TimestampMs),
						Value:     model.SampleValue(sample2.Value),
					},
				},
			},
		}

		assert.Equal(t, expected, res)

		// Verify metadata
		m, err := ing.MetricsMetadata(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, []*mimirpb.MetricMetadata{metadata1}, m.Metadata)
	}

	testLimits()

	// Limits should hold after restart.
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	ing = newIngester()
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	testLimits()
}

// Construct a set of realistic-looking samples, all with slightly different label sets
func benchmarkData(nSeries int) (allLabels []labels.Labels, allSamples []mimirpb.Sample) {
	for j := 0; j < nSeries; j++ {
		labels := chunk.BenchmarkLabels.Copy()
		for i := range labels {
			if labels[i].Name == "cpu" {
				labels[i].Value = fmt.Sprintf("cpu%02d", j)
			}
		}
		allLabels = append(allLabels, labels)
		allSamples = append(allSamples, mimirpb.Sample{TimestampMs: 0, Value: float64(j)})
	}
	return
}

func BenchmarkIngester_QueryStream(b *testing.B) {
	cfg := defaultIngesterTestConfig(b)

	ing, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, defaultLimitsTestConfig(), "", nil)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), ing))
	defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(b, 100*time.Millisecond, 1, func() interface{} {
		return ing.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), "1")

	const (
		series  = 2000
		samples = 1000
	)

	allLabels, allSamples := benchmarkData(series)

	// Bump the timestamp and set a random value on each of our test samples each time round the loop
	for j := 0; j < samples; j++ {
		for i := range allSamples {
			allSamples[i].TimestampMs = int64(j + 1)
			allSamples[i].Value = rand.Float64()
		}
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(allLabels, allSamples, nil, mimirpb.API))
		require.NoError(b, err)
	}

	req := &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samples + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "container_cpu_usage_seconds_total",
		}},
	}

	mockStream := &mockQueryStreamServer{ctx: ctx}

	b.ResetTimer()

	for ix := 0; ix < b.N; ix++ {
		err := ing.QueryStream(req, mockStream)
		require.NoError(b, err)
	}
}

func TestIngesterActiveSeries(t *testing.T) {
	metricLabelsBoolTrue := labels.FromStrings(labels.MetricName, "test", "bool", "true")
	metricLabelsBoolFalse := labels.FromStrings(labels.MetricName, "test", "bool", "false")

	req := func(lbls labels.Labels, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.ToWriteRequest(
			[]labels.Labels{lbls},
			[]mimirpb.Sample{{Value: 1, TimestampMs: t.UnixMilli()}},
			nil,
			mimirpb.API,
		)
	}

	metricNames := []string{
		"cortex_ingester_active_series",
		"cortex_ingester_active_series_custom_tracker",
	}
	userID := "test"

	tests := map[string]struct {
		test                func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer)
		reqs                []*mimirpb.WriteRequest
		expectedMetrics     string
		disableActiveSeries bool
	}{
		"successful push, should count active series": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				now := time.Now()

				for _, req := range []*mimirpb.WriteRequest{
					req(metricLabelsBoolTrue, now.Add(-2*time.Minute)),
					req(metricLabelsBoolTrue, now.Add(-1*time.Minute)),
				} {
					ctx := user.InjectOrgID(context.Background(), userID)
					_, err := ingester.Push(ctx, req)
					require.NoError(t, err)
				}

				// Update active series for metrics check.
				ingester.updateActiveSeries(now)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test"} 1
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true",user="test"} 1
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"should track custom matchers, removing when zero": {
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				firstPushTime := time.Now()

				// We're pushing samples at firstPushTime - 1m, but they're accounted as pushed at firstPushTime
				for _, req := range []*mimirpb.WriteRequest{
					req(metricLabelsBoolTrue, firstPushTime.Add(-time.Minute)),
					req(metricLabelsBoolFalse, firstPushTime.Add(-time.Minute)),
				} {
					ctx := user.InjectOrgID(context.Background(), userID)
					_, err := ingester.Push(ctx, req)
					require.NoError(t, err)
				}

				// Update active series for metrics check.
				ingester.updateActiveSeries(firstPushTime)

				expectedMetrics := `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test"} 2
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_false",user="test"} 1
					cortex_ingester_active_series_custom_tracker{name="bool_is_true",user="test"} 1
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))

				// Sleep for one millisecond, this will make the append time of first push smaller than
				// secondPushTime. This is something required to make the test deterministic
				// (otherwise it _could_ be the same nanosecond theoretically, although unlikely in practice)
				time.Sleep(time.Millisecond)
				secondPushTime := time.Now()
				// Sleep another millisecond to make sure that secondPushTime is strictly less than the append time of the second push.
				time.Sleep(time.Millisecond)

				for _, req := range []*mimirpb.WriteRequest{
					req(metricLabelsBoolTrue, secondPushTime),
					req(metricLabelsBoolTrue, secondPushTime),
					// metricLabelsBoolFalse became inactive so we're not pushing it anymore
				} {
					ctx := user.InjectOrgID(context.Background(), userID)
					_, err := ingester.Push(ctx, req)
					require.NoError(t, err)
				}

				// Update active series for metrics check in the future.
				// We update them in the exact moment in time where append time of the first push is already considered idle,
				// while the second append happens after the purge timestamp.
				ingester.updateActiveSeries(secondPushTime.Add(ingester.cfg.ActiveSeriesMetricsIdleTimeout))

				expectedMetrics = `
					# HELP cortex_ingester_active_series Number of currently active series per user.
					# TYPE cortex_ingester_active_series gauge
					cortex_ingester_active_series{user="test"} 1
					# HELP cortex_ingester_active_series_custom_tracker Number of currently active series matching a pre-configured label matchers per user.
					# TYPE cortex_ingester_active_series_custom_tracker gauge
					cortex_ingester_active_series_custom_tracker{name="bool_is_true",user="test"} 1
				`

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			test: func(t *testing.T, ingester *Ingester, gatherer prometheus.Gatherer) {
				now := time.Now()

				for _, req := range []*mimirpb.WriteRequest{
					req(metricLabelsBoolTrue, now.Add(-2*time.Minute)),
					req(metricLabelsBoolTrue, now.Add(-1*time.Minute)),
				} {
					ctx := user.InjectOrgID(context.Background(), userID)
					_, err := ingester.Push(ctx, req)
					require.NoError(t, err)
				}

				// Update active series for metrics check.
				ingester.updateActiveSeries(now)

				expectedMetrics := ``

				// Check tracked Prometheus metrics
				require.NoError(t, testutil.GatherAndCompare(gatherer, strings.NewReader(expectedMetrics), metricNames...))
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.ActiveSeriesMetricsEnabled = !testData.disableActiveSeries
			cfg.ActiveSeriesCustomTrackers = map[string]string{
				"bool_is_true":  `{bool="true"}`,
				"bool_is_false": `{bool="false"}`,
			}

			ing, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, defaultLimitsTestConfig(), "", registry)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return ing.lifecycler.HealthyInstancesCount()
			})


			testData.test(t, ing, registry)
		})
	}
}

func TestGetIgnoreSeriesLimitForMetricNamesMap(t *testing.T) {
	cfg := Config{}

	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = ", ,,,"
	require.Nil(t, cfg.getIgnoreSeriesLimitForMetricNamesMap())

	cfg.IgnoreSeriesLimitForMetricNames = "foo, bar, ,"
	require.Equal(t, map[string]struct{}{"foo": {}, "bar": {}}, cfg.getIgnoreSeriesLimitForMetricNamesMap())
}
