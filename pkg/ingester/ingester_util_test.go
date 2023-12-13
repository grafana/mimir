// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	dslog "github.com/grafana/dskit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"

	"github.com/grafana/mimir/pkg/ingester/client"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

type TenantLimitsMock struct {
	mock.Mock
	validation.TenantLimits
}

func (t *TenantLimitsMock) ByUserID(userID string) *validation.Limits {
	returnArgs := t.Called(userID)
	if returnArgs.Get(0) == nil {
		return nil
	}
	return returnArgs.Get(0).(*validation.Limits)
}

type loggerWithBuffer struct {
	logger log.Logger
	buf    *bytes.Buffer
}

func newLoggerWithCounter(t *testing.T, buf *bytes.Buffer) *loggerWithBuffer {
	var lvl dslog.Level
	require.NoError(t, lvl.Set("info"))
	logger := dslog.NewGoKitWithWriter(dslog.LogfmtFormat, buf)
	level.NewFilter(logger, lvl.Option)
	return &loggerWithBuffer{
		logger: logger,
		buf:    buf,
	}
}

func (l *loggerWithBuffer) Log(keyvals ...interface{}) error {
	return l.logger.Log(keyvals...)
}

func (l *loggerWithBuffer) count(msg string) int {
	msg = strings.ReplaceAll(msg, "\"", "\\\"")
	return bytes.Count(l.buf.Bytes(), []byte(msg))
}

type fakeUtilizationBasedLimiter struct {
	services.BasicService

	limitingReason string
}

func (l *fakeUtilizationBasedLimiter) LimitingReason() string {
	return l.limitingReason
}

type mockQueryStreamServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockQueryStreamServer) Send(*client.QueryStreamResponse) error {
	return nil
}

func (m *mockQueryStreamServer) Context() context.Context {
	return m.ctx
}

// Logger which does nothing and implements the DebugEnabled interface used by SpanLogger.
type noDebugNoopLogger struct{}

func (noDebugNoopLogger) Log(...interface{}) error { return nil }

func (noDebugNoopLogger) DebugEnabled() bool { return false }

type series struct {
	lbls      labels.Labels
	value     float64
	timestamp int64
}

func pushSeriesToIngester(ctx context.Context, t testing.TB, i *Ingester, series []series) error {
	for _, series := range series {
		req, _, _, _ := mockWriteRequest(t, series.lbls, series.value, series.timestamp)
		_, err := i.Push(ctx, req)
		if err != nil {
			return err
		}
	}
	return nil
}

type uploaderMock struct {
	mock.Mock
}

// Sync mocks BlocksUploader.Sync()
func (m *uploaderMock) Sync(ctx context.Context) (uploaded int, err error) {
	args := m.Called(ctx)
	return args.Int(0), args.Error(1)
}

func mockUserShipper(t *testing.T, i *Ingester) *uploaderMock {
	m := &uploaderMock{}
	userDB, err := i.getOrCreateTSDB(userID, false)
	require.NoError(t, err)
	require.NotNil(t, userDB)

	userDB.shipper = m
	return m
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

func setupFailingIngester(t *testing.T, cfg Config, failingCause error) *failingIngester {
	// Start the first ingester. This ensures the ring will be created.
	fI := newFailingIngester(t, cfg, nil, nil)
	kvStore := fI.kvStore
	ctx := context.Background()
	fI.startWaitAndCheck(ctx, t)
	fI.shutDownWaitAndCheck(ctx, t)

	// Start the same ingester with an error.
	return newFailingIngester(t, cfg, kvStore, failingCause)
}

type failingIngester struct {
	*Ingester
	kvStore      kv.Client
	failingCause error
}

func newFailingIngester(t *testing.T, cfg Config, kvStore kv.Client, failingCause error) *failingIngester {
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	fI := &failingIngester{Ingester: i, failingCause: failingCause}
	if kvStore != nil {
		fI.kvStore = kvStore
	}
	fI.BasicService = services.NewBasicService(fI.starting, fI.updateLoop, fI.stopping)
	return fI
}

func (i *failingIngester) startWaitAndCheck(ctx context.Context, t *testing.T) {
	err := services.StartAndAwaitRunning(ctx, i)
	var expectedHealthyIngesters int
	if i.failingCause == nil {
		require.NoError(t, err)
		expectedHealthyIngesters = 1
	} else {
		require.Error(t, err)
		require.ErrorIs(t, err, i.failingCause)
		expectedHealthyIngesters = 0
	}
	test.Poll(t, 100*time.Millisecond, expectedHealthyIngesters, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})
}

func (i *failingIngester) shutDownWaitAndCheck(ctx context.Context, t *testing.T) {
	// We properly shut down ingester, and ensure that it lifecycler is terminated.
	require.NoError(t, services.StopAndAwaitTerminated(ctx, i))
	require.Equal(t, services.Terminated, i.lifecycler.BasicService.State())
}

func (i *failingIngester) starting(parentCtx context.Context) error {
	if i.failingCause == nil {
		return i.Ingester.starting(parentCtx)
	}
	if errors.Is(i.failingCause, context.Canceled) {
		ctx, cancel := context.WithCancel(parentCtx)
		go func() {
			cancel()
		}()
		return i.Ingester.starting(ctx)
	}
	return i.Ingester.starting(&mockContext{ctx: parentCtx, err: i.failingCause})
}

func (i *failingIngester) getInstance(ctx context.Context) *ring.InstanceDesc {
	d, err := i.lifecycler.KVStore.Get(ctx, IngesterRingKey)
	if err != nil {
		return nil
	}
	instanceDesc, ok := ring.GetOrCreateRingDesc(d).Ingesters[i.lifecycler.ID]
	if !ok {
		return nil
	}
	return &instanceDesc
}

func (i *failingIngester) checkRingState(ctx context.Context, t *testing.T, expectedState ring.InstanceState) {
	instance := i.getInstance(ctx)
	require.NotNil(t, instance)
	require.Equal(t, expectedState, instance.GetState())
}

type mockContext struct {
	ctx context.Context
	err error
}

func (c *mockContext) Deadline() (time.Time, bool) {
	return c.ctx.Deadline()
}

func (c *mockContext) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (c *mockContext) Err() error {
	return c.err
}

func (c *mockContext) Value(key any) interface{} {
	return c.ctx.Value(key)
}

func prepareHealthyIngester(b testing.TB) *Ingester {
	cfg := defaultIngesterTestConfig(b)
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 0
	limits.MaxGlobalSeriesPerUser = 0

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, "", nil)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	b.Cleanup(func() {
		require.NoError(b, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's healthy.
	test.Poll(b, 1*time.Second, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})
	return i
}

func prepareIngesterWithBlocksStorage(t testing.TB, ingesterCfg Config, registerer prometheus.Registerer) (*Ingester, error) {
	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.NativeHistogramsIngestionEnabled = true
	return prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, limitsCfg, "", registerer)
}

func prepareIngesterWithBlocksStorageAndLimits(t testing.TB, ingesterCfg Config, limits validation.Limits, dataDir string, registerer prometheus.Registerer) (*Ingester, error) {
	overrides, err := validation.NewOverrides(limits, nil)
	if err != nil {
		return nil, err
	}
	return prepareIngesterWithBlockStorageAndOverrides(t, ingesterCfg, overrides, dataDir, "", registerer)
}

func prepareIngesterWithBlockStorageAndOverrides(t testing.TB, ingesterCfg Config, overrides *validation.Overrides, dataDir string, bucketDir string, registerer prometheus.Registerer) (*Ingester, error) {
	// Create a data dir if none has been provided.
	if dataDir == "" {
		dataDir = t.TempDir()
	}

	if bucketDir == "" {
		bucketDir = t.TempDir()
	}

	ingesterCfg.BlocksStorageConfig.TSDB.Dir = dataDir
	ingesterCfg.BlocksStorageConfig.Bucket.Backend = "filesystem"
	ingesterCfg.BlocksStorageConfig.Bucket.Filesystem.Directory = bucketDir

	// Disable TSDB head compaction jitter to have predictable tests.
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false

	ingester, err := New(ingesterCfg, overrides, nil, nil, registerer, noDebugNoopLogger{})
	if err != nil {
		return nil, err
	}

	return ingester, nil
}

func mockHistogramWriteRequest(lbls labels.Labels, timestampMs int64, histIdx int, genFloatHist bool) *mimirpb.WriteRequest {
	var histograms []mimirpb.Histogram
	if genFloatHist {
		h := util_test.GenerateTestFloatHistogram(histIdx)
		histograms = []mimirpb.Histogram{mimirpb.FromFloatHistogramToHistogramProto(timestampMs, h)}
	} else {
		h := util_test.GenerateTestHistogram(histIdx)
		histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(timestampMs, h)}
	}
	return mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries([][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(lbls)}, histograms, nil)
}

func mockWriteRequest(t testing.TB, lbls labels.Labels, value float64, timestampMs int64) (*mimirpb.WriteRequest, *client.QueryResponse, *client.QueryStreamResponse, *client.QueryStreamResponse) {
	samples := []mimirpb.Sample{
		{
			TimestampMs: timestampMs,
			Value:       value,
		},
	}

	req := mimirpb.ToWriteRequest([][]mimirpb.LabelAdapter{mimirpb.FromLabelsToLabelAdapters(lbls)}, samples, nil, nil, mimirpb.API)

	// Generate the expected response
	expectedQueryRes := &client.QueryResponse{
		Timeseries: []mimirpb.TimeSeries{
			{
				Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	expectedQueryStreamResSamples := &client.QueryStreamResponse{
		Timeseries: []mimirpb.TimeSeries{
			{
				Labels:  mimirpb.FromLabelsToLabelAdapters(lbls),
				Samples: samples,
			},
		},
	}

	chk := chunkenc.NewXORChunk()
	app, err := chk.Appender()
	require.NoError(t, err)
	app.Append(timestampMs, value)
	chk.Compact()

	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: mimirpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestampMs,
						EndTimestampMs:   timestampMs,
						Encoding:         int32(chunk.PrometheusXorChunk),
						Data:             chk.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryRes, expectedQueryStreamResSamples, expectedQueryStreamResChunks
}

func generateSamplesForLabel(baseLabels labels.Labels, series, samples int) *mimirpb.WriteRequest {
	lbls := make([][]mimirpb.LabelAdapter, 0, series*samples)
	ss := make([]mimirpb.Sample, 0, series*samples)

	for s := 0; s < series; s++ {
		l := labels.NewBuilder(baseLabels).Set("series", strconv.Itoa(s)).Labels()
		for i := 0; i < samples; i++ {
			ss = append(ss, mimirpb.Sample{
				Value:       float64(i),
				TimestampMs: int64(i),
			})
			lbls = append(lbls, mimirpb.FromLabelsToLabelAdapters(l))
		}
	}

	return mimirpb.ToWriteRequest(lbls, ss, nil, nil, mimirpb.API)
}

func pushWithUser(t *testing.T, ingester *Ingester, labelsToPush [][]mimirpb.LabelAdapter, userID string, req func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest) {
	for _, label := range labelsToPush {
		ctx := user.InjectOrgID(context.Background(), userID)
		_, err := ingester.Push(ctx, req(label, time.Now()))
		require.NoError(t, err)
	}
}

func writeRequestSingleSeries(lbls labels.Labels, samples []mimirpb.Sample) *mimirpb.WriteRequest {
	req := &mimirpb.WriteRequest{
		Source: mimirpb.API,
	}

	ts := mimirpb.TimeSeries{}
	ts.Labels = mimirpb.FromLabelsToLabelAdapters(lbls)
	ts.Samples = samples
	req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{TimeSeries: &ts})

	return req
}

func prepareRequestForTargetRequestDuration(ctx context.Context, t *testing.T, i *Ingester, targetRequestDuration time.Duration) *mimirpb.WriteRequest {
	samples := 100000
	ser := 1

	// Find right series&samples count to make sure that push takes given target duration.
	for {
		req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, fmt.Sprintf("test-%d-%d", ser, samples)), ser, samples)

		start := time.Now()
		_, err := i.Push(ctx, req)
		require.NoError(t, err)

		elapsed := time.Since(start)
		t.Log(ser, samples, elapsed)
		if elapsed > targetRequestDuration {
			break
		}

		samples = int(float64(samples) * float64(targetRequestDuration/elapsed) * 1.5) // Adjust number of series to hit our targetRequestDuration push duration.
		for samples >= int(time.Hour.Milliseconds()) {
			// We generate one sample per millisecond, if we have more than an hour of samples TSDB will fail with "out of bounds".
			// So we trade samples for series here.
			samples /= 10
			ser *= 10
		}
	}

	// Now repeat push with number of samples calibrated to our target request duration.
	req := generateSamplesForLabel(labels.FromStrings(labels.MetricName, fmt.Sprintf("real-%d-%d", ser, samples)), ser, samples)
	return req
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
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(nil, nil, nil, testData[userID], mimirpb.API))
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
		_, err := ing.Push(ctx, mimirpb.ToWriteRequest(matrixToLables(testData[userID]), matrixToSamples(testData[userID]), nil, nil, mimirpb.API))
		require.NoError(t, err)
	}

	return userIDs, testData
}

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
func matrixToLables(m model.Matrix) [][]mimirpb.LabelAdapter {
	var labels [][]mimirpb.LabelAdapter
	for _, ss := range m {
		for range ss.Values {
			labels = append(labels, mimirpb.FromMetricsToLabelAdapters(ss.Metric))
		}
	}
	return labels
}

func buildSeriesSet(t *testing.T, series *Series) []labels.Labels {
	var labelSets []labels.Labels
	for series.Next() {
		l := series.At()
		require.NoError(t, series.Err())
		labelSets = append(labelSets, l)
	}
	return labelSets
}

func generateSamples(sampleCount int) []mimirpb.Sample {
	samples := make([]mimirpb.Sample, 0, sampleCount)

	for i := 0; i < sampleCount; i++ {
		samples = append(samples, mimirpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	return samples
}
