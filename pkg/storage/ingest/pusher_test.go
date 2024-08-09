// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type pusherFunc func(context.Context, *mimirpb.WriteRequest) error

func (p pusherFunc) Close() []error {
	return nil
}

func (p pusherFunc) PushToStorage(ctx context.Context, request *mimirpb.WriteRequest) error {
	return p(ctx, request)
}

func TestPusherConsumer(t *testing.T) {
	const tenantID = "t1"
	writeReqs := []*mimirpb.WriteRequest{
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_4")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_5")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_6")}},
	}

	wrBytes := make([][]byte, len(writeReqs))
	for i, wr := range writeReqs {
		var err error
		wrBytes[i], err = wr.Marshal()
		require.NoError(t, err)
	}

	ctx := context.Background()

	type response struct {
		err error
	}

	okResponse := response{nil}

	testCases := map[string]struct {
		records     []record
		responses   []response
		expectedWRs []*mimirpb.WriteRequest
		expErr      string

		expectedLogLines []string
	}{
		"single record": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
			},
			expectedWRs: writeReqs[0:1],
		},
		"multiple records": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[1], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[2], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				okResponse,
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
		},
		"unparsable record": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
				{ctx: ctx, content: []byte{0}, tenantID: tenantID},
				{ctx: ctx, content: wrBytes[1], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				okResponse,
			},
			expectedWRs: writeReqs[0:2],
			expErr:      "",
			expectedLogLines: []string{
				"level=error msg=\"failed to parse write request; skipping\" err=\"parsing ingest consumer write request: proto: WriteRequest: illegal tag 0 (wire type 0)\"",
			},
		},
		"failed processing of record": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[1], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[2], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"failed processing of last record": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[1], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"failed processing & failed unmarshalling": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[1], tenantID: tenantID},
				{ctx: ctx, content: []byte{0}, tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"no records": {},
		"ingester client error": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[1], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[2], tenantID: tenantID},
			},
			responses: []response{
				{err: ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "ingester test error")},
				{err: ingesterError(mimirpb.BAD_DATA, codes.Unknown, "ingester test error")}, // status code doesn't matter
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
			expErr:      "", // since all fof those were client errors, we don't return an error
			expectedLogLines: []string{
				"method=pusherConsumer.pushToStorage level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" user=t1 insight=true err=\"rpc error: code = InvalidArgument desc = ingester test error\"",
				"method=pusherConsumer.pushToStorage level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" user=t1 insight=true err=\"rpc error: code = Unknown desc = ingester test error\"",
			},
		},
		"ingester server error": {
			records: []record{
				{ctx: ctx, content: wrBytes[0], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[1], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[2], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[3], tenantID: tenantID},
				{ctx: ctx, content: wrBytes[4], tenantID: tenantID},
			},
			responses: []response{
				{err: ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "ingester test error")},
				{err: ingesterError(mimirpb.TSDB_UNAVAILABLE, codes.Unavailable, "ingester internal error")},
			},
			expectedWRs: writeReqs[0:2], // the rest of the requests are not attempted
			expErr:      "ingester internal error",
			expectedLogLines: []string{
				"method=pusherConsumer.pushToStorage level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" user=t1 insight=true err=\"rpc error: code = InvalidArgument desc = ingester test error\"",
			},
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {

			receivedReqs := atomic.NewInt64(0)
			pusher := pusherFunc(func(ctx context.Context, request *mimirpb.WriteRequest) error {
				reqIdx := int(receivedReqs.Inc() - 1)
				require.GreaterOrEqualf(t, len(tc.expectedWRs), reqIdx+1, "received more requests (%d) than expected (%d)", reqIdx+1, len(tc.expectedWRs))

				expectedWR := tc.expectedWRs[reqIdx]
				for i, ts := range request.Timeseries {
					assert.Truef(t, ts.Equal(expectedWR.Timeseries[i].TimeSeries), "timeseries %d not equal; got %v, expected %v", i, ts, expectedWR.Timeseries[i].TimeSeries)
				}

				actualTenantID, err := tenant.TenantID(ctx)
				assert.NoError(t, err)
				assert.Equal(t, tenantID, actualTenantID)

				return tc.responses[reqIdx].err
			})

			logs := &concurrency.SyncBuffer{}
			c := newPusherConsumer(pusher, KafkaConfig{}, newPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewLogfmtLogger(logs))
			err := c.consume(context.Background(), tc.records)
			if tc.expErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.expErr)
			}

			var logLines []string
			if logsStr := logs.String(); logsStr != "" {
				logLines = strings.Split(strings.TrimSpace(logsStr), "\n")
				logLines = removeUnimportantLogFields(logLines)
			}
			assert.Equal(t, tc.expectedLogLines, logLines)
		})
	}
}

var unimportantLogFieldsPattern = regexp.MustCompile(`\scaller=\S+\.go:\d+\s`)

func removeUnimportantLogFields(lines []string) []string {
	// The 'caller' field is not important to these tests (we just care about the message and other information),
	// and can change as we refactor code, making these tests brittle. So we remove it before making assertions about the log lines.
	for i, line := range lines {
		lines[i] = unimportantLogFieldsPattern.ReplaceAllString(line, " ")
	}

	return lines
}

func TestPusherConsumer_clientErrorSampling(t *testing.T) {
	type testCase struct {
		sampler         *util_log.Sampler
		err             error
		expectedSampled bool
		expectedReason  string
	}

	plainError := fmt.Errorf("plain")

	for name, tc := range map[string]testCase{
		"nil sampler, plain error": {
			sampler:         nil,
			err:             plainError,
			expectedSampled: true,
			expectedReason:  "",
		},

		"nil sampler, sampled error": {
			sampler:         nil,
			err:             util_log.NewSampler(20).WrapError(plainError), // need to use new sampler to make sure it samples the error
			expectedSampled: true,
			expectedReason:  "sampled 1/20",
		},

		"fallback sampler, plain error": {
			sampler:         util_log.NewSampler(5),
			err:             plainError,
			expectedSampled: true,
			expectedReason:  "sampled 1/5",
		},

		"fallback sampler, sampled error": {
			sampler:         util_log.NewSampler(5),
			err:             util_log.NewSampler(20).WrapError(plainError), // need to use new sampler to make sure it samples the error
			expectedSampled: true,
			expectedReason:  "sampled 1/20",
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newPusherConsumer(nil, KafkaConfig{}, newPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
			c.fallbackClientErrSampler = tc.sampler

			sampled, reason := c.shouldLogClientError(context.Background(), tc.err)
			assert.Equal(t, tc.expectedSampled, sampled)
			assert.Equal(t, tc.expectedReason, reason)
		})
	}
}

func TestPusherConsumer_consume_ShouldLogErrorsHonoringOptionalLogging(t *testing.T) {
	// Create a request that will be used in this test. The content doesn't matter,
	// since we only test errors.
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}}
	reqBytes, err := req.Marshal()
	require.NoError(t, err)

	reqRecord := record{
		ctx:      context.Background(),
		tenantID: "user-1",
		content:  reqBytes,
	}

	setupTest := func(pusherErr error) (*pusherConsumer, *concurrency.SyncBuffer, *prometheus.Registry) {
		pusher := pusherFunc(func(context.Context, *mimirpb.WriteRequest) error {
			return pusherErr
		})

		reg := prometheus.NewPedanticRegistry()
		logs := &concurrency.SyncBuffer{}
		consumer := newPusherConsumer(pusher, KafkaConfig{}, newPusherConsumerMetrics(reg), log.NewLogfmtLogger(logs))

		return consumer, logs, reg
	}

	t.Run("should log a client error if does not implement optional logging interface", func(t *testing.T) {
		pusherErr := ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "mocked error")
		consumer, logs, reg := setupTest(pusherErr)

		// Should return no error on client errors.
		require.NoError(t, consumer.consume(context.Background(), []record{reqRecord}))

		assert.Contains(t, logs.String(), pusherErr.Error())
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_records_failed_total Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
			# TYPE cortex_ingest_storage_reader_records_failed_total counter
			cortex_ingest_storage_reader_records_failed_total{cause="client"} 1
			cortex_ingest_storage_reader_records_failed_total{cause="server"} 0
		`), "cortex_ingest_storage_reader_records_failed_total"))
	})

	t.Run("should log a client error if does implement optional logging interface and ShouldLog() returns true", func(t *testing.T) {
		pusherErrSampler := util_log.NewSampler(100)
		pusherErr := pusherErrSampler.WrapError(ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "mocked error"))

		// Pre-requisite: the mocked error should implement the optional logging interface.
		var optionalLoggingErr middleware.OptionalLogging
		require.ErrorAs(t, pusherErr, &optionalLoggingErr)

		consumer, logs, reg := setupTest(pusherErr)

		// Should return no error on client errors.
		require.NoError(t, consumer.consume(context.Background(), []record{reqRecord}))

		assert.Contains(t, logs.String(), fmt.Sprintf("%s (sampled 1/100)", pusherErr.Error()))
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_records_failed_total Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
			# TYPE cortex_ingest_storage_reader_records_failed_total counter
			cortex_ingest_storage_reader_records_failed_total{cause="client"} 1
			cortex_ingest_storage_reader_records_failed_total{cause="server"} 0
		`), "cortex_ingest_storage_reader_records_failed_total"))
	})

	t.Run("should not log a client error if does implement optional logging interface and ShouldLog() returns false", func(t *testing.T) {
		pusherErr := middleware.DoNotLogError{Err: ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "mocked error")}

		// Pre-requisite: the mocked error should implement the optional logging interface.
		var optionalLoggingErr middleware.OptionalLogging
		require.ErrorAs(t, pusherErr, &optionalLoggingErr)

		consumer, logs, reg := setupTest(pusherErr)

		// Should return no error on client errors.
		require.NoError(t, consumer.consume(context.Background(), []record{reqRecord}))

		assert.Empty(t, logs.String())
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_records_failed_total Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
			# TYPE cortex_ingest_storage_reader_records_failed_total counter
			cortex_ingest_storage_reader_records_failed_total{cause="client"} 1
			cortex_ingest_storage_reader_records_failed_total{cause="server"} 0
		`), "cortex_ingest_storage_reader_records_failed_total"))
	})

}

func TestPusherConsumer_consume_ShouldHonorContextCancellation(t *testing.T) {
	// Create a request that will be used in this test; the content doesn't matter,
	// since we only test errors.
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}}
	reqBytes, err := req.Marshal()
	require.NoError(t, err)

	reqRecord := record{
		ctx:      context.Background(), // The record's context isn't important for the test.
		tenantID: "user-1",
		content:  reqBytes,
	}

	// didPush signals that the testing record was pushed to the pusher.
	didPush := make(chan struct{}, 1)
	pusher := pusherFunc(func(ctx context.Context, _ *mimirpb.WriteRequest) error {
		close(didPush)
		<-ctx.Done()
		return context.Cause(ctx)
	})
	consumer := newPusherConsumer(pusher, KafkaConfig{}, newPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())

	wantCancelErr := cancellation.NewErrorf("stop")

	// For this test, cancelling the top-most context must cancel an in-flight call to push,
	// to prevent pusher from hanging forever.
	canceledCtx, cancel := context.WithCancelCause(context.Background())
	go func() {
		<-didPush
		cancel(wantCancelErr)
	}()

	err = consumer.consume(canceledCtx, []record{reqRecord})
	require.ErrorIs(t, err, wantCancelErr)
}

// ingesterError mimics how the ingester construct errors
func ingesterError(cause mimirpb.ErrorCause, statusCode codes.Code, message string) error {
	errorDetails := &mimirpb.ErrorDetails{Cause: cause}
	statWithDetails, err := status.New(statusCode, message).WithDetails(errorDetails)
	if err != nil {
		panic(err)
	}
	return statWithDetails.Err()
}

type mockPusher struct {
	mock.Mock
}

func (m *mockPusher) PushToStorage(ctx context.Context, request *mimirpb.WriteRequest) error {
	args := m.Called(ctx, request)
	return args.Error(0)
}

func TestShardingPusher(t *testing.T) {
	noopHistogram := promauto.With(prometheus.NewRegistry()).NewHistogram(prometheus.HistogramOpts{Name: "noop", NativeHistogramBucketFactor: 1.1})

	testCases := map[string]struct {
		shardCount   int
		batchSize    int
		requests     []*mimirpb.WriteRequest
		expectedErrs []error

		expectedUpstreamPushes []*mimirpb.WriteRequest
		upstreamPushErrs       []error
		expectedCloseErr       error
	}{
		"push to a single shard and fill exactly capacity": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}},
			},
			expectedErrs: []error{nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_2"),
				}},
			},
			upstreamPushErrs: []error{nil},
			expectedCloseErr: nil,
		},
		"push to multiple shards and fill exactly capacity": {
			shardCount: 2,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_4")}},
			},
			expectedErrs: []error{nil, nil, nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_3"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_2"),
					mockPreallocTimeseries("series_4"),
				}},
			},
			upstreamPushErrs: []error{nil, nil},
			expectedCloseErr: nil,
		},
		"push to single shard and underfill capacity": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
			},
			expectedErrs: []error{nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
			},
			upstreamPushErrs: []error{nil},
			expectedCloseErr: nil,
		},
		"push to single shard and overfill capacity": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
			},
			expectedErrs: []error{nil, nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_2"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3"),
				}},
			},
			upstreamPushErrs: []error{nil, nil},
			expectedCloseErr: nil,
		},
		"push to single shard and overfill only with the series of a singe request": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_2"),
					mockPreallocTimeseries("series_3"),
				}},
			},
			expectedErrs: []error{nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_2"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3"),
				}},
			},
			upstreamPushErrs: []error{nil, nil},
			expectedCloseErr: nil,
		},
		"push to multiple shards and overfill capacity on one shard and underfill on another": {
			shardCount: 2,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
			},

			expectedErrs: []error{nil, nil, nil, nil},
			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_3"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_2"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3"),
				}},
			},
			upstreamPushErrs: []error{nil, nil, nil},
			expectedCloseErr: nil,
		},
		"push to single shard and get an error with an underfilled shard (i.e. when calling Close() on the Pusher)": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}},
			},
			expectedErrs: []error{nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_2"),
				}},
			},
			upstreamPushErrs: []error{assert.AnError},
			expectedCloseErr: assert.AnError,
		},
		// TODO dimitarvdimitrov update test; since now the sharding pusher flushes in the background, we can't guarantee that the error will be returned after the first push or even after the second push;
		"push to single shard and get an error with an overfilled shard (i.e. during some of the pushes)": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
			},
			expectedErrs: []error{nil, nil, assert.AnError},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1"),
					mockPreallocTimeseries("series_2"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3"),
				}},
			},
			upstreamPushErrs: []error{assert.AnError, nil},
			expectedCloseErr: nil,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			require.Equal(t, len(tc.expectedUpstreamPushes), len(tc.upstreamPushErrs))
			require.Equal(t, len(tc.expectedErrs), len(tc.requests))

			pusher := &mockPusher{}
			shardingP := newShardingPusher(noopHistogram, tc.shardCount, tc.batchSize, pusher)

			for i, req := range tc.expectedUpstreamPushes {
				pusher.On("PushToStorage", mock.Anything, req).Return(tc.upstreamPushErrs[i])
			}
			for i, req := range tc.requests {
				err := shardingP.PushToStorage(context.Background(), req)
				assert.ErrorIs(t, err, tc.expectedErrs[i])
			}
			closeErr := shardingP.close()
			assert.ErrorIs(t, closeErr, tc.expectedCloseErr)
			pusher.AssertExpectations(t)
		})
	}
}
