// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
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
			err := c.Consume(context.Background(), tc.records)
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

var unimportantLogFieldsPattern = regexp.MustCompile(`(\s?)caller=\S+\.go:\d+\s`)

func removeUnimportantLogFields(lines []string) []string {
	// The 'caller' field is not important to these tests (we just care about the message and other information),
	// and can change as we refactor code, making these tests brittle. So we remove it before making assertions about the log lines.
	for i, line := range lines {
		lines[i] = unimportantLogFieldsPattern.ReplaceAllString(line, "$1")
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
		require.NoError(t, consumer.Consume(context.Background(), []record{reqRecord}))

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
		require.NoError(t, consumer.Consume(context.Background(), []record{reqRecord}))

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
		require.NoError(t, consumer.Consume(context.Background(), []record{reqRecord}))

		assert.Empty(t, logs.String())
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_records_failed_total Number of records (write requests) which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
			# TYPE cortex_ingest_storage_reader_records_failed_total counter
			cortex_ingest_storage_reader_records_failed_total{cause="client"} 1
			cortex_ingest_storage_reader_records_failed_total{cause="server"} 0
		`), "cortex_ingest_storage_reader_records_failed_total"))
	})

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

func TestParallelStorageShards_ShardWriteRequest(t *testing.T) {
	testCases := map[string]struct {
		shardCount        int
		batchSize         int
		requests          []*mimirpb.WriteRequest
		expectedErrs      []error
		expectedErrsCount int

		expectedUpstreamPushes []*mimirpb.WriteRequest
		upstreamPushErrs       []error
		expectedCloseErr       error
	}{
		"push to a single shard and fill exactly capacity": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_1")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2_1")}},
			},
			expectedErrs: []error{nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1_1"),
					mockPreallocTimeseries("series_2_1"),
				}},
			},
			upstreamPushErrs: []error{nil},
			expectedCloseErr: nil,
		},
		"push to multiple shards and fill exact capacity": {
			shardCount: 2,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_2")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2_2")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_2")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_4_2")}},
			},
			expectedErrs: []error{nil, nil, nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1_2"),
					mockPreallocTimeseries("series_3_2"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_2_2"),
					mockPreallocTimeseries("series_4_2"),
				}},
			},
			upstreamPushErrs: []error{nil, nil},
			expectedCloseErr: nil,
		},
		"push to single shard and underfill capacity": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_3")}},
			},
			expectedErrs: []error{nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_3")}},
			},
			upstreamPushErrs: []error{nil},
			expectedCloseErr: nil,
		},
		"push to single shard and overfill capacity": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_4")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2_4")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_4")}},
			},
			expectedErrs: []error{nil, nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1_4"),
					mockPreallocTimeseries("series_2_4"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3_4"),
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
					mockPreallocTimeseries("series_1_5"),
					mockPreallocTimeseries("series_2_5"),
					mockPreallocTimeseries("series_3_5"),
				}},
			},
			expectedErrs: []error{nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1_5"),
					mockPreallocTimeseries("series_2_5"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3_5"),
				}},
			},
			upstreamPushErrs: []error{nil, nil},
			expectedCloseErr: nil,
		},
		"push to multiple shards and overfill capacity on one shard and underfill on another": {
			shardCount: 2,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_6")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2_6")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_6")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_6")}},
			},

			expectedErrs: []error{nil, nil, nil, nil},
			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1_6"),
					mockPreallocTimeseries("series_3_6"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_2_6"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3_6"),
				}},
			},
			upstreamPushErrs: []error{nil, nil, nil},
			expectedCloseErr: nil,
		},
		"push to single shard and get an error with an underfilled shard (i.e. when calling Close() on the Pusher)": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_7")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2_7")}},
			},
			expectedErrs: []error{nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1_7"),
					mockPreallocTimeseries("series_2_7"),
				}},
			},
			upstreamPushErrs: []error{assert.AnError},
			expectedCloseErr: assert.AnError,
		},
		"push to single shard and get an error with an overfilled shard (i.e. during some of the pushes)": {
			shardCount: 1,
			batchSize:  2,
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1_8")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_2_8")}},

				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_8")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_8")}},

				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_8")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_8")}},

				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_8")}},
				{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3_8")}},
			},
			expectedErrsCount: 1, // at least one of those should fail because the first flush failed

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_1_8"),
					mockPreallocTimeseries("series_2_8"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3_8"),
					mockPreallocTimeseries("series_3_8"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3_8"),
					mockPreallocTimeseries("series_3_8"),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseries("series_3_8"),
					mockPreallocTimeseries("series_3_8"),
				}},
			},
			upstreamPushErrs: []error{assert.AnError, nil, nil, nil},
			expectedCloseErr: nil,
		},

		"push with metadata and exemplars": {
			shardCount: 1,
			batchSize:  3,
			requests: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseriesWithExemplar("series_1_10")},
					Metadata: []*mimirpb.MetricMetadata{
						{
							MetricFamilyName: "series_1_10",
							Type:             mimirpb.COUNTER,
							Help:             "A test counter",
							Unit:             "bytes",
						},
					},
				},
				{
					Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseriesWithExemplar("series_2_10")},
					Metadata: []*mimirpb.MetricMetadata{
						{
							MetricFamilyName: "series_2_10",
							Type:             mimirpb.GAUGE,
							Help:             "A test gauge",
							Unit:             "seconds",
						},
					},
				},
			},
			expectedErrs: []error{nil, nil},

			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{
					Timeseries: []mimirpb.PreallocTimeseries{
						mockPreallocTimeseriesWithExemplar("series_1_10"),
						mockPreallocTimeseriesWithExemplar("series_2_10"),
					},
					Metadata: []*mimirpb.MetricMetadata{
						{
							MetricFamilyName: "series_1_10",
							Type:             mimirpb.COUNTER,
							Help:             "A test counter",
							Unit:             "bytes",
						},
					},
				},
				{
					Metadata: []*mimirpb.MetricMetadata{
						{
							MetricFamilyName: "series_2_10",
							Type:             mimirpb.GAUGE,
							Help:             "A test gauge",
							Unit:             "seconds",
						},
					},
					Timeseries: mimirpb.PreallocTimeseriesSliceFromPool(),
				},
			},
			upstreamPushErrs: []error{nil, nil},
			expectedCloseErr: nil,
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			require.Equal(t, len(tc.expectedUpstreamPushes), len(tc.upstreamPushErrs))
			if len(tc.expectedErrs) != 0 && tc.expectedErrsCount > 0 {
				require.Fail(t, "expectedErrs and expectedErrsCount are mutually exclusive")
			}
			if len(tc.expectedErrs) != 0 {
				require.Equal(t, len(tc.expectedErrs), len(tc.requests))
			}

			pusher := &mockPusher{}
			// run with a buffer of one, so some of the tests can fill the buffer and test the error handling
			const buffer = 1
			metrics := newPusherConsumerMetrics(prometheus.NewPedanticRegistry())
			shardingP := newParallelStorageShards(metrics, tc.shardCount, tc.batchSize, buffer, pusher, labels.StableHash)

			for i, req := range tc.expectedUpstreamPushes {
				pusher.On("PushToStorage", mock.Anything, req).Return(tc.upstreamPushErrs[i])
			}
			var actualPushErrs []error
			for _, req := range tc.requests {
				err := shardingP.ShardWriteRequest(context.Background(), req)
				actualPushErrs = append(actualPushErrs, err)
			}

			if len(tc.expectedErrs) > 0 {
				require.Equal(t, tc.expectedErrs, actualPushErrs)
			} else {
				receivedErrs := 0
				for _, err := range actualPushErrs {
					if err != nil {
						receivedErrs++
					}
				}
				require.Equalf(t, tc.expectedErrsCount, receivedErrs, "received %d errors instead of %d: %v", receivedErrs, tc.expectedErrsCount, actualPushErrs)
			}

			closeErr := shardingP.Stop()
			require.ErrorIs(t, closeErr, tc.expectedCloseErr)
			pusher.AssertNumberOfCalls(t, "PushToStorage", len(tc.expectedUpstreamPushes))
			pusher.AssertExpectations(t)
		})
	}
}

func TestParallelStoragePusher(t *testing.T) {
	type tenantWriteRequest struct {
		tenantID string
		*mimirpb.WriteRequest
	}

	testCases := map[string]struct {
		requests               []tenantWriteRequest
		expectedUpstreamPushes map[string]map[mimirpb.WriteRequest_SourceEnum]int
	}{
		"separate tenants and sources": {
			requests: []tenantWriteRequest{
				{
					tenantID: "tenant1",
					WriteRequest: &mimirpb.WriteRequest{
						Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseriesWithExemplar("series_1")},
						Source:     mimirpb.API,
					},
				},
				{
					tenantID: "tenant1",
					WriteRequest: &mimirpb.WriteRequest{
						Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseriesWithExemplar("series_2")},
						Source:     mimirpb.RULE,
					},
				},
				{
					tenantID: "tenant2",
					WriteRequest: &mimirpb.WriteRequest{
						Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseriesWithExemplar("series_3")},
						Source:     mimirpb.API,
					},
				},
				{
					tenantID: "tenant2",
					WriteRequest: &mimirpb.WriteRequest{
						Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseriesWithExemplar("series_4")},
						Source:     mimirpb.RULE,
					},
				},
			},
			expectedUpstreamPushes: map[string]map[mimirpb.WriteRequest_SourceEnum]int{
				"tenant1": {
					mimirpb.API:  1,
					mimirpb.RULE: 1,
				},
				"tenant2": {
					mimirpb.API:  1,
					mimirpb.RULE: 1,
				},
			},
		},
		"metadata-only requests": {
			requests: []tenantWriteRequest{
				{
					tenantID: "tenant1",
					WriteRequest: &mimirpb.WriteRequest{
						Metadata: []*mimirpb.MetricMetadata{
							{
								MetricFamilyName: "metric1",
								Type:             mimirpb.COUNTER,
								Help:             "A test counter",
								Unit:             "bytes",
							},
						},
						Source: mimirpb.API,
					},
				},
				{
					tenantID: "tenant1",
					WriteRequest: &mimirpb.WriteRequest{
						Metadata: []*mimirpb.MetricMetadata{
							{
								MetricFamilyName: "metric2",
								Type:             mimirpb.GAUGE,
								Help:             "A test gauge",
								Unit:             "seconds",
							},
						},
						Source: mimirpb.RULE,
					},
				},
				{
					tenantID: "tenant2",
					WriteRequest: &mimirpb.WriteRequest{
						Metadata: []*mimirpb.MetricMetadata{
							{
								MetricFamilyName: "metric3",
								Type:             mimirpb.HISTOGRAM,
								Help:             "A test histogram",
								Unit:             "bytes",
							},
						},
						Source: mimirpb.API,
					},
				},
			},
			expectedUpstreamPushes: map[string]map[mimirpb.WriteRequest_SourceEnum]int{
				"tenant1": {
					mimirpb.API:  1,
					mimirpb.RULE: 1,
				},
				"tenant2": {
					mimirpb.API: 1,
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pusher := &mockPusher{}
			logger := log.NewNopLogger()

			receivedPushes := make(map[string]map[mimirpb.WriteRequest_SourceEnum]int)
			var receivedPushesMu sync.Mutex

			pusher.On("PushToStorage", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				tenantID, err := tenant.TenantID(args.Get(0).(context.Context))
				require.NoError(t, err)
				req := args.Get(1).(*mimirpb.WriteRequest)

				receivedPushesMu.Lock()
				defer receivedPushesMu.Unlock()

				if receivedPushes[tenantID] == nil {
					receivedPushes[tenantID] = make(map[mimirpb.WriteRequest_SourceEnum]int)
				}
				receivedPushes[tenantID][req.Source]++
			}).Return(nil)

			metrics := newPusherConsumerMetrics(prometheus.NewPedanticRegistry())
			psp := newParallelStoragePusher(metrics, pusher, 1, 1, logger)

			// Process requests
			for _, req := range tc.requests {
				ctx := user.InjectOrgID(context.Background(), req.tenantID)
				err := psp.PushToStorage(ctx, req.WriteRequest)
				require.NoError(t, err)
			}

			// Close the pusher to flush any remaining data
			errs := psp.Close()
			require.Empty(t, errs)

			// Verify the received pushes
			assert.Equal(t, tc.expectedUpstreamPushes, receivedPushes, "Mismatch in upstream pushes")

			pusher.AssertExpectations(t)
		})
	}
}

func TestBatchingQueue_NoDeadlock(t *testing.T) {
	capacity := 2
	batchSize := 3
	queue := newBatchingQueue(capacity, batchSize)

	ctx := context.Background()
	series := mockPreallocTimeseries("series_1")

	// Start a goroutine to process the queue
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer queue.Done()
		for range queue.Channel() {
			// Simulate processing time
			time.Sleep(50 * time.Millisecond)
			queue.ErrorChannel() <- fmt.Errorf("mock error")
		}
	}()

	// Add items to the queue
	for i := 0; i < batchSize*(capacity+1); i++ {
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series))
	}

	// Close the queue to signal no more items will be added
	err := queue.Close()
	require.ErrorContains(t, err, "mock error")

	wg.Wait()

	// Ensure the queue is empty and no deadlock occurred
	require.Len(t, queue.ch, 0)
	require.Len(t, queue.errCh, 0)
	require.Len(t, queue.currentBatch.Timeseries, 0)
}

func TestBatchingQueue(t *testing.T) {
	capacity := 5
	batchSize := 3

	series1 := mockPreallocTimeseries("series_1")
	series2 := mockPreallocTimeseries("series_2")

	series := []mimirpb.PreallocTimeseries{series1, series2}

	t.Run("batch not flushed because batch size is 3 and we have 2 items in the queue", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, series)

		select {
		case <-queue.Channel():
			t.Fatal("expected batch to not be flushed")
		case <-time.After(100 * time.Millisecond):
		}
	})

	t.Run("batch flushed because batch size is 3 and we have 3 items in the queue", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, series)

		series3 := mockPreallocTimeseries("series_3")
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, series3))

		select {
		case batch := <-queue.Channel():
			require.Len(t, batch.WriteRequest.Timeseries, 3)
			require.Equal(t, series1, batch.WriteRequest.Timeseries[0])
			require.Equal(t, series2, batch.WriteRequest.Timeseries[1])
			require.Equal(t, series3, batch.WriteRequest.Timeseries[2])
		case <-time.After(time.Second):
			t.Fatal("expected batch to be flushed")
		}

		// after the batch is flushed, the queue should be empty.
		require.Len(t, queue.currentBatch.Timeseries, 0)
	})

	t.Run("if you close the queue with items in the queue, the queue should flush the items", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, series)

		// Channel is empty.
		select {
		case <-queue.Channel():
			t.Fatal("expected batch to not be flushed")
		case <-time.After(100 * time.Millisecond):
		}

		// Read in a separate goroutine as when we close the queue, the channel will be closed.
		var batch flushableWriteRequest
		go func() {
			defer queue.Done()
			for b := range queue.Channel() {
				batch = b
				queue.ErrorChannel() <- nil
			}
		}()

		// Close the queue, and the items should be flushed.
		require.NoError(t, queue.Close())

		require.Len(t, batch.WriteRequest.Timeseries, 2)
		require.Equal(t, series1, batch.WriteRequest.Timeseries[0])
		require.Equal(t, series2, batch.WriteRequest.Timeseries[1])
	})

	t.Run("test queue capacity", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, nil)

		// Queue channel is empty because there are only 2 items in the current currentBatch.
		require.Len(t, queue.ch, 0)
		require.Len(t, queue.currentBatch.Timeseries, 0)

		// Add items to the queue until it's full.
		for i := 0; i < capacity*batchSize; i++ {
			s := mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
			require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s))
		}

		// We should have 5 items in the queue channel and 0 items in the currentBatch.
		require.Len(t, queue.ch, 5)
		require.Len(t, queue.currentBatch.Timeseries, 0)

		// Read one item to free up a queue space.
		batch := <-queue.Channel()
		require.Len(t, batch.WriteRequest.Timeseries, 3)

		// Queue should have 4 items now and the currentBatch remains the same.
		require.Len(t, queue.ch, 4)
		require.Len(t, queue.currentBatch.Timeseries, 0)

		// Add three more items to fill up the queue again, this shouldn't block.
		s := mockPreallocTimeseries("series_100")
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s))
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s))
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s))

		require.Len(t, queue.ch, 5)
		require.Len(t, queue.currentBatch.Timeseries, 0)
	})

	t.Run("metadata and exemplars are preserved in batches", func(t *testing.T) {
		const (
			capacity  = 2
			batchSize = 2 // 1 for metadata, 1 for a series
			timestamp = 1234567890
		)
		queue := setupQueue(t, capacity, batchSize, nil)

		// Create a WriteRequest with metadata and a series with exemplars
		timeSeries := mockPreallocTimeseries("series_1")
		timeSeries.Exemplars = append(timeSeries.Exemplars, mimirpb.Exemplar{
			Value:       42.0,
			TimestampMs: timestamp,
			Labels:      []mimirpb.LabelAdapter{{Name: "trace_id", Value: "abc123"}},
		},
		)

		md := &mimirpb.MetricMetadata{
			Type:             mimirpb.COUNTER,
			MetricFamilyName: "test_counter",
			Help:             "A test counter",
			Unit:             "bytes",
		}

		// Add timeseries with exemplars to the queue
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, timeSeries))

		// Add metadata to the queue
		require.NoError(t, queue.AddMetadataToBatch(context.Background(), mimirpb.API, md))

		// Read the batch from the queue
		select {
		case batch, notExhausted := <-queue.Channel():
			require.True(t, notExhausted)
			require.Len(t, batch.WriteRequest.Timeseries, 1)
			require.Len(t, batch.WriteRequest.Metadata, 1)

			// Check that the first series in the batch has the correct exemplars
			require.Len(t, batch.WriteRequest.Timeseries[0].TimeSeries.Exemplars, 1)
			require.Equal(t, 42.0, batch.WriteRequest.Timeseries[0].TimeSeries.Exemplars[0].Value)
			require.Equal(t, int64(timestamp), batch.WriteRequest.Timeseries[0].TimeSeries.Exemplars[0].TimestampMs)
			require.Equal(t, "trace_id", batch.WriteRequest.Timeseries[0].TimeSeries.Exemplars[0].Labels[0].Name)
			require.Equal(t, "abc123", batch.WriteRequest.Timeseries[0].TimeSeries.Exemplars[0].Labels[0].Value)

			// Check that the metadata in the batch is correct
			require.Equal(t, mimirpb.COUNTER, batch.WriteRequest.Metadata[0].Type)
			require.Equal(t, "test_counter", batch.WriteRequest.Metadata[0].MetricFamilyName)
			require.Equal(t, "A test counter", batch.WriteRequest.Metadata[0].Help)
			require.Equal(t, "bytes", batch.WriteRequest.Metadata[0].Unit)

		case <-time.After(time.Second):
			t.Fatal("expected batch to be flushed")
		}
	})
}

func TestBatchingQueue_ErrorHandling(t *testing.T) {
	capacity := 2
	batchSize := 2
	series1 := mockPreallocTimeseries("series_1")
	series2 := mockPreallocTimeseries("series_2")

	t.Run("AddToBatch returns all errors and it pushes the batch when the batch is filled ", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, nil)
		ctx := context.Background()

		// Push 1 series so that the next push will complete the batch.
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series2))

		// Push an error to fill the error channel.
		queue.ErrorChannel() <- fmt.Errorf("mock error 1")
		queue.ErrorChannel() <- fmt.Errorf("mock error 2")

		// AddToBatch should return an error now.
		err := queue.AddToBatch(ctx, mimirpb.API, series2)
		assert.Equal(t, "2 errors: mock error 1; mock error 2", err.Error())
		// Also the batch was pushed.
		select {
		case batch := <-queue.Channel():
			require.Equal(t, series2, batch.WriteRequest.Timeseries[0])
			require.Equal(t, series2, batch.WriteRequest.Timeseries[1])
		default:
			t.Fatal("expected batch to be flushed")
		}

		// AddToBatch should work again.
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series2))
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series2))
	})

	t.Run("Any errors pushed after last AddToBatch call are received on Close", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, nil)
		ctx := context.Background()

		// Add a batch to a batch but make sure nothing is pushed.,
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series1))

		select {
		case <-queue.Channel():
			t.Fatal("expected batch to not be flushed")
		default:
		}

		// Push multiple errors
		queue.ErrorChannel() <- fmt.Errorf("mock error 1")
		queue.ErrorChannel() <- fmt.Errorf("mock error 2")

		// Close and Done on the queue.
		queue.Done()
		err := queue.Close()
		require.Error(t, err)
		assert.Equal(t, "2 errors: mock error 1; mock error 2", err.Error())

		// Batch is also pushed.
		select {
		case batch := <-queue.Channel():
			require.Equal(t, series1, batch.WriteRequest.Timeseries[0])
		default:
			t.Fatal("expected batch to be flushed")
		}
	})
}

func setupQueue(t *testing.T, capacity, batchSize int, series []mimirpb.PreallocTimeseries) *batchingQueue {
	t.Helper()

	queue := newBatchingQueue(capacity, batchSize)

	for _, s := range series {
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s))
	}

	return queue
}
