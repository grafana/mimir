// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimirpb_testutil "github.com/grafana/mimir/pkg/mimirpb/testutil"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/test"
)

type pusherFunc func(context.Context, *mimirpb.WriteRequest) error

func (p pusherFunc) Close() []error {
	return nil
}

func (p pusherFunc) PushToStorageAndReleaseRequest(ctx context.Context, request *mimirpb.WriteRequest) error {
	return p(ctx, request)
}

func (p pusherFunc) NotifyPreCommit(_ context.Context) error {
	return nil
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

	type response struct {
		err error
	}

	okResponse := response{nil}

	testCases := map[string]struct {
		records     []testRecord
		responses   []response
		expectedWRs []*mimirpb.WriteRequest
		expErr      string

		expectedLogLines []string
	}{
		"single record": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
			},
			expectedWRs: writeReqs[0:1],
		},
		"multiple records": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID},
				{content: wrBytes[2], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				okResponse,
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
		},
		"unparsable record": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: []byte{0}, tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID},
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
		"mixed record versions": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID, version: 0},
				{content: wrBytes[1], tenantID: tenantID, version: 1},
				{content: wrBytes[2], tenantID: tenantID, version: 1},
			},
			responses: []response{
				okResponse,
				okResponse,
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
		},
		"unsupported record version": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID, version: 1},
				{content: wrBytes[2], tenantID: tenantID, version: 101},
			},
			responses: []response{
				okResponse,
				okResponse,
			},
			expectedWRs: writeReqs[0:2],
			expErr:      "",
			expectedLogLines: []string{
				"level=error msg=\"failed to parse write request; skipping\" err=\"parsing ingest consumer write request: received a record with an unsupported version: 101, max supported version: 2\"",
			},
		},
		"failed processing of record": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID},
				{content: wrBytes[2], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"failed processing of last record": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"failed processing & failed unmarshalling": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID},
				{content: []byte{0}, tenantID: tenantID},
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
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID},
				{content: wrBytes[2], tenantID: tenantID},
			},
			responses: []response{
				{err: ingesterError(mimirpb.ERROR_CAUSE_BAD_DATA, codes.InvalidArgument, "ingester test error")},
				{err: ingesterError(mimirpb.ERROR_CAUSE_BAD_DATA, codes.Unknown, "ingester test error")}, // status code doesn't matter
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
			expErr:      "", // since all fof those were client errors, we don't return an error
			expectedLogLines: []string{
				"user=t1 level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" insight=true err=\"rpc error: code = InvalidArgument desc = ingester test error\"",
				"user=t1 level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" insight=true err=\"rpc error: code = Unknown desc = ingester test error\"",
			},
		},
		"ingester server error": {
			records: []testRecord{
				{content: wrBytes[0], tenantID: tenantID},
				{content: wrBytes[1], tenantID: tenantID},
				{content: wrBytes[2], tenantID: tenantID},
				{content: wrBytes[3], tenantID: tenantID},
				{content: wrBytes[4], tenantID: tenantID},
			},
			responses: []response{
				{err: ingesterError(mimirpb.ERROR_CAUSE_BAD_DATA, codes.InvalidArgument, "ingester test error")},
				{err: ingesterError(mimirpb.ERROR_CAUSE_TSDB_UNAVAILABLE, codes.Unavailable, "ingester internal error")},
			},
			expectedWRs: writeReqs[0:2], // the rest of the requests are not attempted
			expErr:      "ingester internal error",
			expectedLogLines: []string{
				"user=t1 level=warn msg=\"detected a client error while ingesting write request (the request may have been partially ingested)\" insight=true err=\"rpc error: code = InvalidArgument desc = ingester test error\"",
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
			metrics := NewPusherConsumerMetrics(prometheus.NewPedanticRegistry())
			c := NewPusherConsumer(pusher, KafkaConfig{}, metrics, log.NewLogfmtLogger(logs))
			err := c.Consume(context.Background(), kafkaRecordsAll(t, tc.records))
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

type testRecord struct {
	version  int
	tenantID string
	content  []byte
}

func kafkaRecordsAll(t testing.TB, records []testRecord) iter.Seq[*kgo.Record] {
	return func(yield func(*kgo.Record) bool) {
		for _, r := range records {
			rec := createRecord("test-topic", 1, r.content, r.version)
			rec.Context = t.Context()
			rec.Key = []byte(r.tenantID)

			if !yield(rec) {
				return
			}
		}
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

func TestPushErrorHandler_IsServerError(t *testing.T) {
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
			c := newPushErrorHandler(newStoragePusherMetrics(prometheus.NewPedanticRegistry()), tc.sampler, log.NewNopLogger())

			sampled, reason := c.shouldLogClientError(context.Background(), tc.err)
			assert.Equal(t, tc.expectedSampled, sampled)
			assert.Equal(t, tc.expectedReason, reason)
		})
	}
}

func TestPusherConsumer_Consume_ShouldLogErrorsHonoringOptionalLogging(t *testing.T) {
	// Create a request that will be used in this test. The content doesn't matter,
	// since we only test errors.
	req := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}}
	reqBytes, err := req.Marshal()
	require.NoError(t, err)

	reqRecord := testRecord{
		tenantID: "user-1",
		content:  reqBytes,
	}

	setupTest := func(pusherErr error) (*PusherConsumer, *concurrency.SyncBuffer, *prometheus.Registry) {
		pusher := pusherFunc(func(context.Context, *mimirpb.WriteRequest) error {
			return pusherErr
		})

		reg := prometheus.NewPedanticRegistry()
		logs := &concurrency.SyncBuffer{}
		consumer := NewPusherConsumer(pusher, KafkaConfig{}, NewPusherConsumerMetrics(reg), log.NewLogfmtLogger(logs))

		return consumer, logs, reg
	}

	t.Run("should log a client error if does not implement optional logging interface", func(t *testing.T) {
		pusherErr := ingesterError(mimirpb.ERROR_CAUSE_BAD_DATA, codes.InvalidArgument, "mocked error")
		consumer, logs, reg := setupTest(pusherErr)

		// Should return no error on client errors.
		require.NoError(t, consumer.Consume(context.Background(), kafkaRecordsAll(t, []testRecord{reqRecord})))

		assert.Contains(t, logs.String(), pusherErr.Error())
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_requests_failed_total Number of write requests which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
			# TYPE cortex_ingest_storage_reader_requests_failed_total counter
			cortex_ingest_storage_reader_requests_failed_total{cause="client"} 1
			cortex_ingest_storage_reader_requests_failed_total{cause="server"} 0
		`), "cortex_ingest_storage_reader_requests_failed_total"))
	})

	t.Run("should log a client error if does implement optional logging interface and ShouldLog() returns true", func(t *testing.T) {
		pusherErrSampler := util_log.NewSampler(100)
		pusherErr := pusherErrSampler.WrapError(ingesterError(mimirpb.ERROR_CAUSE_BAD_DATA, codes.InvalidArgument, "mocked error"))

		// Pre-requisite: the mocked error should implement the optional logging interface.
		var optionalLoggingErr middleware.OptionalLogging
		require.ErrorAs(t, pusherErr, &optionalLoggingErr)

		consumer, logs, reg := setupTest(pusherErr)

		// Should return no error on client errors.
		require.NoError(t, consumer.Consume(context.Background(), kafkaRecordsAll(t, []testRecord{reqRecord})))

		assert.Contains(t, logs.String(), fmt.Sprintf("%s (sampled 1/100)", pusherErr.Error()))
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_requests_failed_total Number of write requests which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
			# TYPE cortex_ingest_storage_reader_requests_failed_total counter
			cortex_ingest_storage_reader_requests_failed_total{cause="client"} 1
			cortex_ingest_storage_reader_requests_failed_total{cause="server"} 0
		`), "cortex_ingest_storage_reader_requests_failed_total"))
	})

	t.Run("should not log a client error if does implement optional logging interface and ShouldLog() returns false", func(t *testing.T) {
		pusherErr := middleware.DoNotLogError{Err: ingesterError(mimirpb.ERROR_CAUSE_BAD_DATA, codes.InvalidArgument, "mocked error")}

		// Pre-requisite: the mocked error should implement the optional logging interface.
		var optionalLoggingErr middleware.OptionalLogging
		require.ErrorAs(t, pusherErr, &optionalLoggingErr)

		consumer, logs, reg := setupTest(pusherErr)

		// Should return no error on client errors.
		require.NoError(t, consumer.Consume(context.Background(), kafkaRecordsAll(t, []testRecord{reqRecord})))

		assert.Empty(t, logs.String())
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_requests_failed_total Number of write requests which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
			# TYPE cortex_ingest_storage_reader_requests_failed_total counter
			cortex_ingest_storage_reader_requests_failed_total{cause="client"} 1
			cortex_ingest_storage_reader_requests_failed_total{cause="server"} 0
		`), "cortex_ingest_storage_reader_requests_failed_total"))
	})

}

func TestPusherConsumer_Consume_ShouldNotLeakGoroutinesOnServerError(t *testing.T) {
	test.VerifyNoLeak(t)

	const (
		tenantID         = "tenant-1"
		numWriteRequests = 1000
	)

	serverErr := errors.New("mocked server error")

	scenarios := map[string]struct {
		cfg KafkaConfig
	}{
		"concurrent ingestion disabled": {
			cfg: func() KafkaConfig {
				cfg := KafkaConfig{}
				flagext.DefaultValues(&cfg)
				cfg.IngestionConcurrencyMax = 0

				return cfg
			}(),
		},
		"concurrent ingestion enabled, concurrency = 2, queue capacity = 1, estimated series per batch = 1": {
			cfg: func() KafkaConfig {
				cfg := KafkaConfig{}
				flagext.DefaultValues(&cfg)
				cfg.IngestionConcurrencyMax = 2
				cfg.IngestionConcurrencyEstimatedBytesPerSample = 1
				cfg.IngestionConcurrencyTargetFlushesPerShard = 1
				cfg.IngestionConcurrencyBatchSize = 1
				cfg.IngestionConcurrencyQueueCapacity = 1

				return cfg
			}(),
		},
		"concurrent ingestion enabled, concurrency = 2, queue capacity = 5, estimated series per batch = 1": {
			cfg: func() KafkaConfig {
				cfg := KafkaConfig{}
				flagext.DefaultValues(&cfg)
				cfg.IngestionConcurrencyMax = 2
				cfg.IngestionConcurrencyEstimatedBytesPerSample = 1
				cfg.IngestionConcurrencyTargetFlushesPerShard = 1
				cfg.IngestionConcurrencyBatchSize = 1
				cfg.IngestionConcurrencyQueueCapacity = 5

				return cfg
			}(),
		},
		"concurrent ingestion enabled, concurrency = 2, queue capacity = 5, estimated series per batch = 5": {
			cfg: func() KafkaConfig {
				cfg := KafkaConfig{}
				flagext.DefaultValues(&cfg)
				cfg.IngestionConcurrencyMax = 2
				cfg.IngestionConcurrencyEstimatedBytesPerSample = 1
				cfg.IngestionConcurrencyTargetFlushesPerShard = 1
				cfg.IngestionConcurrencyBatchSize = 5
				cfg.IngestionConcurrencyQueueCapacity = 5

				return cfg
			}(),
		},
	}

	runForEachScenario := func(t *testing.T, run func(t *testing.T, cfg KafkaConfig, fast bool)) {
		for scenarioName, scenarioData := range scenarios {
			scenarioData := scenarioData

			t.Run(scenarioName, func(t *testing.T) {
				t.Parallel()

				for _, fast := range []bool{true, false} {
					fast := fast

					t.Run(fmt.Sprintf("upstream push is fast: %t", fast), func(t *testing.T) {
						t.Parallel()
						run(t, scenarioData.cfg, fast)
					})
				}
			})
		}
	}

	runTest := func(t *testing.T, cfg KafkaConfig, pusher pusherFunc) {
		records := make([]testRecord, 0, numWriteRequests)

		// Generate the records containing the write requests.
		for i := 0; i < numWriteRequests; i++ {
			req := mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries(fmt.Sprintf("series_%d", i))}}
			marshalled, err := req.Marshal()
			require.NoError(t, err)

			records = append(records, testRecord{content: marshalled, tenantID: tenantID})
		}

		metrics := NewPusherConsumerMetrics(prometheus.NewPedanticRegistry())
		consumer := NewPusherConsumer(pusher, cfg, metrics, log.NewNopLogger())

		// We expect consumption to fail.
		err := consumer.Consume(context.Background(), kafkaRecordsAll(t, records))
		require.ErrorContains(t, err, "consuming record")
		require.ErrorContains(t, err, serverErr.Error())
	}

	t.Run("all push fail", func(t *testing.T) {
		t.Parallel()

		runForEachScenario(t, func(t *testing.T, cfg KafkaConfig, fast bool) {
			pusher := pusherFunc(func(_ context.Context, _ *mimirpb.WriteRequest) error {
				if !fast {
					time.Sleep(time.Duration(rand.Int63n(int64(250 * time.Millisecond))))
				}

				return serverErr
			})

			runTest(t, cfg, pusher)
		})
	})

	t.Run("the 1st push fail", func(t *testing.T) {
		t.Parallel()

		runForEachScenario(t, func(t *testing.T, cfg KafkaConfig, fast bool) {
			pushCount := atomic.NewInt64(0)

			pusher := pusherFunc(func(_ context.Context, _ *mimirpb.WriteRequest) error {
				if !fast {
					time.Sleep(time.Duration(rand.Int63n(int64(250 * time.Millisecond))))
				}

				if pushCount.Inc() == 1 {
					return serverErr
				}

				return nil
			})

			runTest(t, cfg, pusher)
		})
	})

	t.Run("the 10th push fail", func(t *testing.T) {
		t.Parallel()

		runForEachScenario(t, func(t *testing.T, cfg KafkaConfig, fast bool) {
			pushCount := atomic.NewInt64(0)

			pusher := pusherFunc(func(_ context.Context, _ *mimirpb.WriteRequest) error {
				if !fast {
					time.Sleep(time.Duration(rand.Int63n(int64(250 * time.Millisecond))))
				}

				if pushCount.Inc() == 10 {
					return serverErr
				}

				return nil
			})

			runTest(t, cfg, pusher)
		})
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

func (m *mockPusher) PushToStorageAndReleaseRequest(ctx context.Context, request *mimirpb.WriteRequest) error {
	args := m.Called(ctx, comparableWriteRequest(request))
	request.FreeBuffer()
	return args.Error(0)
}

func comparableWriteRequest(wr *mimirpb.WriteRequest) *mimirpb.WriteRequest {
	b, err := wr.Marshal()
	if err != nil {
		panic(err)
	}
	var clone mimirpb.WriteRequest
	err = clone.Unmarshal(b)
	if err != nil {
		panic(err)
	}
	return &clone
}

func asDeserializedWriteRequest(wr *mimirpb.WriteRequest) *mimirpb.WriteRequest {
	b, err := wr.Marshal()
	if err != nil {
		panic(err)
	}
	var preq mimirpb.PreallocWriteRequest
	err = DeserializeRecordContent(b, &preq, 1)
	if err != nil {
		panic(err)
	}
	return &preq.WriteRequest
}

func (m *mockPusher) NotifyPreCommit(_ context.Context) error {
	args := m.Called()
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
		"preserving sample order between requests": {
			shardCount: 4,
			batchSize:  4, // Large enough to hold all samples for one series
			requests: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithSample("series_a", 1, 2),
					mockPreallocTimeseriesWithSample("series_b", 1, 2),
					mockPreallocTimeseriesWithSample("series_c", 1, 2),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithSample("series_b", 2, 3),
					mockPreallocTimeseriesWithSample("series_a", 2, 3),
					mockPreallocTimeseriesWithSample("series_c", 2, 3),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithSample("series_b", 3, 4),
					mockPreallocTimeseriesWithSample("series_c", 3, 4),
					mockPreallocTimeseriesWithSample("series_a", 3, 4),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithSample("series_c", 4, 5),
					mockPreallocTimeseriesWithSample("series_b", 4, 5),
					mockPreallocTimeseriesWithSample("series_a", 4, 5),
				}},
			},
			// We expect no errors during the push calls themselves as flushing happens on Close.
			expectedErrs: []error{nil, nil, nil, nil},

			// We expect 3 final pushes after Close(), one for each shard.
			// The exact content depends on the hashing, so we can't predict the exact order or grouping easily.
			// We'll verify the content manually in the test logic override below.
			expectedUpstreamPushes: []*mimirpb.WriteRequest{
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithSample("series_a", 1, 2),
					mockPreallocTimeseriesWithSample("series_a", 2, 3),
					mockPreallocTimeseriesWithSample("series_a", 3, 4),
					mockPreallocTimeseriesWithSample("series_a", 4, 5),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithSample("series_b", 1, 2),
					mockPreallocTimeseriesWithSample("series_b", 2, 3),
					mockPreallocTimeseriesWithSample("series_b", 3, 4),
					mockPreallocTimeseriesWithSample("series_b", 4, 5),
				}},
				{Timeseries: []mimirpb.PreallocTimeseries{
					mockPreallocTimeseriesWithSample("series_c", 1, 2),
					mockPreallocTimeseriesWithSample("series_c", 2, 3),
					mockPreallocTimeseriesWithSample("series_c", 3, 4),
					mockPreallocTimeseriesWithSample("series_c", 4, 5),
				}},
			},
			upstreamPushErrs: []error{nil, nil, nil},
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
			reg := prometheus.NewPedanticRegistry()
			metrics := newStoragePusherMetrics(reg)
			errorHandler := newPushErrorHandler(metrics, nil, log.NewNopLogger())
			shardingP := newParallelStorageShards(metrics, errorHandler, tc.shardCount, tc.batchSize, buffer, pusher)

			for i, req := range tc.requests {
				req = asDeserializedWriteRequest(req)
				trackedBuf := mimirpb_testutil.TrackBuffer(req)
				tc.requests[i] = req

				// When everything's done, check that there are no buffer leaks.
				defer func() {
					require.Equal(t, 0, trackedBuf.RefCount())
				}()
			}

			upstreamPushErrsCount := 0
			for i, req := range tc.expectedUpstreamPushes {
				err := tc.upstreamPushErrs[i]
				pusher.On("PushToStorageAndReleaseRequest", mock.Anything, comparableWriteRequest(req)).Return(err)
				if err != nil {
					upstreamPushErrsCount++
				}
			}
			var actualPushErrs []error
			for _, req := range tc.requests {
				err := shardingP.PushToStorageAndReleaseRequest(context.Background(), req)
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

			closeErr := shardingP.Close()
			if tc.expectedCloseErr != nil {
				require.Len(t, closeErr, 1)
				require.ErrorIs(t, closeErr[0], tc.expectedCloseErr)
			} else {
				require.Empty(t, closeErr)
			}
			pusher.AssertNumberOfCalls(t, "PushToStorageAndReleaseRequest", len(tc.expectedUpstreamPushes))
			pusher.AssertExpectations(t)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_ingest_storage_reader_requests_total Number of attempted write requests after batching records from Kafka.
				# TYPE cortex_ingest_storage_reader_requests_total counter
				cortex_ingest_storage_reader_requests_total %d
				# HELP cortex_ingest_storage_reader_requests_failed_total Number of write requests which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.
				# TYPE cortex_ingest_storage_reader_requests_failed_total counter
				cortex_ingest_storage_reader_requests_failed_total{cause="server"} %d
				cortex_ingest_storage_reader_requests_failed_total{cause="client"} 0
			`, len(tc.expectedUpstreamPushes), upstreamPushErrsCount)),
				"cortex_ingest_storage_reader_requests_total",
				"cortex_ingest_storage_reader_requests_failed_total",
			),
			)
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

	for _, sequentialPusherEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("sequentialPusherEnabled=%v", sequentialPusherEnabled), func(t *testing.T) {
			for name, tc := range testCases {
				t.Run(name, func(t *testing.T) {
					pusher := &mockPusher{}
					logger := log.NewNopLogger()

					receivedPushes := make(map[string]map[mimirpb.WriteRequest_SourceEnum]int)
					var receivedPushesMu sync.Mutex

					pusher.On("PushToStorageAndReleaseRequest", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
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

					samplesPerTenant := make(map[string]int)
					for _, req := range tc.requests {
						samplesPerTenant[req.tenantID] += len(req.Timeseries)
					}

					metrics := newStoragePusherMetrics(prometheus.NewPedanticRegistry())
					psp := newParallelStoragePusher(metrics, pusher, samplesPerTenant, 0, 1, 1, 5, 500, 80, sequentialPusherEnabled, logger)

					// Process requests
					for _, req := range tc.requests {
						ctx := user.InjectOrgID(context.Background(), req.tenantID)
						err := psp.PushToStorageAndReleaseRequest(ctx, req.WriteRequest)
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
		})
	}
}

func TestParallelStoragePusher_idealShardsFor(t *testing.T) {
	testCases := map[string]struct {
		bytesPerTenant int
		bytesPerSample int
		batchSize      int
		targetFlushes  int
		maxShards      int

		expected int
	}{
		"with a low number of time series and target flushes, we expect a low number of shards": {
			bytesPerTenant: 1000,
			bytesPerSample: 100,
			batchSize:      10,
			targetFlushes:  1,
			maxShards:      1,

			expected: 1,
		},
		"with a higher number of max shards, the ideal number of shards is returned": {
			bytesPerTenant: 10000,
			bytesPerSample: 100,
			batchSize:      10,
			targetFlushes:  1,
			maxShards:      15,

			expected: 10,
		},
		"with a higher number of shards, we're capped by max shards ": {
			bytesPerTenant: 100000,
			bytesPerSample: 100,
			batchSize:      10,
			targetFlushes:  1,
			maxShards:      5,

			expected: 5,
		},
		"with a small number of batches and a small number of flushes, we expect more shards": {
			bytesPerTenant: 20000,
			bytesPerSample: 200,
			batchSize:      5,
			targetFlushes:  2,
			maxShards:      15,

			expected: 10,
		},
		"when we expect less than 1 shard, it should still be 1": {
			bytesPerTenant: 500,
			bytesPerSample: 100,
			batchSize:      10,
			targetFlushes:  1,
			maxShards:      10,

			expected: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			psp := &parallelStoragePusher{
				bytesPerTenant: map[string]int{"tenant1": tc.bytesPerTenant},
				bytesPerSample: tc.bytesPerSample,
				batchSize:      tc.batchSize,
				targetFlushes:  tc.targetFlushes,
				maxShards:      tc.maxShards,
				metrics:        newStoragePusherMetrics(prometheus.NewPedanticRegistry()),
			}

			idealShards := psp.idealShardsFor("tenant1")
			require.Equal(t, tc.expected, idealShards, "Mismatch in ideal shards")
		})
	}
}

func TestParallelStoragePusher_Fuzzy(t *testing.T) {
	test.VerifyNoLeak(t)

	for _, sequentialPusherEnabled := range []bool{true, false} {
		t.Run(fmt.Sprintf("sequentialPusherEnabled=%v", sequentialPusherEnabled), func(t *testing.T) {
			const (
				numTenants               = 10
				numWriteRequests         = 100
				numSeriesPerWriteRequest = 100
			)

			var (
				serverErr                   = errors.New("mocked server error")
				serverErrsCount             = atomic.NewInt64(0)
				enqueuedTimeSeriesReqs      = 0
				enqueuedTimeSeriesPerTenant = map[string][]string{}
				pushedTimeSeriesPerTenant   = map[string][]string{}
				pushedTimeSeriesPerTenantMx = sync.Mutex{}
			)

			generateWriteRequest := func(batchID, numSeries int) *mimirpb.WriteRequest {
				timeseries := make([]mimirpb.PreallocTimeseries, 0, numSeries)
				for i := 0; i < numSeries; i++ {
					timeseries = append(timeseries, mockPreallocTimeseries(fmt.Sprintf("series_%d_%d", batchID, i)))
				}

				return &mimirpb.WriteRequest{Timeseries: timeseries, Source: mimirpb.API}
			}

			pusher := pusherFunc(func(ctx context.Context, req *mimirpb.WriteRequest) error {
				tenantID, err := user.ExtractOrgID(ctx)
				require.NoError(t, err)

				// Keep track of the received series.
				pushedTimeSeriesPerTenantMx.Lock()
				for _, series := range req.Timeseries {
					pushedTimeSeriesPerTenant[tenantID] = append(pushedTimeSeriesPerTenant[tenantID], series.Labels[0].Value)
				}
				pushedTimeSeriesPerTenantMx.Unlock()

				// Simulate some slowdown.
				time.Sleep(time.Millisecond)

				// Simulate a 0.1% failure rate.
				if rand.Intn(1000) == 0 {
					serverErrsCount.Inc()
					return serverErr
				}

				return nil
			})

			// Configure the pusher so that it uses multiple shards per tenant.
			const maxShards = 10
			const batchSize = 1
			const queueCapacity = 5
			const bytesPerSample = 1
			const targetFlushes = 1
			bytesPerTenant := map[string]int{}
			for tenantID := 0; tenantID < numTenants; tenantID++ {
				bytesPerTenant[fmt.Sprintf("tenant-%d", tenantID)] = 5
			}

			metrics := newStoragePusherMetrics(prometheus.NewPedanticRegistry())
			psp := newParallelStoragePusher(metrics, pusher, bytesPerTenant, 0, maxShards, batchSize, queueCapacity, bytesPerSample, targetFlushes, sequentialPusherEnabled, nil)

			// Keep pushing batches of write requests until we hit an error.
			for batchID := 0; batchID < numWriteRequests; batchID++ {
				// Generate the tenant owning this batch.
				tenantID := fmt.Sprintf("tenant-%d", batchID%numTenants)
				ctx := user.InjectOrgID(context.Background(), tenantID)

				// Ensure the tenant will use more than 1 shard.
				require.Greater(t, psp.idealShardsFor(tenantID), 1)

				req := generateWriteRequest(batchID, numSeriesPerWriteRequest)

				// We need this for later, but psp's PushToStorage destroys the request by freeing resources.
				requestSeriesLabelValues := []string{}
				for _, series := range req.Timeseries {
					requestSeriesLabelValues = append(requestSeriesLabelValues, series.Labels[0].Value)
				}
				if err := psp.PushToStorageAndReleaseRequest(ctx, req); err == nil {
					enqueuedTimeSeriesReqs++

					// Keep track of the enqueued series. We don't keep track of it if there was an error because, in case
					// of an error, only some series may been added to a batch (it breaks on the first failed "append to batch").
					enqueuedTimeSeriesPerTenant[tenantID] = append(enqueuedTimeSeriesPerTenant[tenantID], requestSeriesLabelValues...)
				} else {
					// We received an error. Make sure a server error was reported by the upstream pusher.
					require.Greater(t, serverErrsCount.Load(), int64(0))

					// Break on first error.
					break
				}
			}

			t.Logf("enqueued time series requests: %d", enqueuedTimeSeriesReqs)

			// Close the parallel pusher.
			if errs := psp.Close(); len(errs) > 0 {
				// We received an error. Make sure a server error was reported by the upstream pusher.
				require.Greater(t, serverErrsCount.Load(), int64(0))
			}

			// Ensure all enqueued series have been pushed.
			for tenantID, tenantSeries := range enqueuedTimeSeriesPerTenant {
				for _, series := range tenantSeries {
					require.Contains(t, pushedTimeSeriesPerTenant[tenantID], series)
				}
			}
		})
	}
}

func TestBatchingQueue_NoDeadlock(t *testing.T) {
	capacity := 2
	batchSize := 3
	reg := prometheus.NewPedanticRegistry()
	m := newBatchingQueueMetrics(reg)
	queue := newBatchingQueue(capacity, batchSize, m)

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
			queue.ReportError(fmt.Errorf("mock error"))
		}
	}()

	// Add items to the queue
	for i := 0; i < batchSize*(capacity+1); i++ {
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series, &mimirpb.BufferHolder{}))
	}

	// Close the queue to signal no more items will be added
	err := queue.Close()
	require.ErrorContains(t, err, "mock error")

	wg.Wait()

	// Ensure the queue is empty and no deadlock occurred
	require.Len(t, queue.ch, 0)
	require.Len(t, queue.errs, 0)
	require.Len(t, queue.currentBatch.Timeseries, 0)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP cortex_ingest_storage_reader_batching_queue_flush_errors_total Number of errors encountered while flushing a batch of samples to the storage.
# TYPE cortex_ingest_storage_reader_batching_queue_flush_errors_total counter
cortex_ingest_storage_reader_batching_queue_flush_errors_total 0
# HELP cortex_ingest_storage_reader_batching_queue_flush_total Number of times a batch of samples is flushed to the storage.
# TYPE cortex_ingest_storage_reader_batching_queue_flush_total counter
cortex_ingest_storage_reader_batching_queue_flush_total 3
`)))
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
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, series3, &mimirpb.BufferHolder{}))

		select {
		case batch := <-queue.Channel():
			require.Len(t, batch.Timeseries, 3)
			require.Equal(t, series1, batch.Timeseries[0])
			require.Equal(t, series2, batch.Timeseries[1])
			require.Equal(t, series3, batch.Timeseries[2])
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
				queue.ReportError(nil)
			}
		}()

		// Close the queue, and the items should be flushed.
		require.NoError(t, queue.Close())

		require.Len(t, batch.Timeseries, 2)
		require.Equal(t, series1, batch.Timeseries[0])
		require.Equal(t, series2, batch.Timeseries[1])
	})

	t.Run("test queue capacity", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, nil)

		// Queue channel is empty because there are only 2 items in the current currentBatch.
		require.Len(t, queue.ch, 0)
		require.Len(t, queue.currentBatch.Timeseries, 0)

		// Add items to the queue until it's full.
		for i := 0; i < capacity*batchSize; i++ {
			s := mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
			require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s, &mimirpb.BufferHolder{}))
		}

		// We should have 5 items in the queue channel and 0 items in the currentBatch.
		require.Len(t, queue.ch, 5)
		require.Len(t, queue.currentBatch.Timeseries, 0)

		// Read one item to free up a queue space.
		batch := <-queue.Channel()
		require.Len(t, batch.Timeseries, 3)

		// Queue should have 4 items now and the currentBatch remains the same.
		require.Len(t, queue.ch, 4)
		require.Len(t, queue.currentBatch.Timeseries, 0)

		// Add three more items to fill up the queue again, this shouldn't block.
		s := mockPreallocTimeseries("series_100")
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s, &mimirpb.BufferHolder{}))
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s, &mimirpb.BufferHolder{}))
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s, &mimirpb.BufferHolder{}))

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
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, timeSeries, &mimirpb.BufferHolder{}))

		// Add metadata to the queue
		require.NoError(t, queue.AddMetadataToBatch(context.Background(), mimirpb.API, md, &mimirpb.BufferHolder{}))

		// Read the batch from the queue
		select {
		case batch, notExhausted := <-queue.Channel():
			require.True(t, notExhausted)
			require.Len(t, batch.Timeseries, 1)
			require.Len(t, batch.Metadata, 1)

			// Check that the first series in the batch has the correct exemplars
			require.Len(t, batch.Timeseries[0].Exemplars, 1)
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
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series2, &mimirpb.BufferHolder{}))

		// Push an error to fill the error channel.
		queue.ReportError(fmt.Errorf("mock error 1"))
		queue.ReportError(fmt.Errorf("mock error 2"))

		// AddToBatch should return an error now.
		err := queue.AddToBatch(ctx, mimirpb.API, series2, &mimirpb.BufferHolder{})
		assert.Equal(t, "2 errors: mock error 1; mock error 2", err.Error())
		// Also the batch was pushed.
		select {
		case batch := <-queue.Channel():
			require.Equal(t, series2, batch.Timeseries[0])
			require.Equal(t, series2, batch.Timeseries[1])
		default:
			t.Fatal("expected batch to be flushed")
		}

		// AddToBatch should work again.
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series2, &mimirpb.BufferHolder{}))
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series2, &mimirpb.BufferHolder{}))
	})

	t.Run("Any errors pushed after last AddToBatch call are received on Close", func(t *testing.T) {
		queue := setupQueue(t, capacity, batchSize, nil)
		ctx := context.Background()

		// Add a batch to a batch but make sure nothing is pushed.,
		require.NoError(t, queue.AddToBatch(ctx, mimirpb.API, series1, &mimirpb.BufferHolder{}))

		select {
		case <-queue.Channel():
			t.Fatal("expected batch to not be flushed")
		default:
		}

		// Push multiple errors
		queue.ReportError(fmt.Errorf("mock error 1"))
		queue.ReportError(fmt.Errorf("mock error 2"))

		// Close and Done on the queue.
		queue.Done()
		err := queue.Close()
		require.Error(t, err)
		assert.Equal(t, "2 errors: mock error 1; mock error 2", err.Error())

		// Batch is also pushed.
		select {
		case batch := <-queue.Channel():
			require.Equal(t, series1, batch.Timeseries[0])
		default:
			t.Fatal("expected batch to be flushed")
		}
	})
}

func setupQueue(t *testing.T, capacity, batchSize int, series []mimirpb.PreallocTimeseries) *batchingQueue {
	t.Helper()

	reg := prometheus.NewPedanticRegistry()
	m := newBatchingQueueMetrics(reg)
	queue := newBatchingQueue(capacity, batchSize, m)

	for _, s := range series {
		require.NoError(t, queue.AddToBatch(context.Background(), mimirpb.API, s, &mimirpb.BufferHolder{}))
	}

	return queue
}

func BenchmarkPusherConsumer(b *testing.B) {
	pusher := pusherFunc(func(ctx context.Context, request *mimirpb.WriteRequest) error {
		request.FreeBuffer()
		mimirpb.ReuseSlice(request.Timeseries)
		return nil
	})

	records := make([]*kgo.Record, 50)
	for i := range records {
		wr := &mimirpb.WriteRequest{Timeseries: make([]mimirpb.PreallocTimeseries, 100)}
		for j := range len(wr.Timeseries) {
			wr.Timeseries[j] = mockPreallocTimeseries(fmt.Sprintf("series_%d", i))
		}
		content, err := wr.Marshal()
		require.NoError(b, err)
		records[i] = createRecord("test-topic", 1, content, 1)
		records[i].Context = context.Background()
	}

	b.Run("sequential pusher", func(b *testing.B) {
		kcfg := KafkaConfig{}
		flagext.DefaultValues(&kcfg)
		kcfg.IngestionConcurrencyMax = 0
		metrics := NewPusherConsumerMetrics(prometheus.NewPedanticRegistry())
		c := NewPusherConsumer(pusher, kcfg, metrics, log.NewNopLogger())
		b.ResetTimer()

		for range b.N {
			err := c.Consume(context.Background(), slices.Values(records))
			require.NoError(b, err)
		}
	})

	b.Run("parallel pusher", func(b *testing.B) {
		kcfg := KafkaConfig{}
		flagext.DefaultValues(&kcfg)
		kcfg.IngestionConcurrencyMax = 2
		metrics := NewPusherConsumerMetrics(prometheus.NewPedanticRegistry())
		c := NewPusherConsumer(pusher, kcfg, metrics, log.NewNopLogger())
		b.ResetTimer()

		for range b.N {
			err := c.Consume(context.Background(), slices.Values(records))
			require.NoError(b, err)
		}
	})
}

func BenchmarkPusherConsumer_ParallelPusher_MultiTenant(b *testing.B) {
	const timeseriesPerRecord = 10

	// Create a no-op pusher that just releases the request (shared across all runs).
	pusher := pusherFunc(func(_ context.Context, request *mimirpb.WriteRequest) error {
		request.FreeBuffer()
		mimirpb.ReuseSlice(request.Timeseries)
		return nil
	})

	// Configure the ingester with Kafka settings using the same settings used in Grafana Cloud.
	kcfg := KafkaConfig{}
	flagext.DefaultValues(&kcfg)
	kcfg.IngestionConcurrencyMax = 8
	kcfg.IngestionConcurrencyBatchSize = 150
	kcfg.IngestionConcurrencyEstimatedBytesPerSample = 200
	kcfg.IngestionConcurrencyQueueCapacity = 3
	kcfg.IngestionConcurrencyTargetFlushesPerShard = 40
	kcfg.IngestionConcurrencySequentialPusherEnabled = false

	for _, numRecords := range []int{1, 10, 100, 1000} {
		for _, numTenants := range []int{1, 10, 100, 1000} {
			// Create records upfront, outside b.Run(), so record creation doesn't affect timing.
			records := make([]*kgo.Record, numRecords)
			for i := range records {
				tenantID := fmt.Sprintf("tenant-%d", i%numTenants)
				wr := &mimirpb.WriteRequest{Timeseries: make([]mimirpb.PreallocTimeseries, timeseriesPerRecord)}
				for j := range wr.Timeseries {
					wr.Timeseries[j] = mockPreallocTimeseries(fmt.Sprintf("series_%d_%d", i, j))
				}
				content, err := wr.Marshal()
				require.NoError(b, err)

				records[i] = createRecord("test-topic", 1, content, 1)
				records[i].Key = []byte(tenantID)
				records[i].Context = context.Background()
			}

			b.Run(fmt.Sprintf("records=%d,tenants=%d", numRecords, numTenants), func(b *testing.B) {
				metrics := NewPusherConsumerMetrics(prometheus.NewPedanticRegistry())
				c := NewPusherConsumer(pusher, kcfg, metrics, log.NewNopLogger())

				b.ResetTimer()
				for range b.N {
					err := c.Consume(context.Background(), slices.Values(records))
					require.NoError(b, err)
				}
			})
		}
	}
}
