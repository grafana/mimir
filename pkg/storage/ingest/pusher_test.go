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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

type pusherFunc func(context.Context, *mimirpb.WriteRequest) error

func (p pusherFunc) PushToStorage(ctx context.Context, request *mimirpb.WriteRequest) error {
	return p(ctx, request)
}

func TestPusherConsumer(t *testing.T) {
	const tenantID = "t1"
	writeReqs := []*mimirpb.WriteRequest{
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1"), mockPreallocTimeseries("series_2")}},
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

			receivedReqs := 0
			pusher := pusherFunc(func(ctx context.Context, request *mimirpb.WriteRequest) error {
				defer func() { receivedReqs++ }()
				require.GreaterOrEqualf(t, len(tc.expectedWRs), receivedReqs+1, "received more requests (%d) than expected (%d)", receivedReqs+1, len(tc.expectedWRs))

				expectedWR := tc.expectedWRs[receivedReqs]
				for i, ts := range request.Timeseries {
					assert.Truef(t, ts.Equal(expectedWR.Timeseries[i].TimeSeries), "timeseries %d not equal; got %v, expected %v", i, ts, expectedWR.Timeseries[i].TimeSeries)
				}

				actualTenantID, err := tenant.TenantID(ctx)
				assert.NoError(t, err)
				assert.Equal(t, tenantID, actualTenantID)

				return tc.responses[receivedReqs].err
			})

			logs := &concurrency.SyncBuffer{}
			c := newPusherConsumer(pusher, nil, prometheus.NewPedanticRegistry(), log.NewLogfmtLogger(logs))
			err := c.consume(context.Background(), tc.records)
			if tc.expErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.expErr)
			}

			var logLines []string
			if logsStr := logs.String(); logsStr != "" {
				logLines = strings.Split(strings.TrimSpace(logsStr), "\n")
			}
			assert.Equal(t, tc.expectedLogLines, logLines)
		})
	}
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
			c := newPusherConsumer(nil, tc.sampler, prometheus.NewPedanticRegistry(), log.NewNopLogger())

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
		consumer := newPusherConsumer(pusher, nil, reg, log.NewLogfmtLogger(logs))

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
	consumer := newPusherConsumer(pusher, nil, prometheus.NewPedanticRegistry(), log.NewNopLogger())

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
