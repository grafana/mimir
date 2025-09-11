// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/v2/frontend_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package v2

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/servicediscovery"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	utiltest "github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	utiltest.VerifyNoLeakTestMain(m)
}

const testFrontendWorkerConcurrency = 5

func setupFrontend(t *testing.T, reg prometheus.Registerer, schedulerReplyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend) (*Frontend, *mockScheduler) {
	return setupFrontendWithConcurrencyAndServerOptions(t, reg, schedulerReplyFunc, testFrontendWorkerConcurrency)
}

func setupFrontendWithConcurrencyAndServerOptions(t *testing.T, reg prometheus.Registerer, schedulerReplyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend, concurrency int, opts ...grpc.ServerOption) (*Frontend, *mockScheduler) {
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	server := grpc.NewServer(opts...)

	h, p, err := net.SplitHostPort(l.Addr().String())
	require.NoError(t, err)

	grpcPort, err := strconv.Atoi(p)
	require.NoError(t, err)

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.SchedulerAddress = l.Addr().String()
	cfg.WorkerConcurrency = concurrency
	cfg.Addr = h
	cfg.Port = grpcPort

	logger := log.NewLogfmtLogger(os.Stdout)
	codec := querymiddleware.NewCodec(prometheus.NewPedanticRegistry(), 0*time.Minute, "json", nil)

	f, err := NewFrontend(cfg, limits{}, logger, reg, codec)
	require.NoError(t, err)

	frontendv2pb.RegisterFrontendForQuerierServer(server, f)

	ms := newMockScheduler(t, f, schedulerReplyFunc)
	schedulerpb.RegisterSchedulerForFrontendServer(server, ms)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
		server.GracefulStop()
	})

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), f))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), f)
	})

	// Wait for frontend to connect to scheduler.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		ms.mu.Lock()
		defer ms.mu.Unlock()

		return len(ms.frontendAddr)
	})

	return f, ms
}

func sendResponseWithDelay(f *Frontend, delay time.Duration, userID string, queryID uint64, resp *httpgrpc.HTTPResponse) {
	if delay > 0 {
		time.Sleep(delay)
	}

	ctx := user.InjectOrgID(context.Background(), userID)
	_, _ = f.QueryResult(ctx, &frontendv2pb.QueryResultRequest{
		QueryID:      queryID,
		HttpResponse: resp,
		Stats:        &stats.SafeStats{},
	})
}

func sendStreamingResponse(t *testing.T, f *Frontend, userID string, queryID uint64, resp ...*frontendv2pb.QueryResultStreamRequest) {
	for _, m := range resp {
		m.QueryID = queryID
	}

	ctx := user.InjectOrgID(context.Background(), userID)
	stream := &mockQueryResultStreamServer{
		ctx:  ctx,
		msgs: resp,
	}

	err := f.QueryResultStream(stream)
	if err != nil {
		// If QueryResultStream fails, it's not necessarily a problem (eg. it might be that the context was cancelled)
		// So just log it but don't fail the test.
		t.Logf("sendStreamingResponse: QueryResultStream returned %v", err)
	}
}

func TestFrontend_HTTPGRPC_HappyPath(t *testing.T) {
	const (
		body   = "all fine here"
		userID = "test"
	)

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		// We cannot call QueryResult directly, as Frontend is not yet waiting for the response.
		// It first needs to be told that enqueuing has succeeded.
		go sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
			Code: 200,
			Body: []byte(body),
		})

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	req := &httpgrpc.HTTPRequest{
		Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
	}
	resp, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)
	require.Equal(t, int32(200), resp.Code)
	require.Equal(t, []byte(body), resp.Body)
}

func TestFrontend_Protobuf_HappyPath(t *testing.T) {
	const userID = "test"

	expectedMessages := []*frontendv2pb.QueryResultStreamRequest{
		newStringMessage("first message"),
		newStringMessage("second message"),
	}

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		go sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[0], msg)
	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[1], msg)

	// Response stream exhausted.
	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, msg)
}

func TestFrontend_Protobuf_QuerierResponseReceivedBeforeSchedulerResponse(t *testing.T) {
	const userID = "test"

	expectedMessages := []*frontendv2pb.QueryResultStreamRequest{
		newStringMessage("first message"),
		newStringMessage("second message"),
	}

	responseRead := make(chan struct{})

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		// Send the entire response, and wait until it has been received by the test below.
		sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		select {
		case <-responseRead:
			// Test has read the entire response, continue.
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for response to be read by test")
		}

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[0], msg)
	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[1], msg)

	// Response stream exhausted.
	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, msg)

	close(responseRead)
}

func TestFrontend_Protobuf_ResponseClosedBeforeStreamExhausted(t *testing.T) {
	const userID = "test"

	expectedMessages := []*frontendv2pb.QueryResultStreamRequest{
		newStringMessage("first message"),
		newStringMessage("second message"),
		newStringMessage("third message"),
	}

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		go sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[0], msg)
	resp.Close() // We expect all goroutines to be cleaned up after this (verified by the VerifyNoLeakTestMain call in TestMain above)
}

func TestFrontend_Protobuf_ErrorReturnedByQuerier(t *testing.T) {
	const userID = "test"

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		errorMessage := newErrorMessage(mimirpb.QUERY_ERROR_TYPE_BAD_DATA, "something went wrong")
		go sendStreamingResponse(t, f, userID, msg.QueryID, errorMessage)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.Equal(t, apierror.New(apierror.TypeBadData, "something went wrong"), err)
	require.Nil(t, msg)
}

func TestFrontend_ShouldTrackPerRequestMetrics(t *testing.T) {
	const (
		body   = "all fine here"
		userID = "test"
	)

	testCases := map[string]struct {
		sendQuerierResponse func(t *testing.T, f *Frontend, queryID uint64)
		makeRequest         func(t *testing.T, f *Frontend)
	}{
		"HTTP-over-gRPC": {
			sendQuerierResponse: func(t *testing.T, f *Frontend, queryID uint64) {
				// We cannot call QueryResult directly, as Frontend is not yet waiting for the response.
				// It first needs to be told that enqueuing has succeeded.
				go sendResponseWithDelay(f, 100*time.Millisecond, userID, queryID, &httpgrpc.HTTPResponse{
					Code: 200,
					Body: []byte(body),
				})
			},
			makeRequest: func(t *testing.T, f *Frontend) {
				req := &httpgrpc.HTTPRequest{
					Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
				}
				resp, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
				require.NoError(t, err)
				require.Equal(t, int32(200), resp.Code)
				require.Equal(t, []byte(body), resp.Body)
			},
		},
		"Protobuf": {
			sendQuerierResponse: func(t *testing.T, f *Frontend, queryID uint64) {
				go sendStreamingResponse(t, f, userID, queryID, newStringMessage("the message"))
			},
			makeRequest: func(t *testing.T, f *Frontend) {
				ctx := user.InjectOrgID(context.Background(), userID)
				req := &querierpb.EvaluateQueryRequest{}
				resp, err := f.DoProtobufRequest(ctx, req)
				require.NoError(t, err)
				t.Cleanup(resp.Close)
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewRegistry()

			f, _ := setupFrontend(t, reg, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				testCase.sendQuerierResponse(t, f, msg.QueryID)
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			})

			// Assert on cortex_query_frontend_enqueue_duration_seconds.
			metricsMap, err := metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			require.NotEmpty(t, metricsMap["cortex_query_frontend_enqueue_duration_seconds"])
			require.Len(t, metricsMap["cortex_query_frontend_enqueue_duration_seconds"].GetMetric(), 1)
			assert.Equal(t, makeLabels("scheduler_address", f.cfg.SchedulerAddress), metricsMap["cortex_query_frontend_enqueue_duration_seconds"].GetMetric()[0].GetLabel())
			assert.Equal(t, uint64(0), metricsMap["cortex_query_frontend_enqueue_duration_seconds"].GetMetric()[0].GetHistogram().GetSampleCount())

			testCase.makeRequest(t, f)

			// Wait for the request to be sent to the scheduler.
			// For HTTP-over-gRPC, this should be true before RoundTripGRPC returns, but for Protobuf requests,
			// this is done asynchronously after DoProtobufRequest returns.
			require.Eventually(t, func() bool {
				metricsMap, err = metrics.NewMetricFamilyMapFromGatherer(reg)
				require.NoError(t, err)
				require.NotEmpty(t, metricsMap["cortex_query_frontend_enqueue_duration_seconds"])
				require.Len(t, metricsMap["cortex_query_frontend_enqueue_duration_seconds"].GetMetric(), 1)
				assert.Equal(t, makeLabels("scheduler_address", f.cfg.SchedulerAddress), metricsMap["cortex_query_frontend_enqueue_duration_seconds"].GetMetric()[0].GetLabel())
				return metricsMap["cortex_query_frontend_enqueue_duration_seconds"].GetMetric()[0].GetHistogram().GetSampleCount() == 1
			}, time.Second, time.Millisecond*100)

			// Manually remove the address, check that label is removed.
			f.schedulerWorkers.InstanceRemoved(servicediscovery.Instance{Address: f.cfg.SchedulerAddress, InUse: true})

			metricsMap, err = metrics.NewMetricFamilyMapFromGatherer(reg)
			require.NoError(t, err)
			assert.Empty(t, metricsMap["cortex_query_frontend_enqueue_duration_seconds"])
		})
	}
}

func TestFrontend_HTTPGRPC_RetryEnqueue(t *testing.T) {
	// Frontend uses worker concurrency to compute number of retries. We use one less failure.
	failures := atomic.NewInt64(testFrontendWorkerConcurrency - 1)
	const (
		body   = "hello world"
		userID = "test"
	)

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		fail := failures.Dec()
		if fail >= 0 {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
		}

		go sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
			Code: 200,
			Body: []byte(body),
		})

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})
	req := &httpgrpc.HTTPRequest{
		Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
	}
	_, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)
}

func TestFrontend_Protobuf_RetryEnqueue(t *testing.T) {
	// Frontend uses worker concurrency to compute number of retries. We use one less failure.
	failures := atomic.NewInt64(testFrontendWorkerConcurrency - 1)
	const userID = "test"

	expectedMessages := []*frontendv2pb.QueryResultStreamRequest{
		newStringMessage("first message"),
	}

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		fail := failures.Dec()
		if fail >= 0 {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
		}

		go sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[0], msg)
}

func TestFrontend_Protobuf_EnqueueRetriesExhausted(t *testing.T) {
	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.Equal(t, apierror.New(apierror.TypeInternal, "failed to enqueue request"), err)
	require.Nil(t, msg)
}

func TestFrontend_Protobuf_ReadingResponseAfterAllMessagesReceived(t *testing.T) {
	const userID = "test"

	expectedMessages := []*frontendv2pb.QueryResultStreamRequest{
		newStringMessage("first message"),
		newStringMessage("second message"),
		newStringMessage("third message"),
	}

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		go sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[0], msg)

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[1], msg)

	// Wait until the last message has been buffered into the stream channel and the stream's context has been cancelled by DoProtobufRequest.
	select {
	case <-resp.ctx.Done():
		// Context cancelled, continue.
	case <-time.After(time.Second):
		require.Fail(t, "gave up waiting for stream context to be cancelled")
	}

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedMessages[2], msg, "should still be able to read last message after stream has been completely read")

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, msg)
}

func TestFrontend_HTTPGRPC_TooManyRequests(t *testing.T) {
	schedulerEnqueueAttempts := atomic.NewInt64(0)
	f, _ := setupFrontend(t, nil, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		schedulerEnqueueAttempts.Inc()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
	})

	req := &httpgrpc.HTTPRequest{
		Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
	}
	resp, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), "test"), req)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusTooManyRequests), resp.Code)

	require.Equal(t, int64(1), schedulerEnqueueAttempts.Load(), "should not retry on 'too many outstanding requests' error")
}

func TestFrontend_Protobuf_TooManyRequests(t *testing.T) {
	schedulerEnqueueAttempts := atomic.NewInt64(0)
	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		schedulerEnqueueAttempts.Inc()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.Equal(t, apierror.New(apierror.TypeTooManyRequests, "too many outstanding requests"), err)
	require.Nil(t, msg)

	require.Equal(t, int64(1), schedulerEnqueueAttempts.Load(), "should not retry on 'too many outstanding requests' error")
}

func TestFrontend_HTTPGRPC_SchedulerError(t *testing.T) {
	schedulerEnqueueAttempts := atomic.NewInt64(0)
	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		schedulerEnqueueAttempts.Inc()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "something went wrong inside the scheduler"}
	})

	req := &httpgrpc.HTTPRequest{
		Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
	}
	resp, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), "test"), req)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusInternalServerError), resp.Code)
	require.Equal(t, "something went wrong inside the scheduler", string(resp.Body))

	require.Equal(t, int64(1), schedulerEnqueueAttempts.Load(), "should not retry on scheduler errors")
}

func TestFrontend_Protobuf_SchedulerError(t *testing.T) {
	schedulerEnqueueAttempts := atomic.NewInt64(0)
	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		schedulerEnqueueAttempts.Inc()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "something went wrong inside the scheduler"}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.Equal(t, apierror.New(apierror.TypeInternal, "something went wrong inside the scheduler"), err)
	require.Nil(t, msg)

	require.Equal(t, int64(1), schedulerEnqueueAttempts.Load(), "should not retry on scheduler errors")
}

func TestFrontend_HTTPGRPC_EnqueueFailures(t *testing.T) {
	t.Run("scheduler is shutting down with valid query", func(t *testing.T) {
		f, _ := setupFrontend(t, nil, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
		})

		req := &httpgrpc.HTTPRequest{
			Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up",
		}
		_, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), "test"), req)
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "failed to enqueue request"))
	})
	t.Run("scheduler is running fine", func(t *testing.T) {
		f, _ := setupFrontend(t, nil, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		})

		cases := []struct {
			name, url, error string
		}{
			{
				name:  "start time is wrong",
				url:   "/api/v1/query_range?start=9466camnsd84800&end=946771200&step=60&query=up{}",
				error: `rpc error: code = Code(400) desc = failed to enqueue request: invalid parameter "start": cannot parse "9466camnsd84800" to a valid timestamp`,
			},
			{
				name:  "end time is wrong",
				url:   "/api/v1/query_range?start=946684800&end=946771200dgiu&step=60&query=up{}",
				error: `rpc error: code = Code(400) desc = failed to enqueue request: invalid parameter "end": cannot parse "946771200dgiu" to a valid timestamp`,
			},
			{
				name:  "query time is wrong",
				url:   "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{",
				error: `rpc error: code = Code(400) desc = failed to enqueue request: invalid parameter "query": 1:4: parse error: unexpected end of input inside braces`,
			},
			{
				name:  "no query provided",
				url:   "/api/v1/query_range?start=946684800&end=946771200&step=60",
				error: `rpc error: code = Code(400) desc = failed to enqueue request: invalid parameter "query": unknown position: parse error: no expression found in input`,
			},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				req := &httpgrpc.HTTPRequest{
					Url: c.url,
				}
				_, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), "test"), req)
				require.Error(t, err)
				require.Equal(t, c.error, err.Error())
			})
		}
	})
}

func TestFrontendCancellation(t *testing.T) {
	testCases := map[string]func(ctx context.Context, t *testing.T, f *Frontend){
		"HTTP-over-gRPC": func(ctx context.Context, t *testing.T, f *Frontend) {
			req := &httpgrpc.HTTPRequest{
				Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
			}
			resp, _, err := f.RoundTripGRPC(ctx, req)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, resp)
		},
		"Protobuf": func(ctx context.Context, t *testing.T, f *Frontend) {
			req := &querierpb.EvaluateQueryRequest{}
			resp, err := f.DoProtobufRequest(ctx, req)
			require.NoError(t, err)
			t.Cleanup(resp.Close)

			msg, err := resp.Next(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, msg)
		},
	}

	for name, makeRequest := range testCases {
		t.Run(name, func(t *testing.T) {
			f, ms := setupFrontend(t, nil, nil)

			ctx, cancel := context.WithTimeout(user.InjectOrgID(context.Background(), "test"), 200*time.Millisecond)
			defer cancel()

			makeRequest(ctx, t, f)

			// We wait a bit to make sure scheduler receives the cancellation request.
			test.Poll(t, time.Second, 2, func() interface{} {
				ms.mu.Lock()
				defer ms.mu.Unlock()

				return len(ms.msgs)
			})

			ms.checkWithLock(func() {
				require.Len(t, ms.msgs, 2)
				require.Equal(t, schedulerpb.ENQUEUE, ms.msgs[0].Type)
				require.Equal(t, schedulerpb.CANCEL, ms.msgs[1].Type)
				require.Equal(t, ms.msgs[0].QueryID, ms.msgs[1].QueryID)
			})
		})
	}

}

// When frontendWorker that processed the request is busy (processing a new request or cancelling a previous one)
// we still need to make sure that the cancellation reach the scheduler at some point.
// Issue: https://github.com/grafana/mimir/issues/740
func TestFrontendWorkerCancellation(t *testing.T) {
	testCases := map[string]func(ctx context.Context, t *testing.T, f *Frontend){
		"HTTP-over-gRPC": func(ctx context.Context, t *testing.T, f *Frontend) {
			req := &httpgrpc.HTTPRequest{
				Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
			}
			resp, _, err := f.RoundTripGRPC(ctx, req)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, resp)
		},
		"Protobuf": func(ctx context.Context, t *testing.T, f *Frontend) {
			req := &querierpb.EvaluateQueryRequest{}
			resp, err := f.DoProtobufRequest(ctx, req)
			require.NoError(t, err)
			t.Cleanup(resp.Close)

			msg, err := resp.Next(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, msg)
		},
	}

	for name, makeRequest := range testCases {
		t.Run(name, func(t *testing.T) {
			f, ms := setupFrontend(t, nil, nil)

			ctx, cancel := context.WithTimeout(user.InjectOrgID(context.Background(), "test"), 200*time.Millisecond)
			defer cancel()

			// send multiple requests > maxconcurrency of scheduler. So that it keeps all the frontend worker busy in serving requests.
			reqCount := testFrontendWorkerConcurrency + 5
			var wg sync.WaitGroup
			for i := 0; i < reqCount; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					makeRequest(ctx, t, f)
				}()
			}

			wg.Wait()

			// We wait a bit to make sure scheduler receives the cancellation request.
			// 2 * reqCount because for every request, should also be corresponding cancel request
			test.Poll(t, 5*time.Second, 2*reqCount, func() interface{} {
				ms.mu.Lock()
				defer ms.mu.Unlock()

				return len(ms.msgs)
			})

			ms.checkWithLock(func() {
				require.Len(t, ms.msgs, 2*reqCount)
				msgTypeCounts := map[schedulerpb.FrontendToSchedulerType]int{}
				for _, msg := range ms.msgs {
					msgTypeCounts[msg.Type]++
				}
				expectedMsgTypeCounts := map[schedulerpb.FrontendToSchedulerType]int{
					schedulerpb.ENQUEUE: reqCount,
					schedulerpb.CANCEL:  reqCount,
				}
				require.Equalf(t, expectedMsgTypeCounts, msgTypeCounts,
					"Should receive %d enqueue (%d) requests, and %d cancel (%d) requests.", reqCount, schedulerpb.ENQUEUE, reqCount, schedulerpb.CANCEL,
				)
			})
		})
	}
}

func TestFrontendFailedCancellation(t *testing.T) {
	testCases := map[string]func(ctx context.Context, t *testing.T, f *Frontend){
		"HTTP-over-gRPC": func(ctx context.Context, t *testing.T, f *Frontend) {
			req := &httpgrpc.HTTPRequest{
				Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
			}
			resp, _, err := f.RoundTripGRPC(ctx, req)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, resp)
		},
		"Protobuf": func(ctx context.Context, t *testing.T, f *Frontend) {
			req := &querierpb.EvaluateQueryRequest{}
			resp, err := f.DoProtobufRequest(ctx, req)
			require.NoError(t, err)
			t.Cleanup(resp.Close)

			msg, err := resp.Next(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, msg)
		},
	}

	for name, makeRequest := range testCases {
		t.Run(name, func(t *testing.T) {
			f, ms := setupFrontend(t, nil, nil)

			ctx, cancel := context.WithCancel(user.InjectOrgID(context.Background(), "test"))
			defer cancel()

			go func() {
				time.Sleep(100 * time.Millisecond)

				// stop scheduler workers
				addr := ""
				f.schedulerWorkers.mu.Lock()
				for k := range f.schedulerWorkers.workers {
					addr = k
					break
				}
				f.schedulerWorkers.mu.Unlock()

				f.schedulerWorkers.InstanceRemoved(servicediscovery.Instance{Address: addr, InUse: true})

				// Wait for worker goroutines to stop.
				time.Sleep(100 * time.Millisecond)

				// Cancel request. Frontend will try to send cancellation to scheduler, but that will fail (not visible to user).
				// Everything else should still work fine.
				cancel()
			}()

			makeRequest(ctx, t, f)

			ms.checkWithLock(func() {
				require.Equal(t, 1, len(ms.msgs))
			})
		})
	}
}

func TestFrontend_Protobuf_ReadingResponseWithCanceledContext(t *testing.T) {
	signal := make(chan struct{})

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		<-signal // Don't respond until the test has attempted to read from the stream.
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	cancelledContext, cancel := context.WithCancel(ctx)
	cancel()

	start := time.Now()
	msg, err := resp.Next(cancelledContext)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, msg)
	require.Less(t, time.Since(start), time.Second, "calling Next() with a cancelled context should not block")

	close(signal)
}

func TestFrontend_Protobuf_ReadingCancelledRequest(t *testing.T) {
	ctx, cancel := context.WithCancelCause(user.InjectOrgID(context.Background(), "test"))
	cancellationError := cancellation.NewErrorf("the request has been canceled")

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		cancel(cancellationError)
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req)
	require.NoError(t, err)
	defer resp.Close()

	uncancelledContext := context.Background()
	msg, err := resp.Next(uncancelledContext)
	require.ErrorIs(t, err, cancellationError)
	require.Nil(t, msg)
}

func TestFrontendStreamingResponse(t *testing.T) {
	const (
		userID = "test"
	)

	for _, tt := range []struct {
		name                string
		sendResultStream    func(f *Frontend, msg *schedulerpb.FrontendToScheduler) error
		expectStreamError   bool
		expectContentLength int
		expectBody          string
	}{
		{
			name: "metadata and two chunks",
			sendResultStream: func(f *Frontend, msg *schedulerpb.FrontendToScheduler) error {
				body := "result stream body"
				headers := []*httpgrpc.Header{{Key: "Content-Length", Values: []string{strconv.Itoa(len(body))}}}
				resp := &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte(body), Headers: headers}
				s := &mockQueryResultStreamServer{ctx: user.InjectOrgID(context.Background(), userID)}
				s.msgs = append(s.msgs,
					metadataRequest(msg, int(resp.Code), resp.Headers),
					bodyChunkRequest(msg, resp.Body[:len(resp.Body)/2]),
					bodyChunkRequest(msg, resp.Body[len(resp.Body)/2:]),
				)
				return f.QueryResultStream(s)
			},
			expectBody:          "result stream body",
			expectContentLength: 18,
		},
		{
			name: "received metadata only, no body data sent",
			sendResultStream: func(f *Frontend, msg *schedulerpb.FrontendToScheduler) error {
				s := &mockQueryResultStreamServer{ctx: user.InjectOrgID(context.Background(), userID)}
				s.msgs = append(s.msgs,
					metadataRequest(msg, http.StatusOK, []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"0"}}}),
				)
				return f.QueryResultStream(s)
			},
			expectBody: "",
		},
		{
			name: "metadata and empty body chunks",
			sendResultStream: func(f *Frontend, msg *schedulerpb.FrontendToScheduler) error {
				s := &mockQueryResultStreamServer{ctx: user.InjectOrgID(context.Background(), userID)}
				s.msgs = append(s.msgs,
					metadataRequest(msg, http.StatusOK, []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"0"}}}),
					bodyChunkRequest(msg, nil),
					bodyChunkRequest(msg, nil),
				)
				return f.QueryResultStream(s)
			},
			expectBody: "",
		},
		{
			name: "errors on wrong message sequence",
			sendResultStream: func(f *Frontend, msg *schedulerpb.FrontendToScheduler) error {
				s := &mockQueryResultStreamServer{ctx: user.InjectOrgID(context.Background(), userID)}
				s.msgs = append(s.msgs,
					metadataRequest(msg, http.StatusOK, []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"16"}}}),
					bodyChunkRequest(msg, []byte("part 1/2")),
					metadataRequest(msg, http.StatusOK, nil),
					bodyChunkRequest(msg, []byte("part 2/2")),
				)
				return f.QueryResultStream(s)
			},
			expectStreamError:   true,
			expectBody:          "part 1/2",
			expectContentLength: 16,
		},
		{
			name: "context cancelled while streaming response",
			sendResultStream: func(f *Frontend, msg *schedulerpb.FrontendToScheduler) error {
				ctx, cancelCause := context.WithCancelCause(user.InjectOrgID(context.Background(), userID))
				recvCalled := make(chan chan struct{})
				cancelAfterCalls := 2
				s := &mockQueryResultStreamServer{ctx: ctx, recvCalled: recvCalled}
				go func() {
					recvCount := 0
					for called := range recvCalled {
						recvCount++
						if recvCount == cancelAfterCalls {
							cancelCause(fmt.Errorf("streaming cancelled"))
						}
						called <- struct{}{}
					}
				}()
				s.msgs = append(s.msgs,
					metadataRequest(msg, http.StatusOK, []*httpgrpc.Header{{Key: "Content-Length", Values: []string{"24"}}}),
					bodyChunkRequest(msg, []byte("part 1/3")),
					bodyChunkRequest(msg, []byte("part 2/3")),
					bodyChunkRequest(msg, []byte("part 3/3")),
				)
				return f.QueryResultStream(s)
			},
			expectStreamError:   true,
			expectBody:          "part 1/3",
			expectContentLength: 24,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				go func() {
					err := tt.sendResultStream(f, msg)
					if tt.expectStreamError {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				}()
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			})

			req := httptest.NewRequest("GET", "/api/v1/cardinality/active_series?selector=metric", nil)
			rt := httpgrpcutil.AdaptGrpcRoundTripperToHTTPRoundTripper(f)

			resp, err := rt.RoundTrip(req.WithContext(user.InjectOrgID(context.Background(), userID)))
			require.NoError(t, err)
			defer func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			if tt.expectStreamError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectBody, string(body))

			contentLength, err := strconv.Atoi(resp.Header.Get("Content-Length"))
			require.NoError(t, err)
			require.Equal(t, tt.expectContentLength, contentLength)

			require.NoError(t, resp.Body.Close())
		})
	}
}

func metadataRequest(msg *schedulerpb.FrontendToScheduler, statusCode int, headers []*httpgrpc.Header) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		QueryID: msg.QueryID,
		Data: &frontendv2pb.QueryResultStreamRequest_Metadata{Metadata: &frontendv2pb.QueryResultMetadata{
			Code:    int32(statusCode),
			Headers: headers,
			Stats:   &stats.SafeStats{},
		}},
	}
}

func bodyChunkRequest(msg *schedulerpb.FrontendToScheduler, content []byte) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		QueryID: msg.QueryID,
		Data:    &frontendv2pb.QueryResultStreamRequest_Body{Body: &frontendv2pb.QueryResultBody{Chunk: content}},
	}
}

type mockQueryResultStreamServer struct {
	ctx        context.Context
	msgs       []*frontendv2pb.QueryResultStreamRequest
	next       int
	recvCalled chan chan struct{}

	grpc.ServerStream
}

func (s *mockQueryResultStreamServer) Context() context.Context {
	return s.ctx
}

func (s *mockQueryResultStreamServer) SendAndClose(_ *frontendv2pb.QueryResultResponse) error {
	return s.ctx.Err()
}

func (s *mockQueryResultStreamServer) Recv() (*frontendv2pb.QueryResultStreamRequest, error) {
	if err := s.ctx.Err(); err != nil {
		if s.recvCalled != nil {
			close(s.recvCalled)
		}
		return nil, err
	}
	if s.recvCalled != nil {
		called := make(chan struct{}, 1)
		s.recvCalled <- called
		<-called
	}
	if s.next >= len(s.msgs) {
		return nil, io.EOF
	}
	defer func() { s.next++ }()
	return s.msgs[s.next], nil
}

type mockScheduler struct {
	t *testing.T
	f *Frontend

	replyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend

	mu           sync.Mutex
	frontendAddr map[string]int
	msgs         []*schedulerpb.FrontendToScheduler
}

func newMockScheduler(t *testing.T, f *Frontend, replyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend) *mockScheduler {
	return &mockScheduler{t: t, f: f, frontendAddr: map[string]int{}, replyFunc: replyFunc}
}

func (m *mockScheduler) checkWithLock(fn func()) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fn()
}

func (m *mockScheduler) FrontendLoop(frontend schedulerpb.SchedulerForFrontend_FrontendLoopServer) error {
	init, err := frontend.Recv()
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.frontendAddr[init.FrontendAddress]++
	m.mu.Unlock()

	// Ack INIT from frontend.
	if err := frontend.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}); err != nil {
		return err
	}

	for {
		msg, err := frontend.Recv()
		if err != nil {
			return err
		}

		m.mu.Lock()
		m.msgs = append(m.msgs, msg)
		m.mu.Unlock()

		reply := &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		if m.replyFunc != nil {
			reply = m.replyFunc(m.f, msg)
		}

		if err := frontend.Send(reply); err != nil {
			return err
		}
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup       func(cfg *Config)
		expectedErr string
	}{
		"should pass with default config": {
			setup: func(*Config) {},
		},
		"should pass if scheduler address is configured, and query-scheduler discovery mode is the default one": {
			setup: func(cfg *Config) {
				cfg.SchedulerAddress = "localhost:9095"
			},
		},
		"should fail if query-scheduler service discovery is set to ring, and scheduler address is configured": {
			setup: func(cfg *Config) {
				cfg.QuerySchedulerDiscovery.Mode = schedulerdiscovery.ModeRing
				cfg.SchedulerAddress = "localhost:9095"
			},
			expectedErr: `scheduler address cannot be specified when query-scheduler service discovery mode is set to 'ring'`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			actualErr := cfg.Validate()
			if testData.expectedErr == "" {
				require.NoError(t, actualErr)
			} else {
				require.Error(t, actualErr)
				assert.ErrorContains(t, actualErr, testData.expectedErr)
			}
		})
	}
}

func TestWithClosingGrpcServer(t *testing.T) {
	// This test is easier with single frontend worker.
	const frontendConcurrency = 1
	const userID = "test"

	f, _ := setupFrontendWithConcurrencyAndServerOptions(t, nil, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
	}, frontendConcurrency, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     100 * time.Millisecond,
		MaxConnectionAge:      100 * time.Millisecond,
		MaxConnectionAgeGrace: 100 * time.Millisecond,
		Time:                  1 * time.Second,
		Timeout:               1 * time.Second,
	}))

	// Connection will be established on the first roundtrip.
	req := &httpgrpc.HTTPRequest{
		Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
	}
	resp, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)
	require.Equal(t, int(resp.Code), http.StatusTooManyRequests)

	// Verify that there is one stream open.
	require.Equal(t, 1, checkStreamGoroutines())

	// Wait a bit, to make sure that server closes connection.
	time.Sleep(1 * time.Second)

	// Despite server closing connections, stream-related goroutines still exist.
	require.Equal(t, 1, checkStreamGoroutines())

	// Another request will work as before, because worker will recreate connection.
	resp, _, err = f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)
	require.Equal(t, int(resp.Code), http.StatusTooManyRequests)

	// There should still be only one stream open, and one goroutine created for it.
	// Previously frontend leaked goroutine because stream that received "EOF" due to server closing the connection, never stopped its goroutine.
	require.Equal(t, 1, checkStreamGoroutines())
}

func checkStreamGoroutines() int {
	const streamGoroutineStackFrameTrailer = "created by google.golang.org/grpc.newClientStreamWithParams"

	buf := make([]byte, 1000000)
	stacklen := runtime.Stack(buf, true)

	goroutineStacks := string(buf[:stacklen])
	return strings.Count(goroutineStacks, streamGoroutineStackFrameTrailer)
}

func makeLabels(namesAndValues ...string) []*dto.LabelPair {
	out := []*dto.LabelPair(nil)

	for i := 0; i+1 < len(namesAndValues); i = i + 2 {
		out = append(out, &dto.LabelPair{
			Name:  proto.String(namesAndValues[i]),
			Value: proto.String(namesAndValues[i+1]),
		})
	}

	return out
}

type limits struct {
	queryIngestersWithin time.Duration
}

func (l limits) QueryIngestersWithin(string) time.Duration {
	return l.queryIngestersWithin
}

func newStringMessage(s string) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_EvaluateQueryResponse{
			EvaluateQueryResponse: &querierpb.EvaluateQueryResponse{
				Message: &querierpb.EvaluateQueryResponse_StringValue{
					StringValue: &querierpb.EvaluateQueryResponseStringValue{
						Value: s,
					},
				},
			},
		},
	}
}

func newErrorMessage(typ mimirpb.QueryErrorType, msg string) *frontendv2pb.QueryResultStreamRequest {
	return &frontendv2pb.QueryResultStreamRequest{
		Data: &frontendv2pb.QueryResultStreamRequest_Error{
			Error: &querierpb.Error{
				Type:    typ,
				Message: msg,
			},
		},
	}
}
