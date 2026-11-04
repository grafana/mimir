// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/v2/frontend_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package v2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/servicediscovery"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/scheduler/schedulerdiscovery"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/rootqueryid"
	utiltest "github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	utiltest.VerifyNoLeakTestMain(m)
}

const testFrontendWorkerConcurrency = 5

func setupFrontend(t testing.TB, reg prometheus.Registerer, schedulerReplyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend) (*Frontend, *mockScheduler) {
	return setupFrontendWithConcurrencyAndServerOptions(t, reg, schedulerReplyFunc, testFrontendWorkerConcurrency, log.NewLogfmtLogger(os.Stdout), nil)
}

// setupFrontendWithConfig is setupFrontend with a chance to change the config before the
// frontend is built.
func setupFrontendWithConfig(t testing.TB, reg prometheus.Registerer, schedulerReplyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend, mutateCfg func(*Config)) (*Frontend, *mockScheduler) {
	return setupFrontendWithConcurrencyAndServerOptions(t, reg, schedulerReplyFunc, testFrontendWorkerConcurrency, log.NewLogfmtLogger(os.Stdout), mutateCfg)
}

// mutateCfg may be nil, in which case the default config is used.
func setupFrontendWithConcurrencyAndServerOptions(t testing.TB, reg prometheus.Registerer, schedulerReplyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend, concurrency int, logger log.Logger, mutateCfg func(*Config), opts ...grpc.ServerOption) (*Frontend, *mockScheduler) {
	// We need to use different ports as the scheduler does not need the user header interceptor,
	// whereas the frontend does.
	frontendListener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	schedulerListener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	schedulerOpts := opts
	frontendOpts := append(
		slices.Clone(opts),
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
	)

	schedulerServer := grpc.NewServer(schedulerOpts...)
	frontendServer := grpc.NewServer(frontendOpts...)

	frontendHost, frontendPort, err := net.SplitHostPort(frontendListener.Addr().String())
	require.NoError(t, err)

	grpcPort, err := strconv.Atoi(frontendPort)
	require.NoError(t, err)

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.SchedulerAddress = schedulerListener.Addr().String()
	cfg.WorkerConcurrency = concurrency
	cfg.Addr = frontendHost
	cfg.Port = grpcPort
	cfg.QueryStoreAfter = 12 * time.Hour
	if mutateCfg != nil {
		mutateCfg(&cfg)
	}

	codec := newTestCodec()

	f, err := NewFrontend(cfg, limits{queryIngestersWithin: 13 * time.Hour}, logger, reg, codec)
	require.NoError(t, err)

	frontendv2pb.RegisterFrontendForQuerierServer(frontendServer, f)

	ms := newMockScheduler(t, f, schedulerReplyFunc)
	ms.server = schedulerServer
	schedulerpb.RegisterSchedulerForFrontendServer(schedulerServer, ms)

	go func() {
		_ = frontendServer.Serve(frontendListener)
	}()

	go func() {
		_ = schedulerServer.Serve(schedulerListener)
	}()

	t.Cleanup(func() {
		_ = frontendListener.Close()
		frontendServer.GracefulStop()
	})

	t.Cleanup(func() {
		_ = schedulerListener.Close()
		schedulerServer.GracefulStop()
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

func sendResponseWithDelay(f *Frontend, delay time.Duration, userID string, queryID uint64, resp *httpgrpc.HTTPResponse) error {
	if delay > 0 {
		time.Sleep(delay)
	}

	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := f.QueryResult(ctx, &frontendv2pb.QueryResultRequest{
		QueryID:      queryID,
		HttpResponse: resp,
		Stats:        &stats.SafeStats{},
	})
	return err
}

func sendStreamingResponse(t testing.TB, f *Frontend, userID string, queryID uint64, resp ...*frontendv2pb.QueryResultStreamRequest) {
	require.NoError(t, sendStreamingResponseWithErrorCapture(f, userID, queryID, nil, resp...))
}

func sendStreamingResponseWithErrorCapture(f *Frontend, userID string, queryID uint64, beforeLastMessageSent func(), resp ...*frontendv2pb.QueryResultStreamRequest) error {
	for _, m := range resp {
		m.QueryID = queryID
	}

	ctx := user.InjectOrgID(context.Background(), userID)

	conn, err := grpc.NewClient(
		net.JoinHostPort(f.cfg.Addr, strconv.Itoa(f.cfg.Port)),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(middleware.ClientUserHeaderInterceptor),
		grpc.WithChainStreamInterceptor(middleware.StreamClientUserHeaderInterceptor),
	)

	if err != nil {
		return err
	}
	defer conn.Close()

	client := frontendv2pb.NewFrontendForQuerierClient(conn)
	stream, err := client.QueryResultStream(ctx)
	if err != nil {
		return err
	}

	for idx, m := range resp {
		if idx == len(resp)-1 && beforeLastMessageSent != nil {
			beforeLastMessageSent()
		}

		if err := stream.Send(m); err != nil {
			if errors.Is(err, io.EOF) {
				// If Send returns EOF, then we need to call RecvMsg to get that error.
				// See the docs for grpc.ClientStream for more details.
				receivedErr := stream.RecvMsg(&frontendv2pb.QueryResultResponse{})
				if receivedErr == nil {
					return fmt.Errorf("writing message to stream failed with EOF, and RecvMsg returned no error")
				} else {
					return fmt.Errorf("writing message to stream failed due to error not generated by client: %w", receivedErr)
				}
			}

			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

func sendStreamingResponseFromEncodedMessages(t testing.TB, f *Frontend, userID string, queryID uint64, resp ...[]byte) {
	ctx := user.InjectOrgID(context.Background(), userID)
	stream := &mockUnmarshallingQueryResultStreamServer{
		queryID: queryID,
		ctx:     ctx,
		msgs:    resp,
	}

	if err := f.QueryResultStream(stream); err != nil {
		t.Errorf("QueryResultStream returned %v", err)
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
		go func() {
			_ = sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
				Code: 200,
				Body: []byte(body),
			})
		}()

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

func TestFrontend_HTTPGRPC_QuerierCancelled(t *testing.T) {
	// A query-scheduler cancels the dispatched requests of a query-frontend when the last stream from that query-frontend ends.
	// The querier then replies 499, although the caller did not cancel.
	const (
		userID               = "test"
		querierFetchedSeries = 7
	)

	testCases := map[string]struct {
		querierCode  int32
		querierBody  string
		expectedCode int32
		expectErr    bool
		// The query-frontend merges the querier stats only for a response that is not a server error.
		expectedFetchedSeries uint64
	}{
		"querier cancelled, caller did not cancel": {
			querierCode:           499,
			querierBody:           `{"status":"error","errorType":"canceled","error":"context canceled: query cancelled"}`,
			expectedCode:          http.StatusInternalServerError,
			expectErr:             true,
			expectedFetchedSeries: 0,
		},
		"other querier error": {
			querierCode:           http.StatusUnprocessableEntity,
			querierBody:           `{"status":"error","errorType":"execution","error":"something went wrong"}`,
			expectedCode:          http.StatusUnprocessableEntity,
			expectedFetchedSeries: querierFetchedSeries,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			sendErr := make(chan error, 1)
			f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				// The query-frontend registers the request before it enqueues it, so the querier can reply here.
				querierStats := &stats.SafeStats{}
				querierStats.AddFetchedSeries(querierFetchedSeries)
				_, err := f.QueryResult(user.InjectOrgID(t.Context(), userID), &frontendv2pb.QueryResultRequest{
					QueryID:      msg.QueryID,
					HttpResponse: &httpgrpc.HTTPResponse{Code: tc.querierCode, Body: []byte(tc.querierBody)},
					Stats:        querierStats,
				})
				sendErr <- err

				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			})

			reqStats, ctx := stats.ContextWithEmptyStats(user.InjectOrgID(t.Context(), userID))
			req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
			resp, body, err := f.RoundTripGRPC(ctx, req)

			// The callback ran before the query-scheduler replied to the enqueue, so its result is here.
			select {
			case sendErr := <-sendErr:
				require.NoError(t, sendErr)
			default:
				require.FailNow(t, "the query-scheduler did not get the request")
			}

			if tc.expectErr {
				// The retry middleware retries a 5xx for range and instant queries.
				errResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "expected an httpgrpc error, got response %v and error %v", resp, err)
				require.Nil(t, resp)
				require.Nil(t, body)
				require.Equal(t, tc.expectedCode, errResp.Code)
				require.Equal(t, "query cancelled in the querier, although the caller did not cancel it: "+tc.querierBody, string(errResp.Body))
			} else {
				require.NoError(t, err)
				require.NoError(t, body.Close())
				require.Equal(t, tc.expectedCode, resp.Code)
			}

			require.Equal(t, tc.expectedFetchedSeries, reqStats.LoadFetchedSeries())
			// The request must not keep an in-flight slot after it returns.
			require.Equal(t, 0.0, testutil.ToFloat64(f.inflightRequestCount))
		})
	}
}

// A caller can cancel while the querier's 499 arrives.
// Both cases of the select in RoundTripGRPC are then ready, and Go picks one at random.
// In both cases the caller's cancellation must win, and the query-frontend must not return a 500.
func TestFrontend_HTTPGRPC_QuerierCancelledWhileCallerCancels(t *testing.T) {
	const (
		userID      = "test"
		querierBody = `{"status":"error","errorType":"canceled","error":"context canceled: query cancelled"}`
		// Each run takes one of the two cases at random, so the runs take both.
		runs = 20
	)

	var (
		cancelCallerMx sync.Mutex
		cancelCaller   context.CancelFunc
		sendErr        = make(chan error, 1)
	)

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			// The query-frontend sends CANCEL when it takes the caller's cancellation.
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		}

		// The query-frontend registers the request before it enqueues it, so the querier can reply here.
		_, err := f.QueryResult(user.InjectOrgID(t.Context(), userID), &frontendv2pb.QueryResultRequest{
			QueryID:      msg.QueryID,
			HttpResponse: &httpgrpc.HTTPResponse{Code: statusClientClosedRequest, Body: []byte(querierBody)},
			Stats:        &stats.SafeStats{},
		})
		sendErr <- err

		// The caller cancels before the enqueue returns, so both cases of the select are ready.
		cancelCallerMx.Lock()
		cancelCaller()
		cancelCallerMx.Unlock()

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
	for range runs {
		ctx, cancel := context.WithCancel(user.InjectOrgID(t.Context(), userID))
		cancelCallerMx.Lock()
		cancelCaller = cancel
		cancelCallerMx.Unlock()

		resp, body, err := f.RoundTripGRPC(ctx, req)

		// The callback ran before the query-scheduler replied to the enqueue, so its result is here.
		select {
		case sendErr := <-sendErr:
			require.NoError(t, sendErr)
		default:
			require.FailNow(t, "the query-scheduler did not get the request")
		}

		if err != nil {
			// The select took the caller's cancellation.
			require.ErrorIs(t, err, context.Canceled)
		} else {
			// The select took the querier's 499 and kept it, because the caller cancelled.
			require.Equal(t, int32(statusClientClosedRequest), resp.Code)
			require.NoError(t, body.Close())
		}
		cancel()
	}
}

// The response body from RoundTripGRPC reaches the query-frontend middleware chain as an
// http.Response.Body, and that chain closes it more than once:
// httpQueryRequestRoundTripperHandler.Do closes it, and so does readResponseBody in the
// codec it hands the response to. Every extra close used to decrement the in-flight gauge
// again, so it walked below zero and stayed there until the process restarted.
func TestFrontend_HTTPGRPC_ClosingResponseBodyMoreThanOnceDoesNotSkewInflightRequests(t *testing.T) {
	const (
		body   = "all fine here"
		userID = "test"
	)

	reg := prometheus.NewPedanticRegistry()
	f, _ := setupFrontend(t, reg, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		go func() {
			_ = sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
				Code: 200,
				Body: []byte(body),
			})
		}()

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	req := &httpgrpc.HTTPRequest{
		Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
	}
	resp, respBody, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)
	require.Equal(t, int32(200), resp.Code)

	require.NoError(t, respBody.Close())
	require.NoError(t, respBody.Close())

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_frontend_queries_in_progress Number of queries in progress handled by this frontend.
		# TYPE cortex_query_frontend_queries_in_progress gauge
		cortex_query_frontend_queries_in_progress 0
	`), "cortex_query_frontend_queries_in_progress"))
}

func TestFrontend_Protobuf_HappyPath(t *testing.T) {
	const userID = "test"

	expectedMessages := []*frontendv2pb.QueryResultStreamRequest{
		newStringMessage("first message"),
		newStringMessage("second message"),
	}

	headers := map[string][]string{"Some-Extra-Header": {"some-value", "some-other-value"}}

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		require.Equal(t, []string{ingesterQueryComponent}, msg.AdditionalQueueDimensions)
		expectedMetadata := []schedulerpb.MetadataItem{{Key: "Some-Extra-Header", Value: []string{"some-value", "some-other-value"}}}
		require.Equal(t, expectedMetadata, msg.GetProtobufRequest().Metadata)

		go sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	ctx = querymiddleware.ContextWithHeadersToPropagate(ctx, headers)
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now().Add(-5*time.Hour), time.Now())
	require.NoError(t, err)
	defer resp.Close()
	defer resp.Close() // Closing a response stream multiple times should not panic.

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	buf := msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[0], msg)

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	buf = msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[1], msg)

	// Response stream exhausted.
	msg, err = resp.Next(ctx)
	require.EqualError(t, err, "end of stream reached")
	require.Nil(t, msg)
}

// This test checks that we don't send unnecessary cancellation messages to the query-scheduler
// when a request has been completely read successfully.
//
// Previously, there was a race between calling Close() on the stream returned by DoProtobufRequest()
// and receiveResultForProtobufRequest() observing that the stream was finished.
// If Close() won the race, it would cause the cancellation monitoring goroutine started by
// DoProtobufRequest() to send a cancellation message to the scheduler, even though that was unnecessary.
//
// While this had no user-visible impact, cancelling a request causes the scheduler to close its stream
// with the querier (to signal the cancellation). This means queriers had to reestablish the stream,
// which takes time. If all querier workers were closed due to this, then scheduler would then
// reshuffle querier tenant assignments for shuffle sharding, which would reduce the effectiveness of
// shuffle sharding.
func TestFrontend_Protobuf_ShouldNotCancelRequestAfterSuccess(t *testing.T) {
	for _, exhaustStream := range []bool{true, false} {
		t.Run(fmt.Sprintf("exhaust stream=%v", exhaustStream), func(t *testing.T) {
			const userID = "test"
			cancellations := atomic.NewInt64(0)

			f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				switch msg.Type {
				case schedulerpb.ENQUEUE:
					go sendStreamingResponse(t, f, userID, msg.QueryID, newStringMessage("first message"))
				case schedulerpb.CANCEL:
					cancellations.Inc()
				default:
					return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
				}

				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			})

			for range 10000 { // Send many requests to try to trigger the race condition that previously caused this test to fail.
				ctx := user.InjectOrgID(context.Background(), userID)
				ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
				req := &querierpb.EvaluateQueryRequest{}
				resp, err := f.DoProtobufRequest(ctx, req, time.Now().Add(-5*time.Hour), time.Now())
				require.NoError(t, err)

				msg, err := resp.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, "first message", msg.GetEvaluateQueryResponse().GetStringValue().Value)

				if exhaustStream {
					// Response stream exhausted.
					msg, err = resp.Next(ctx)
					require.EqualError(t, err, "end of stream reached")
					require.Nil(t, msg)
				}

				resp.Close()
			}

			require.Zero(t, cancellations.Load(), "expected no cancellations to be sent to the scheduler, but at least one was")
		})
	}
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
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	buf := msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[0], msg)

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	buf = msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[1], msg)

	// Response stream exhausted.
	msg, err = resp.Next(ctx)
	require.EqualError(t, err, "end of stream reached")
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
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		go func() {
			err := sendStreamingResponseWithErrorCapture(f, userID, msg.QueryID, nil, expectedMessages...)
			require.EqualError(t, err, "rpc error: code = Canceled desc = context canceled: stream closed")
		}()

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	buf := msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[0], msg)
	resp.Close() // We expect all goroutines to be cleaned up after this (verified by the VerifyNoLeakTestMain call in TestMain above)
}

func TestFrontend_Protobuf_ResponseClosedBeforeResponseReceived(t *testing.T) {
	respChannel := make(chan ResponseStream)
	defer close(respChannel)

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		go func() {
			resp := <-respChannel

			// Close the stream returned by DoProtobufRequest once we're confident the goroutine in DoProtobufRequest has observed that the request has been enqueued
			// and is waiting for streamContext to be cancelled.
			// This ensures that closing the stream doesn't trigger the code path that calls writeEnqueueError().
			time.Sleep(10 * time.Millisecond)
			resp.Close()
		}()

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), "user-1")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	respChannel <- resp

	nextReturned := make(chan struct{})

	go func() {
		defer close(nextReturned)
		// Next shouldn't block forever if Close is called before the querier responds.
		msg, err := resp.Next(ctx)
		require.ErrorIs(t, err, errStreamClosed)
		require.Nil(t, msg)

		// Subsequent calls to Next should also return the same error.
		msg, err = resp.Next(ctx)
		require.ErrorIs(t, err, errStreamClosed)
		require.Nil(t, msg)
	}()

	select {
	case <-nextReturned:
		// Nothing to do.
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for Next to return")
	}
}

func TestFrontend_Protobuf_ErrorReturnedByQuerier(t *testing.T) {
	const userID = "test"

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		errorMessage := newErrorMessage(mimirpb.QUERY_ERROR_TYPE_BAD_DATA, "something went wrong")
		go sendStreamingResponse(t, f, userID, msg.QueryID, errorMessage)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.Equal(t, apierror.New(apierror.TypeBadData, "something went wrong"), err)
	require.Nil(t, msg)
}

func TestFrontend_Protobuf_QuerierCancelledWhileStreaming(t *testing.T) {
	// A query-scheduler cancels the dispatched requests of a query-frontend when the last stream from that query-frontend ends.
	// A querier that already streams its response then ends the stream with a cancellation error.
	// The caller did not cancel.
	// So the error must be retryable.
	const userID = "test"

	f, ms := setupFrontendWithConcurrencyAndServerOptions(t, prometheus.NewPedanticRegistry(), nil, 1, log.NewNopLogger(), nil)

	ctx := user.InjectOrgID(t.Context(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	resp, err := f.DoProtobufRequest(ctx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	test.Poll(t, time.Second, 1.0, func() any {
		return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
	})
	var queryID uint64
	ms.checkWithLock(func() { queryID = ms.msgs[0].QueryID })

	sendErr := make(chan error, 1)
	go func() {
		sendErr <- sendStreamingResponseWithErrorCapture(f, userID, queryID, nil, newStringMessage("first message"), newErrorMessage(mimirpb.QUERY_ERROR_TYPE_CANCELED, "context canceled: query cancelled"))
	}()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	msg.FreeBuffer()

	msg, err = resp.Next(ctx)
	// The retry middleware retries TypeInternal, but not TypeCanceled.
	require.Equal(t, apierror.New(apierror.TypeInternal, "query cancelled in the querier, although the caller did not cancel it: context canceled: query cancelled"), err)
	require.Nil(t, msg)
	require.NoError(t, <-sendErr)
}

func TestProtobufResponseStream_ErrorFromMessage(t *testing.T) {
	cancelledCtx, cancel := context.WithCancel(t.Context())
	cancel()

	testCases := map[string]struct {
		requestContext context.Context
		errorType      mimirpb.QueryErrorType
		expectedErr    error
	}{
		"querier cancelled, caller did not cancel": {
			requestContext: t.Context(),
			errorType:      mimirpb.QUERY_ERROR_TYPE_CANCELED,
			expectedErr:    apierror.New(apierror.TypeInternal, "query cancelled in the querier, although the caller did not cancel it: something went wrong"),
		},
		"querier cancelled, caller cancelled": {
			requestContext: cancelledCtx,
			errorType:      mimirpb.QUERY_ERROR_TYPE_CANCELED,
			expectedErr:    apierror.New(apierror.TypeCanceled, "something went wrong"),
		},
		"other error": {
			requestContext: t.Context(),
			errorType:      mimirpb.QUERY_ERROR_TYPE_EXECUTION,
			expectedErr:    apierror.New(apierror.TypeExec, "something went wrong"),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			s := &ProtobufResponseStream{requestContext: tc.requestContext}
			err := s.errorFromMessage(newErrorMessage(tc.errorType, "something went wrong"))
			require.Equal(t, tc.expectedErr, err)
		})
	}
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
				go func() {
					_ = sendResponseWithDelay(f, 100*time.Millisecond, userID, queryID, &httpgrpc.HTTPResponse{
						Code: 200,
						Body: []byte(body),
					})
				}()
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
				ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
				req := &querierpb.EvaluateQueryRequest{}
				resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
				require.NoError(t, err)
				t.Cleanup(resp.Close)
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewRegistry()

			f, _ := setupFrontend(t, reg, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				if msg.Type != schedulerpb.ENQUEUE {
					return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
				}

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

		go func() {
			_ = sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
				Code: 200,
				Body: []byte(body),
			})
		}()

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
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		fail := failures.Dec()
		if fail >= 0 {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
		}

		go sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	buf := msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[0], msg)
}

func TestFrontend_Protobuf_EnqueueRetriesExhausted(t *testing.T) {
	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
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
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		go sendStreamingResponse(t, f, userID, msg.QueryID, expectedMessages...)

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	r, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	defer r.Close()
	resp := r.(*ProtobufResponseStream)

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	buf := msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[0], msg)

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	buf = msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[1], msg)

	// Wait until the last message has been buffered into the stream channel and the stream's context has been cancelled by DoProtobufRequest.
	select {
	case <-resp.streamContext.Done():
		// Context cancelled, continue.
	case <-time.After(time.Second):
		require.Fail(t, "gave up waiting for stream context to be cancelled")
	}

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	buf = msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, expectedMessages[2], msg, "should still be able to read last message after stream has been completely read")

	msg, err = resp.Next(ctx)
	require.EqualError(t, err, "end of stream reached")
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
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		schedulerEnqueueAttempts.Inc()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
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
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		schedulerEnqueueAttempts.Inc()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "something went wrong inside the scheduler"}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
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
				error: `invalid parameter "start": cannot parse "9466camnsd84800" to a valid timestamp`,
			},
			{
				name:  "end time is wrong",
				url:   "/api/v1/query_range?start=946684800&end=946771200dgiu&step=60&query=up{}",
				error: `invalid parameter "end": cannot parse "946771200dgiu" to a valid timestamp`,
			},
			{
				name:  "query time is wrong",
				url:   "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{",
				error: `invalid parameter "query": 1:4: parse error: unexpected end of input inside braces`,
			},
			{
				name:  "no query provided",
				url:   "/api/v1/query_range?start=946684800&end=946771200&step=60",
				error: `invalid parameter "query": unknown position: parse error: no expression found in input`,
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

func TestFrontend_EnqueueRequestsMetric(t *testing.T) {
	const userID = "test"

	testCases := map[string]struct {
		reply           func(f *Frontend, msg *schedulerpb.FrontendToScheduler, attempt int64) *schedulerpb.SchedulerToFrontend
		expectedCode    int32
		expectedOutcome string
	}{
		"enqueued": {
			reply: func(f *Frontend, msg *schedulerpb.FrontendToScheduler, _ int64) *schedulerpb.SchedulerToFrontend {
				go func() {
					_ = sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{Code: http.StatusOK})
				}()
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			},
			expectedCode:    http.StatusOK,
			expectedOutcome: "enqueued",
		},
		"enqueued after failed attempts": {
			reply: func(f *Frontend, msg *schedulerpb.FrontendToScheduler, attempt int64) *schedulerpb.SchedulerToFrontend {
				if attempt < testFrontendWorkerConcurrency {
					return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
				}
				go func() {
					_ = sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{Code: http.StatusOK})
				}()
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			},
			expectedCode:    http.StatusOK,
			expectedOutcome: "enqueued",
		},
		"too many requests": {
			reply: func(*Frontend, *schedulerpb.FrontendToScheduler, int64) *schedulerpb.SchedulerToFrontend {
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
			},
			expectedCode:    http.StatusTooManyRequests,
			expectedOutcome: "too_many_requests",
		},
		"scheduler error": {
			reply: func(*Frontend, *schedulerpb.FrontendToScheduler, int64) *schedulerpb.SchedulerToFrontend {
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "something went wrong inside the scheduler"}
			},
			expectedCode:    http.StatusInternalServerError,
			expectedOutcome: "scheduler_error",
		},
		"retries exhausted": {
			reply: func(*Frontend, *schedulerpb.FrontendToScheduler, int64) *schedulerpb.SchedulerToFrontend {
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
			},
			expectedCode:    http.StatusInternalServerError,
			expectedOutcome: "retries_exhausted",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			attempts := atomic.NewInt64(0)
			f, _ := setupFrontend(t, reg, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				return tc.reply(f, msg, attempts.Inc())
			})

			req := &httpgrpc.HTTPRequest{
				Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
			}
			resp, body, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
			if err != nil {
				errResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "unexpected error: %v", err)
				resp = errResp
			} else {
				require.NoError(t, body.Close())
			}
			require.Equal(t, tc.expectedCode, resp.Code)

			verifyEnqueueRequestsMetric(t, reg, map[string]int{tc.expectedOutcome: 1})
		})
	}

	t.Run("Protobuf request", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		f, _ := setupFrontend(t, reg, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
		})

		ctx := user.InjectOrgID(context.Background(), userID)
		ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
		resp, err := f.DoProtobufRequest(ctx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
		require.NoError(t, err)
		defer resp.Close()

		msg, err := resp.Next(ctx)
		require.Equal(t, apierror.New(apierror.TypeInternal, "failed to enqueue request"), err)
		require.Nil(t, msg)

		verifyEnqueueRequestsMetric(t, reg, map[string]int{"retries_exhausted": 1})
	})

	t.Run("cancelled while no worker takes the request", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		release := make(chan struct{})
		// With one worker, the first request holds the only worker until release is closed.
		f, ms := setupFrontendWithConcurrencyAndServerOptions(t, reg, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
			<-release
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
		}, 1, log.NewNopLogger(), nil)

		req := &httpgrpc.HTTPRequest{
			Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
		}

		firstDone := make(chan *httpgrpc.HTTPResponse, 1)
		firstErr := make(chan error, 1)
		go func() {
			resp, body, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
			if err == nil {
				err = body.Close()
			}
			firstDone <- resp
			firstErr <- err
		}()

		test.Poll(t, time.Second, 1, func() interface{} {
			var n int
			ms.checkWithLock(func() { n = len(ms.msgs) })
			return n
		})

		ctx, cancel := context.WithTimeout(user.InjectOrgID(context.Background(), userID), 100*time.Millisecond)
		defer cancel()
		_, _, err := f.RoundTripGRPC(ctx, req)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		close(release)
		require.NoError(t, <-firstErr)
		require.Equal(t, int32(http.StatusTooManyRequests), (<-firstDone).Code)

		verifyEnqueueRequestsMetric(t, reg, map[string]int{"cancelled": 1, "too_many_requests": 1})
	})
}

// verifyEnqueueRequestsMetric expects the given count for each enqueue outcome, and 0 for the others.
func verifyEnqueueRequestsMetric(t *testing.T, reg prometheus.Gatherer, counts map[string]int) {
	expected := map[string]int{"cancelled": 0, "enqueued": 0, "invalid_request": 0, "retries_exhausted": 0, "scheduler_error": 0, "too_many_requests": 0}
	maps.Copy(expected, counts)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
		# TYPE cortex_query_frontend_enqueue_requests_total counter
		cortex_query_frontend_enqueue_requests_total{outcome="cancelled"} %d
		cortex_query_frontend_enqueue_requests_total{outcome="enqueued"} %d
		cortex_query_frontend_enqueue_requests_total{outcome="invalid_request"} %d
		cortex_query_frontend_enqueue_requests_total{outcome="retries_exhausted"} %d
		cortex_query_frontend_enqueue_requests_total{outcome="scheduler_error"} %d
		cortex_query_frontend_enqueue_requests_total{outcome="too_many_requests"} %d
	`, expected["cancelled"], expected["enqueued"], expected["invalid_request"], expected["retries_exhausted"], expected["scheduler_error"], expected["too_many_requests"])), "cortex_query_frontend_enqueue_requests_total"))
}

// When the last stream from a query-frontend ends, the query-scheduler drops the queued requests of that query-frontend with no reply.
// The query-frontend must then fail each request that waits on that query-scheduler with a retryable error.
func TestFrontend_SchedulerLost(t *testing.T) {
	const userID = "test"

	// Each trigger returns a function that ends what the trigger started.
	triggers := map[string]func(t *testing.T, f *Frontend, ms *mockScheduler, shutDownNextStream *atomic.Bool) func(){
		"query-scheduler removed by service discovery": func(_ *testing.T, f *Frontend, _ *mockScheduler, _ *atomic.Bool) func() {
			f.schedulerWorkers.InstanceRemoved(servicediscovery.Instance{Address: f.cfg.SchedulerAddress, InUse: true})
			return func() {}
		},
		"query-scheduler crashes while its stream is idle": func(_ *testing.T, _ *Frontend, ms *mockScheduler, _ *atomic.Bool) func() {
			// A crashed query-scheduler sends no SHUTTING_DOWN.
			// The query-frontend has no message to send on the idle stream.
			ms.server.Stop()
			return func() {}
		},
		"last stream ends with SHUTTING_DOWN": func(t *testing.T, f *Frontend, _ *mockScheduler, shutDownNextStream *atomic.Bool) func() {
			// A stopping query-scheduler replies SHUTTING_DOWN to the next message on a stream, and the stream ends.
			shutDownNextStream.Store(true)
			ctx, cancel := context.WithCancel(user.InjectOrgID(t.Context(), userID))
			errCh := make(chan error, 1)
			go func() {
				_, _, err := f.RoundTripGRPC(ctx, &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"})
				errCh <- err
			}()
			return func() {
				// This request waits for the worker to open a new stream before its retry.
				// The test does not need its result, so it cancels the request.
				cancel()
				require.ErrorIs(t, <-errCh, context.Canceled)
			}
		},
	}

	paths := map[string]struct {
		request     func(ctx context.Context, f *Frontend) error
		expectedErr func(t *testing.T, err error)
	}{
		"HTTP request": {
			request: func(ctx context.Context, f *Frontend) error {
				_, body, err := f.RoundTripGRPC(ctx, &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"})
				if err == nil {
					return errors.Join(errors.New("expected an error, got a response"), body.Close())
				}
				return err
			},
			expectedErr: func(t *testing.T, err error) {
				// The retry middleware retries a 5xx.
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "expected an httpgrpc error, got %v", err)
				require.Equal(t, int32(http.StatusInternalServerError), resp.Code)
			},
		},
		"Protobuf request": {
			request: func(ctx context.Context, f *Frontend) error {
				ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
				resp, err := f.DoProtobufRequest(ctx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
				if err != nil {
					return err
				}
				defer resp.Close()

				msg, err := resp.Next(ctx)
				if err == nil {
					msg.FreeBuffer()
					return errors.New("expected an error, got a message")
				}
				return err
			},
			expectedErr: func(t *testing.T, err error) {
				// The retry middleware retries only TypeInternal.
				var apiErr *apierror.APIError
				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, apierror.TypeInternal, apiErr.Type)
			},
		},
	}

	for triggerName, trigger := range triggers {
		for pathName, path := range paths {
			t.Run(fmt.Sprintf("%s, %s", triggerName, pathName), func(t *testing.T) {
				reg := prometheus.NewPedanticRegistry()
				shutDownNextStream := atomic.NewBool(false)
				// With one worker, the query-frontend has one stream to the query-scheduler.
				f, ms := setupFrontendWithConcurrencyAndServerOptions(t, reg, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
					if shutDownNextStream.CompareAndSwap(true, false) {
						return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
					}
					return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
				}, 1, log.NewNopLogger(), nil)

				// No querier replies, so only the deadline ends the wait if the query-frontend does not see the lost query-scheduler.
				ctx, cancel := context.WithTimeout(user.InjectOrgID(t.Context(), userID), 2*time.Second)
				defer cancel()
				reqStats, ctx := stats.ContextWithEmptyStats(ctx)

				errCh := make(chan error, 1)
				go func() {
					errCh <- path.request(ctx, f)
				}()

				test.Poll(t, time.Second, 1.0, func() any {
					return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
				})

				finishTrigger := trigger(t, f, ms, shutDownNextStream)

				path.expectedErr(t, <-errCh)
				finishTrigger()
				verifySchedulerLostRequestsMetric(t, reg, 1)
				// No querier reports a queue time for a lost request.
				require.Greater(t, reqStats.LoadQueueTime(), time.Duration(0))
			})
		}
	}
}

func TestFrontend_SchedulerLostSendsCancelOnNextStream(t *testing.T) {
	// The query-scheduler counts a stream from its INIT, and the query-frontend counts it from the reply to INIT.
	// So the query-frontend can lose all its streams while the query-scheduler still counts one and keeps the request.
	// The query-frontend then sends CANCEL for the lost request on its next stream.
	const userID = "test"

	requests := map[string]func(ctx context.Context, f *Frontend) error{
		"HTTP request": func(ctx context.Context, f *Frontend) error {
			_, body, err := f.RoundTripGRPC(ctx, &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"})
			if err == nil {
				return errors.Join(errors.New("expected an error, got a response"), body.Close())
			}
			return err
		},
		"Protobuf request": func(ctx context.Context, f *Frontend) error {
			ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
			resp, err := f.DoProtobufRequest(ctx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
			if err != nil {
				return err
			}
			defer resp.Close()

			msg, err := resp.Next(ctx)
			if err == nil {
				msg.FreeBuffer()
				return errors.New("expected an error, got a message")
			}
			return err
		},
	}

	for name, request := range requests {
		t.Run(name, func(t *testing.T) {
			shutDownNextStream := atomic.NewBool(false)
			// With one worker, the query-frontend has one stream to the query-scheduler.
			f, ms := setupFrontendWithConcurrencyAndServerOptions(t, prometheus.NewPedanticRegistry(), func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				if shutDownNextStream.CompareAndSwap(true, false) {
					return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
				}
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			}, 1, log.NewNopLogger(), nil)

			ctx, cancel := context.WithTimeout(user.InjectOrgID(t.Context(), userID), 2*time.Second)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- request(ctx, f)
			}()

			test.Poll(t, time.Second, 1.0, func() any {
				return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
			})
			var lostQueryID uint64
			ms.checkWithLock(func() {
				require.Len(t, ms.msgs, 1)
				lostQueryID = ms.msgs[0].QueryID
			})

			// A stopping query-scheduler replies SHUTTING_DOWN to the next message on a stream, and the stream ends.
			// The mock query-scheduler accepts the next stream.
			shutDownNextStream.Store(true)
			nextCtx, cancelNext := context.WithCancel(user.InjectOrgID(t.Context(), userID))
			nextErrCh := make(chan error, 1)
			go func() {
				_, _, err := f.RoundTripGRPC(nextCtx, &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"})
				nextErrCh <- err
			}()

			require.Error(t, <-errCh)
			test.Poll(t, time.Second, true, func() any {
				found := false
				ms.checkWithLock(func() {
					for _, msg := range ms.msgs {
						if msg.Type == schedulerpb.CANCEL && msg.QueryID == lostQueryID {
							found = true
						}
					}
				})
				return found
			})

			// No querier replies to the next request, so the test cancels it.
			cancelNext()
			require.ErrorIs(t, <-nextErrCh, context.Canceled)
		})
	}
}

// One query-scheduler stop can lose more requests than the cancel queue of a worker holds.
// A lost request must then still fail fast, and log the full queue at debug level only.
func TestFrontend_SchedulerLostWithFullCancelQueue(t *testing.T) {
	const userID = "test"

	paths := map[string]struct {
		request     func(ctx context.Context, f *Frontend) error
		expectedErr func(t *testing.T, err error)
	}{
		"HTTP request": {
			request: func(ctx context.Context, f *Frontend) error {
				_, body, err := f.RoundTripGRPC(ctx, &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"})
				if err == nil {
					return errors.Join(errors.New("expected an error, got a response"), body.Close())
				}
				return err
			},
			expectedErr: func(t *testing.T, err error) {
				resp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "expected an httpgrpc error, got %v", err)
				require.Equal(t, int32(http.StatusInternalServerError), resp.Code)
			},
		},
		"Protobuf request": {
			request: func(ctx context.Context, f *Frontend) error {
				ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
				resp, err := f.DoProtobufRequest(ctx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
				if err != nil {
					return err
				}
				defer resp.Close()

				msg, err := resp.Next(ctx)
				if err == nil {
					msg.FreeBuffer()
					return errors.New("expected an error, got a message")
				}
				return err
			},
			expectedErr: func(t *testing.T, err error) {
				var apiErr *apierror.APIError
				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, apierror.TypeInternal, apiErr.Type)
			},
		},
	}

	for name, path := range paths {
		t.Run(name, func(t *testing.T) {
			logs := &concurrency.SyncBuffer{}
			releaseCancel := make(chan struct{})
			// With one worker, the query-frontend has one stream to the query-scheduler.
			f, ms := setupFrontendWithConcurrencyAndServerOptions(t, prometheus.NewPedanticRegistry(), func(_ *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				if msg.Type == schedulerpb.CANCEL {
					// The worker waits for this reply.
					// Until then, it does not read its cancel queue.
					<-releaseCancel
				}
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			}, 1, log.NewLogfmtLogger(logs), nil)
			// Cleanups run in reverse order.
			// So the mock query-scheduler replies before the servers stop.
			t.Cleanup(func() { close(releaseCancel) })

			ctx, cancel := context.WithTimeout(user.InjectOrgID(t.Context(), userID), 2*time.Second)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- path.request(ctx, f)
			}()

			test.Poll(t, time.Second, 1.0, func() any {
				return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
			})

			f.schedulerWorkers.mu.Lock()
			w := f.schedulerWorkers.workers[f.cfg.SchedulerAddress]
			f.schedulerWorkers.mu.Unlock()
			require.NotNil(t, w)

			// The worker sends this cancellation, then waits for the reply.
			w.cancelCh <- 0
			test.Poll(t, time.Second, 2, func() any {
				var msgs int
				ms.checkWithLock(func() { msgs = len(ms.msgs) })
				return msgs
			})
			for range schedulerWorkerCancelChanCapacity {
				w.cancelCh <- 0
			}

			f.schedulerWorkers.InstanceRemoved(servicediscovery.Instance{Address: f.cfg.SchedulerAddress, InUse: true})

			path.expectedErr(t, <-errCh)

			var queueFullLines []string
			for line := range strings.Lines(logs.String()) {
				if strings.Contains(line, "queue full") {
					queueFullLines = append(queueFullLines, line)
				}
			}
			require.Len(t, queueFullLines, 1)
			require.Contains(t, queueFullLines[0], "level=debug")
		})
	}
}

// A query-scheduler keeps the requests of a query-frontend while one stream from that query-frontend stays open.
func TestFrontend_HTTPGRPC_SchedulerStreamEndsWhileAnotherStaysOpen(t *testing.T) {
	const userID = "test"

	reg := prometheus.NewPedanticRegistry()
	shutDownNextStream := atomic.NewBool(false)
	f, ms := setupFrontendWithConcurrencyAndServerOptions(t, reg, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if shutDownNextStream.CompareAndSwap(true, false) {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
		}
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}, 2, log.NewNopLogger(), nil)

	openedStreams := func() any {
		n := 0
		ms.checkWithLock(func() {
			for _, c := range ms.frontendAddr {
				n += c
			}
		})
		return n
	}
	enqueued := func() any {
		return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
	}
	test.Poll(t, time.Second, 2, openedStreams)

	req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
	ctx, cancel := context.WithTimeout(user.InjectOrgID(t.Context(), userID), 5*time.Second)
	defer cancel()

	type result struct {
		resp *httpgrpc.HTTPResponse
		err  error
	}
	firstResult := make(chan result, 1)
	go func() {
		resp, body, err := f.RoundTripGRPC(ctx, req)
		if err == nil {
			err = body.Close()
		}
		firstResult <- result{resp: resp, err: err}
	}()
	test.Poll(t, time.Second, 1.0, enqueued)
	var firstQueryID uint64
	ms.checkWithLock(func() { firstQueryID = ms.msgs[0].QueryID })

	// The second request ends one stream, then the query-frontend enqueues it again on the other stream.
	shutDownNextStream.Store(true)
	secondCtx, cancelSecond := context.WithCancel(ctx)
	secondErr := make(chan error, 1)
	go func() {
		_, _, err := f.RoundTripGRPC(secondCtx, req)
		secondErr <- err
	}()
	test.Poll(t, time.Second, 2.0, enqueued)
	// The worker opens the ended stream again, so the query-frontend has handled the end of that stream.
	test.Poll(t, time.Second, 3, openedStreams)

	select {
	case r := <-firstResult:
		require.FailNow(t, "the first request ended while a stream to its query-scheduler stayed open", "response: %v, error: %v", r.resp, r.err)
	default:
	}

	require.NoError(t, sendResponseWithDelay(f, 0, userID, firstQueryID, &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte("ok")}))
	r := <-firstResult
	require.NoError(t, r.err)
	require.Equal(t, int32(http.StatusOK), r.resp.Code)

	cancelSecond()
	require.ErrorIs(t, <-secondErr, context.Canceled)
	verifySchedulerLostRequestsMetric(t, reg, 0)
}

// Once a querier starts to send a response, the query-frontend reads it to the end, also after it loses the query-scheduler.
func TestFrontend_Protobuf_SchedulerLostAfterResponseStarted(t *testing.T) {
	const userID = "test"

	reg := prometheus.NewPedanticRegistry()
	f, ms := setupFrontendWithConcurrencyAndServerOptions(t, reg, nil, 1, log.NewNopLogger(), nil)

	ctx := user.InjectOrgID(t.Context(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	resp, err := f.DoProtobufRequest(ctx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	test.Poll(t, time.Second, 1.0, func() any {
		return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
	})
	var queryID uint64
	ms.checkWithLock(func() { queryID = ms.msgs[0].QueryID })

	schedulerRemoved := make(chan struct{})
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- sendStreamingResponseWithErrorCapture(f, userID, queryID, func() { <-schedulerRemoved }, newStringMessage("first message"), newStringMessage("second message"))
	}()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	msg.FreeBuffer()

	f.schedulerWorkers.InstanceRemoved(servicediscovery.Instance{Address: f.cfg.SchedulerAddress, InUse: true})
	close(schedulerRemoved)

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	msg.FreeBuffer()

	msg, err = resp.Next(ctx)
	require.EqualError(t, err, "end of stream reached")
	require.Nil(t, msg)
	require.NoError(t, <-sendErr)
	verifySchedulerLostRequestsMetric(t, reg, 0)
}

func TestFrontend_HTTPGRPC_SchedulerLostAfterQuerierTookRequest(t *testing.T) {
	const userID = "test"

	testCases := map[string]struct {
		querierResponse *httpgrpc.HTTPResponse
		expectedCode    int32
		expectedErr     string
	}{
		"querier replies": {
			querierResponse: &httpgrpc.HTTPResponse{Code: http.StatusOK, Body: []byte("ok")},
			expectedCode:    http.StatusOK,
		},
		// After a loss, the querier's reply comes through the fallback in the lost case.
		// A querier cancellation that the caller did not ask for must become a 500 there too.
		"querier cancelled, caller did not cancel": {
			querierResponse: &httpgrpc.HTTPResponse{Code: statusClientClosedRequest, Body: []byte("context canceled")},
			expectedErr:     "query cancelled in the querier, although the caller did not cancel it: context canceled",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			f, ms := setupFrontendWithConcurrencyAndServerOptions(t, reg, nil, 1, log.NewNopLogger(), nil)

			ctx, cancel := context.WithTimeout(user.InjectOrgID(t.Context(), userID), 5*time.Second)
			defer cancel()

			type result struct {
				resp *httpgrpc.HTTPResponse
				err  error
			}
			resultCh := make(chan result, 1)
			go func() {
				resp, body, err := f.RoundTripGRPC(ctx, &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"})
				if err == nil {
					err = body.Close()
				}
				resultCh <- result{resp: resp, err: err}
			}()

			test.Poll(t, time.Second, 1.0, func() any {
				return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
			})
			var queryID uint64
			ms.checkWithLock(func() { queryID = ms.msgs[0].QueryID })

			// QueryResult and QueryResultStream take the request from f.requests, then send the response.
			// The loss comes between these two steps.
			req := f.requests.getAndDelete(queryID)
			require.NotNil(t, req)
			f.schedulerWorkers.InstanceRemoved(servicediscovery.Instance{Address: f.cfg.SchedulerAddress, InUse: true})

			select {
			case r := <-resultCh:
				require.FailNow(t, "the request ended after a querier took it", "response: %v, error: %v", r.resp, r.err)
			case <-time.After(100 * time.Millisecond):
			}

			req.httpResponse <- queryResultWithBody{queryResult: &frontendv2pb.QueryResultRequest{
				QueryID:      queryID,
				HttpResponse: tc.querierResponse,
				Stats:        &stats.SafeStats{},
			}}
			r := <-resultCh
			if tc.expectedErr != "" {
				errResp, ok := httpgrpc.HTTPResponseFromError(r.err)
				require.True(t, ok, "expected an httpgrpc error, got response %v and error %v", r.resp, r.err)
				require.Nil(t, r.resp)
				require.Equal(t, int32(http.StatusInternalServerError), errResp.Code)
				require.Equal(t, tc.expectedErr, string(errResp.Body))
			} else {
				require.NoError(t, r.err)
				require.Equal(t, tc.expectedCode, r.resp.Code)
			}
			verifySchedulerLostRequestsMetric(t, reg, 0)
		})
	}
}

func TestFrontend_HTTPGRPC_SchedulerLostAgainOnNextStream(t *testing.T) {
	// After a loss, the worker opens a new stream with a new lost channel.
	// A request on the new stream waits until the new stream also ends.
	const userID = "test"

	reg := prometheus.NewPedanticRegistry()
	shutDownNextStream := atomic.NewBool(false)
	// With one worker, the query-frontend has one stream to the query-scheduler.
	f, _ := setupFrontendWithConcurrencyAndServerOptions(t, reg, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if shutDownNextStream.CompareAndSwap(true, false) {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
		}
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}, 1, log.NewNopLogger(), nil)

	ctx, cancel := context.WithTimeout(user.InjectOrgID(t.Context(), userID), 5*time.Second)
	defer cancel()

	roundTrip := func(ctx context.Context) <-chan error {
		errCh := make(chan error, 1)
		go func() {
			_, body, err := f.RoundTripGRPC(ctx, &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"})
			if err == nil {
				err = errors.Join(errors.New("expected an error, got a response"), body.Close())
			}
			errCh <- err
		}()
		return errCh
	}
	enqueued := func() any {
		return testutil.ToFloat64(f.enqueueRequests.WithLabelValues("enqueued"))
	}
	requireLost := func(err error) {
		resp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "expected an httpgrpc error, got %v", err)
		require.Equal(t, int32(http.StatusInternalServerError), resp.Code)
		require.Equal(t, "lost all streams to the query-scheduler that holds the request", string(resp.Body))
	}

	first := roundTrip(ctx)
	test.Poll(t, time.Second, 1.0, enqueued)

	// The second request ends the first stream, then the query-frontend enqueues it again on the next stream.
	shutDownNextStream.Store(true)
	second := roundTrip(ctx)
	requireLost(<-first)
	test.Poll(t, time.Second, 2.0, enqueued)

	select {
	case err := <-second:
		require.FailNow(t, "the request on the next stream ended before that stream ended", "error: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// The next message on the second stream ends it.
	shutDownNextStream.Store(true)
	thirdCtx, cancelThird := context.WithCancel(ctx)
	third := roundTrip(thirdCtx)
	requireLost(<-second)
	verifySchedulerLostRequestsMetric(t, reg, 2)

	// No querier replies to the third request, so the test cancels it.
	cancelThird()
	require.ErrorIs(t, <-third, context.Canceled)
}

func TestFrontendSchedulerWorker_SchedulerLoopOnIdleStream(t *testing.T) {
	testCases := map[string]struct {
		// end ends the idle stream.
		end         func(stream *fakeFrontendLoopClient)
		expectedErr string
	}{
		"query-scheduler sends SHUTTING_DOWN right after its last reply": {
			end: func(stream *fakeFrontendLoopClient) {
				stream.replies <- &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN}
			},
			expectedErr: "scheduler is shutting down",
		},
		"query-scheduler crashes": {
			end: func(stream *fakeFrontendLoopClient) {
				// Like gRPC, the fake ends the stream context before Recv returns the error that ended the stream.
				stream.cancel()
				// The worker must wait for that error.
				time.Sleep(100 * time.Millisecond)
				stream.endErr <- errors.New("query-scheduler crashed")
			},
			expectedErr: "query-scheduler crashed",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			stream := &fakeFrontendLoopClient{
				ctx:     ctx,
				cancel:  cancel,
				replies: make(chan *schedulerpb.SchedulerToFrontend, 1),
				endErr:  make(chan error),
				closed:  make(chan struct{}),
			}
			t.Cleanup(func() { close(stream.closed) })
			stream.replies <- &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK} // The reply to INIT.

			w := newFrontendSchedulerWorker(nil, "scheduler", "frontend", make(chan *frontendRequest), frontendToSchedulerAdapter{}, 1, nil, log.NewNopLogger())
			t.Cleanup(func() { w.cancel(nil) })

			result := make(chan error, 1)
			go func() {
				result <- w.schedulerLoop(newRecvStream(stream))
			}()
			// The stream is open after the reply to INIT.
			test.Poll(t, time.Second, 1, func() any {
				w.streamsMu.Lock()
				defer w.streamsMu.Unlock()
				return w.openStreams
			})

			tc.end(stream)
			require.EqualError(t, <-result, tc.expectedErr)
		})
	}
}

func verifySchedulerLostRequestsMetric(t *testing.T, reg prometheus.Gatherer, count int) {
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_query_frontend_scheduler_lost_requests_total Total number of requests that failed because the query-frontend lost all its streams to the query-scheduler that held them.
		# TYPE cortex_query_frontend_scheduler_lost_requests_total counter
		cortex_query_frontend_scheduler_lost_requests_total %d
	`, count)), "cortex_query_frontend_scheduler_lost_requests_total"))
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
			resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
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
			ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
			reqStats, ctx := stats.ContextWithEmptyStats(ctx)
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

			require.Greater(t, reqStats.LoadQueueTime(), time.Duration(0))
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
			resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
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
			ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))

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

func TestFrontend_Protobuf_ReadingResponseWithCanceledContext(t *testing.T) {
	signal := make(chan struct{})

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		<-signal // Don't respond until the test has attempted to read from the stream.
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), "test")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
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

func TestFrontend_Protobuf_ReadingCancelledRequestBeforeResponseReceivedFromQuerier(t *testing.T) {
	ctx, cancel := context.WithCancelCause(user.InjectOrgID(context.Background(), "test"))
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	cancellationError := cancellation.NewErrorf("the request has been canceled")

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		cancel(cancellationError)
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	uncancelledContext := context.Background()
	msg, err := resp.Next(uncancelledContext)
	require.ErrorIs(t, err, cancellationError)
	require.Nil(t, msg)
}

func TestFrontend_Protobuf_ReadingCancelledRequestAfterResponseReceivedFromQuerier(t *testing.T) {
	ctx, cancel := context.WithCancelCause(user.InjectOrgID(context.Background(), "test"))
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		switch msg.Type {
		case schedulerpb.ENQUEUE:
			go func() {
				err := sendStreamingResponseWithErrorCapture(f, msg.UserID, msg.QueryID, nil, newStringMessage("first message"), newStringMessage("second message"), newStringMessage("third message"))
				require.EqualError(t, err, "rpc error: code = Canceled desc = context canceled: the request has been canceled", "received unexpected error from query-frontend")
			}()
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		case schedulerpb.CANCEL:
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		default:
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}
	})

	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)

	uncancelledContext := context.Background()
	msg, err := resp.Next(uncancelledContext)
	require.NoError(t, err)
	require.Equal(t, "first message", msg.GetEvaluateQueryResponse().GetStringValue().Value)

	// At this point, the next message should be waiting in the stream's channel, and receiveResultForProtobufRequest will be reading the next message from the gRPC stream.
	// Cancel the original request, and confirm that we never receive a nil message and no error from a call to Next:
	// we should either receive the cancellation error or the next remaining message.
	// Once we've seen a cancellation error, then that's all we should get.
	cancellationError := cancellation.NewErrorf("the request has been canceled")
	cancel(cancellationError)

	seenSecondMessage := false
	seenThirdMessage := false
	seenCancellationError := false

	for range 10 {
		msg, err := resp.Next(uncancelledContext)
		if err != nil {
			require.ErrorIs(t, err, cancellationError)
			seenCancellationError = true
			continue
		}

		require.NotNil(t, msg)
		require.False(t, seenCancellationError, "got a non-nil message after already observing the cancellation")

		if !seenSecondMessage {
			require.Equal(t, "second message", msg.GetEvaluateQueryResponse().GetStringValue().Value)
			seenSecondMessage = true
		} else if !seenThirdMessage {
			require.Equal(t, "second message", msg.GetEvaluateQueryResponse().GetStringValue().Value)
			seenThirdMessage = true
		} else {
			require.Failf(t, "received unexpected message", "received message %v", msg)
		}
	}

	require.True(t, seenCancellationError)
}

func TestFrontend_HTTPGRPC_ResponseSentTwice(t *testing.T) {
	const (
		body   = "all fine here"
		userID = "test"
	)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	responseSucceeded := atomic.NewBool(false)
	responseError := atomic.NewError(nil)
	queryID := atomic.NewUint64(0)

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		queryID.Store(msg.QueryID)

		for range 2 {
			go func() {
				defer wg.Done()

				err := sendResponseWithDelay(f, 100*time.Millisecond, userID, msg.QueryID, &httpgrpc.HTTPResponse{
					Code: 200,
					Body: []byte(body),
				})

				if err != nil {
					responseError.CompareAndSwap(nil, err)
				} else {
					responseSucceeded.Store(true)
				}
			}()
		}

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	req := &httpgrpc.HTTPRequest{
		Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}",
	}
	resp, _, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)
	require.Equal(t, int32(200), resp.Code)
	require.Equal(t, []byte(body), resp.Body)

	// Wait for both responses to be sent off, then confirm one failed in the way we expect.
	wg.Wait()
	require.True(t, responseSucceeded.Load())
	require.Equal(t, responseError.Load(), status.Errorf(codes.FailedPrecondition, "query %v not found, cancelled or response already received", queryID.Load()))
}

func TestFrontend_Protobuf_ResponseSentTwice(t *testing.T) {
	const userID = "test"
	wg := &sync.WaitGroup{}
	wg.Add(2)
	responseSucceeded := atomic.NewBool(false)
	responseError := atomic.NewError(nil)
	queryID := atomic.NewUint64(0)
	queryIDReceived := make(chan struct{})

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		queryID.Store(msg.QueryID)
		close(queryIDReceived)

		for range 2 {
			go func() {
				defer wg.Done()

				err := sendStreamingResponseWithErrorCapture(
					f, userID, msg.QueryID, nil,
					newStringMessage("first message"),
					newStringMessage("second message"),
				)

				if err != nil {
					responseError.CompareAndSwap(nil, err)
				} else {
					responseSucceeded.Store(true)
				}
			}()
		}

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	<-queryIDReceived
	firstMessage := newStringMessage("first message")
	firstMessage.QueryID = queryID.Load()
	secondMessage := newStringMessage("second message")
	secondMessage.QueryID = queryID.Load()

	msg, err := resp.Next(ctx)
	require.NoError(t, err)
	buf := msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, firstMessage, msg)

	msg, err = resp.Next(ctx)
	require.NoError(t, err)
	buf = msg.Buffer()
	defer buf.Free()
	msg.SetBuffer(nil) // We don't care about the contents of the buffer in the assertion below.
	require.Equal(t, secondMessage, msg)

	// Response stream exhausted.
	msg, err = resp.Next(ctx)
	require.EqualError(t, err, "end of stream reached")
	require.Nil(t, msg)

	// Wait for both responses to be sent off, then confirm one failed in the way we expect.
	wg.Wait()
	require.True(t, responseSucceeded.Load())
	require.Equal(t, responseError.Load(), status.Errorf(codes.FailedPrecondition, "query %v not found, cancelled or response already received", queryID))
}

func TestFrontend_Protobuf_ResponseWithUnexpectedUserID(t *testing.T) {
	queryID := atomic.NewUint64(0)
	errChan := make(chan error)

	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		go func() {
			queryID.Store(msg.QueryID)
			errChan <- sendStreamingResponseWithErrorCapture(f, "some-other-user", msg.QueryID, nil, newStringMessage("first message"), newStringMessage("second message"))
		}()

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), "the-user")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	req := &querierpb.EvaluateQueryRequest{}
	resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
	require.NoError(t, err)
	defer resp.Close()

	errSentToQuerier := <-errChan
	require.Equal(t, errSentToQuerier, status.Errorf(codes.FailedPrecondition, `got response for query ID %v, expected user "the-user", but response had "some-other-user"`, queryID))
}

func TestFrontend_Protobuf_MultipleConcurrentResponses(t *testing.T) {
	f, _ := setupFrontend(t, nil, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: fmt.Sprintf("unexpected message type %v sent to scheduler", msg.Type)}
		}

		go func() {
			req := &querierpb.EvaluateQueryRequest{}
			require.NoError(t, prototypes.UnmarshalAny(msg.GetProtobufRequest().Payload, req))

			resp := generateConcurrencyTestResponse(req.BatchSize)
			sendStreamingResponse(t, f, msg.UserID, msg.QueryID, resp)
		}()

		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	ctx := user.InjectOrgID(context.Background(), "the-user")
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
	wg := &sync.WaitGroup{}

	// Launch a number of simultaneous requests to ensure that returned response messages don't share the underlying gRPC buffer.
	// Labels in series metadata messages contain unsafe references
	// to the underlying gRPC buffer, so if that buffer is returned to the pool too early, the response will be corrupted.
	for idx := range 50 {
		wg.Go(func() {
			for range 5 {
				req := &querierpb.EvaluateQueryRequest{
					BatchSize: uint64(idx), // Abuse this parameter so we can smuggle an ID to the handler above, so it can generate predictable but different responses for each request.
				}

				resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
				require.NoError(t, err)

				msg, err := resp.Next(ctx)
				require.NoErrorf(t, err, "request %d failed with error", idx)

				// Compare the evaluation response, rather than the top-level message.
				// The evaluation response is the part that contains the interesting fields for this test,
				// and avoiding the top-level message means we don't get failures because the buffers each message
				// holds a reference to is different.
				expected := generateConcurrencyTestResponse(uint64(idx)).GetEvaluateQueryResponse()
				payload := msg.GetEvaluateQueryResponse()
				require.Equal(t, expected, payload)

				msg.FreeBuffer()
				resp.Close()
			}
		})
	}

	wg.Wait()
}

// generateConcurrencyTestResponse generates a unique but predictable querier response message based on idx.
//
// The message is constructed to have a high likelihood of causing noticeable changes in the other message
// if two messages share the same underlying buffer.
func generateConcurrencyTestResponse(idx uint64) *frontendv2pb.QueryResultStreamRequest {
	seriesCount := idx%13 + 1
	var series []labels.Labels
	for seriesIdx := range seriesCount {
		// Add some labels to help identify the response so we can check it is the response we expect.
		lbls := []string{
			"response", strconv.FormatUint(idx, 10),
			"series", strconv.FormatUint(seriesIdx, 10),
		}

		// Add a small number of long labels of different lengths to increase the likelihood of overwriting something else
		// if the buffer is shared by two messages.
		for i := range idx % 5 {
			lbls = append(lbls, fmt.Sprintf("label_%v", i), strings.Repeat("abcde12345fghij6789!", int(idx)))
		}

		series = append(series, labels.FromStrings(lbls...))
	}

	return newSeriesMetadata(0, false, series...)
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

// TestFrontendStreamingResponseAfterCancellationDoesNotLeak reproduces the race where the querier's
// streaming response claims the request just after RoundTripGRPC's cancellation branch found the
// httpResponse channel still empty and returned: nothing will ever read the response or its body pipe,
// so the handler must not stay blocked writing body chunks once the request context is done.
func TestFrontendStreamingResponseAfterCancellationDoesNotLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const userID = "test"

		f := &Frontend{requests: newRequestsInProgress(), log: log.NewNopLogger()}

		ctx, cancel := context.WithCancelCause(user.InjectOrgID(t.Context(), userID))
		freq := &frontendRequest{
			queryID:      117,
			userID:       userID,
			ctx:          ctx,
			httpResponse: make(chan queryResultWithBody, 1),
		}
		f.requests.put(freq)

		// RoundTripGRPC's cancellation branch has already returned and cancelled the request context.
		cause := cancellation.NewErrorf("request cancelled")
		cancel(cause)

		msg := &schedulerpb.FrontendToScheduler{QueryID: freq.queryID}
		stream := &mockQueryResultStreamServer{
			ctx: user.InjectOrgID(t.Context(), userID),
			msgs: []*frontendv2pb.QueryResultStreamRequest{
				metadataRequest(msg, http.StatusOK, nil),
				bodyChunkRequest(msg, []byte("a body chunk nobody will read")),
			},
		}

		streamReturned := make(chan error, 1)
		go func() {
			streamReturned <- f.QueryResultStream(stream)
		}()

		select {
		case err := <-streamReturned:
			require.ErrorIs(t, err, cause)
		case <-time.After(time.Second):
			// Only reached when the handler is stuck: synctest's fake clock advances once no
			// goroutine in the bubble can run. Unblock the handler so its goroutine doesn't
			// also trip the leak detector.
			res := <-freq.httpResponse
			_ = res.bodyStream.Close()
			t.Fatal("QueryResultStream is still blocked writing the body of a response nobody will read")
		}
	})
}

// TestFrontendStreamingResponseDiscardedAfterDrainDoesNotLeak reproduces the case where
// RoundTripGRPC's cancellation branch drains the streaming response from the httpResponse channel
// but discards it without ever reading its body: cancelling the request context alone (which the
// deferred cleanup does) must be enough to unblock the handler writing body chunks to the pipe.
func TestFrontendStreamingResponseDiscardedAfterDrainDoesNotLeak(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const userID = "test"

		f := &Frontend{requests: newRequestsInProgress(), log: log.NewNopLogger()}

		ctx, cancel := context.WithCancelCause(user.InjectOrgID(t.Context(), userID))
		freq := &frontendRequest{
			queryID:      118,
			userID:       userID,
			ctx:          ctx,
			httpResponse: make(chan queryResultWithBody, 1),
		}
		f.requests.put(freq)

		msg := &schedulerpb.FrontendToScheduler{QueryID: freq.queryID}
		stream := &mockQueryResultStreamServer{
			ctx: user.InjectOrgID(t.Context(), userID),
			msgs: []*frontendv2pb.QueryResultStreamRequest{
				metadataRequest(msg, http.StatusOK, nil),
				bodyChunkRequest(msg, []byte("a body chunk nobody will read")),
			},
		}

		streamReturned := make(chan error, 1)
		go func() {
			streamReturned <- f.QueryResultStream(stream)
		}()

		// RoundTripGRPC's cancellation branch drains the response, discarding it without reading the
		// body, and then its deferred cleanup cancels the request context.
		res := <-freq.httpResponse
		cause := cancellation.NewErrorf("request cancelled")
		cancel(cause)

		select {
		case err := <-streamReturned:
			require.ErrorIs(t, err, cause)
		case <-time.After(time.Second):
			// Only reached when the handler is stuck: synctest's fake clock advances once no
			// goroutine in the bubble can run. Unblock the handler so its goroutine doesn't
			// also trip the leak detector.
			_ = res.bodyStream.Close()
			t.Fatal("QueryResultStream is still blocked writing the body of a discarded response")
		}
	})
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

// mockUnmarshallingQueryResultStreamServer is like mockQueryResultStreamServer, but unmarhsals
// responses from bytes, to better emulate the performance characteristics of a real stream.
type mockUnmarshallingQueryResultStreamServer struct {
	ctx     context.Context
	msgs    [][]byte
	next    int
	queryID uint64

	grpc.ServerStream
}

func (s *mockUnmarshallingQueryResultStreamServer) Context() context.Context {
	return s.ctx
}

func (s *mockUnmarshallingQueryResultStreamServer) SendAndClose(_ *frontendv2pb.QueryResultResponse) error {
	return s.ctx.Err()
}

func (s *mockUnmarshallingQueryResultStreamServer) Recv() (*frontendv2pb.QueryResultStreamRequest, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}

	if s.next >= len(s.msgs) {
		return nil, io.EOF
	}
	defer func() { s.next++ }()

	b := s.msgs[s.next]
	msg := &frontendv2pb.QueryResultStreamRequest{}
	if err := msg.Unmarshal(b); err != nil {
		return nil, err
	}

	msg.QueryID = s.queryID

	return msg, nil
}

// fakeFrontendLoopClient is a FrontendLoop client stream that the test ends.
type fakeFrontendLoopClient struct {
	grpc.ClientStream

	ctx    context.Context
	cancel context.CancelFunc
	// replies holds the messages that Recv returns.
	replies chan *schedulerpb.SchedulerToFrontend
	// endErr ends the stream with an error.
	endErr chan error
	// closed ends a Recv that waits, at the end of the test.
	closed chan struct{}
}

func (c *fakeFrontendLoopClient) Send(*schedulerpb.FrontendToScheduler) error { return nil }

func (c *fakeFrontendLoopClient) CloseSend() error { return nil }

func (c *fakeFrontendLoopClient) Context() context.Context { return c.ctx }

func (c *fakeFrontendLoopClient) Recv() (*schedulerpb.SchedulerToFrontend, error) {
	select {
	case msg := <-c.replies:
		return msg, nil
	case err := <-c.endErr:
		return nil, err
	case <-c.closed:
		return nil, io.EOF
	}
}

type mockScheduler struct {
	t      testing.TB
	f      *Frontend
	server *grpc.Server

	replyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend

	mu           sync.Mutex
	frontendAddr map[string]int
	msgs         []*schedulerpb.FrontendToScheduler
}

func newMockScheduler(t testing.TB, f *Frontend, replyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend) *mockScheduler {
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
	}, frontendConcurrency, log.NewLogfmtLogger(os.Stdout), nil, grpc.KeepaliveParams(keepalive.ServerParameters{
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

const rangeURLFormat = "/api/v1/query_range?end=%d&query=go_goroutines{}&start=%d&step=%d"

func makeRangeHTTPRequest(ctx context.Context, start, end time.Time, step int64) *http.Request {
	rangeURL := fmt.Sprintf(rangeURLFormat, end.Unix(), start.Unix(), step)
	rangeHTTPReq, _ := http.NewRequestWithContext(ctx, "GET", rangeURL, bytes.NewReader([]byte{}))
	rangeHTTPReq.RequestURI = rangeHTTPReq.URL.RequestURI()
	return rangeHTTPReq
}

const instantURLFormat = "/api/v1/query?query=go_goroutines{}&time=%d"

func makeInstantHTTPRequest(ctx context.Context, time time.Time) *http.Request {
	instantURL := fmt.Sprintf(instantURLFormat, time.Unix())
	instantHTTPReq, _ := http.NewRequestWithContext(ctx, "GET", instantURL, bytes.NewReader([]byte{}))
	instantHTTPReq.RequestURI = instantHTTPReq.URL.RequestURI()
	return instantHTTPReq
}

const labelValuesURLFormat = "/prometheus/api/v1/label/__name__/values"
const labelValuesURLFormatWithStartOnly = "/prometheus/api/v1/label/__name__/values?start=%d"
const labelValuesURLFormatWithEndOnly = "/prometheus/api/v1/label/__name__/values?end=%d"
const labelValuesURLFormatWithStartAndEnd = "/prometheus/api/v1/label/__name__/values?end=%d&start=%d"

func makeLabelValuesHTTPRequest(ctx context.Context, start, end *time.Time) *http.Request {
	var labelValuesURL string
	switch {
	case start == nil && end == nil:
		labelValuesURL = labelValuesURLFormat
	case start != nil && end == nil:
		labelValuesURL = fmt.Sprintf(labelValuesURLFormatWithStartOnly, start.Unix())
	case start == nil && end != nil:
		labelValuesURL = fmt.Sprintf(labelValuesURLFormatWithEndOnly, end.Unix())
	case start != nil && end != nil:
		labelValuesURL = fmt.Sprintf(labelValuesURLFormatWithStartAndEnd, end.Unix(), start.Unix())
	}

	labelValuesHTTPReq, _ := http.NewRequestWithContext(ctx, "GET", labelValuesURL, bytes.NewReader([]byte{}))
	labelValuesHTTPReq.RequestURI = labelValuesHTTPReq.URL.RequestURI()
	return labelValuesHTTPReq
}

func TestExtractAdditionalQueueDimensions(t *testing.T) {
	frontend := &Frontend{
		cfg:    Config{QueryStoreAfter: 12 * time.Hour},
		limits: limits{queryIngestersWithin: 13 * time.Hour},
		codec:  newTestCodec(),
	}

	now := time.Now()

	// range and label queries have `start` and `end` params,
	// requiring different cases than instant queries with only a `time` param
	// label query start and end params are optional; these tests are only for when both are present
	rangeAndLabelQueryTests := map[string]struct {
		start                       time.Time
		end                         time.Time
		expectedAddlQueueDimensions []string
	}{
		"query with start after query store after is ingesters only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:                            |----|
			start:                       now.Add(-frontend.cfg.QueryStoreAfter).Add(1 * time.Minute),
			end:                         now,
			expectedAddlQueueDimensions: []string{ingesterQueryComponent},
		},
		"query with end before query ingesters within is store-gateways only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:        |----|
			start:                       now.Add(-frontend.limits.QueryIngestersWithin("")).Add(-1 * time.Hour),
			end:                         now.Add(-frontend.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			expectedAddlQueueDimensions: []string{storeGatewayQueryComponent},
		},
		"query with start before query ingesters and end after query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:            |--------------|
			start:                       now.Add(-frontend.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			end:                         now.Add(-frontend.cfg.QueryStoreAfter).Add(1 * time.Minute),
			expectedAddlQueueDimensions: []string{ingesterAndStoreGatewayQueryComponent},
		},
		"query with start before query ingesters and end before query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:             |---------|
			start:                       now.Add(-frontend.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			end:                         now.Add(-frontend.cfg.QueryStoreAfter).Add(-1 * time.Minute),
			expectedAddlQueueDimensions: []string{ingesterAndStoreGatewayQueryComponent},
		},
		"query with start after query ingesters and end after query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:                  |---------|
			start:                       now.Add(-frontend.limits.QueryIngestersWithin("")).Add(-1 * time.Minute),
			end:                         now.Add(-frontend.cfg.QueryStoreAfter).Add(-1 * time.Minute),
			expectedAddlQueueDimensions: []string{ingesterAndStoreGatewayQueryComponent},
		},
		"query with start and end between query ingesters and query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time range:                 |-----|
			start:                       now.Add(-frontend.limits.QueryIngestersWithin("")).Add(29 * time.Minute),
			end:                         now.Add(-frontend.limits.QueryIngestersWithin("")).Add(31 * time.Minute),
			expectedAddlQueueDimensions: []string{ingesterAndStoreGatewayQueryComponent},
		},
	}

	for testName, testData := range rangeAndLabelQueryTests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "tenant-0")

			rangeHTTPReq := makeRangeHTTPRequest(ctx, testData.start, testData.end, 60)
			labelValuesHTTPReq := makeLabelValuesHTTPRequest(ctx, &testData.start, &testData.end)

			reqs := []*http.Request{rangeHTTPReq, labelValuesHTTPReq}

			for _, req := range reqs {
				httpgrpcReq, err := httpgrpc.FromHTTPRequest(req)
				require.NoError(t, err)

				additionalQueueDimensions, err := frontend.extractTouchedQueryComponentsForHTTPRequest(
					ctx, httpgrpcReq, now,
				)
				require.NoError(t, err)
				require.Equal(t, testData.expectedAddlQueueDimensions, additionalQueueDimensions)
			}
		})
	}

	instantQueryTests := map[string]struct {
		time                        time.Time
		expectedAddlQueueDimensions []string
	}{
		"query with time after query store after is ingesters only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time:                                 |
			time:                        now.Add(-frontend.cfg.QueryStoreAfter).Add(1 * time.Minute),
			expectedAddlQueueDimensions: []string{ingesterQueryComponent},
		},
		"query with end before query ingesters within is store-gateways only": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time:                   |
			time:                        now.Add(-frontend.limits.QueryIngestersWithin("")).Add(-1 * time.Hour),
			expectedAddlQueueDimensions: []string{storeGatewayQueryComponent},
		},
		"query with start and end between query ingesters and query store is ingesters-and-store-gateways": {
			// query ingesters:                |------------------
			// query store-gateways:   ------------------|
			// query time:                          |
			time:                        now.Add(-frontend.limits.QueryIngestersWithin("")).Add(30 * time.Minute),
			expectedAddlQueueDimensions: []string{ingesterAndStoreGatewayQueryComponent},
		},
	}
	for testName, testData := range instantQueryTests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "tenant-0")

			instantHTTPReq := makeInstantHTTPRequest(ctx, testData.time)
			httpgrpcReq, err := httpgrpc.FromHTTPRequest(instantHTTPReq)
			require.NoError(t, err)

			additionalQueueDimensions, err := frontend.extractTouchedQueryComponentsForHTTPRequest(
				ctx, httpgrpcReq, now,
			)
			require.NoError(t, err)
			require.Equal(t, testData.expectedAddlQueueDimensions, additionalQueueDimensions)
		})
	}

}

func TestQueryDecoding(t *testing.T) {
	frontend := &Frontend{
		cfg:    Config{QueryStoreAfter: 12 * time.Hour},
		limits: limits{queryIngestersWithin: 13 * time.Hour},
		codec:  newTestCodec(),
	}

	now := time.Now()

	// these timestamps are arbitrary; these tests only check for successful decoding,
	// not the logic of assigning additional queue dimensions
	start := now.Add(-frontend.limits.QueryIngestersWithin("")).Add(29 * time.Minute)
	end := now.Add(-frontend.limits.QueryIngestersWithin("")).Add(31 * time.Minute)

	labelQueryTimeParamTests := map[string]struct {
		start *time.Time
		end   *time.Time
	}{
		"labels query without end time param passes validation": {
			start: &start,
			end:   nil,
		},
		"labels query without start time param passes validation": {
			start: nil,
			end:   &end,
		},
		"labels query without start or end time param passes validation": {
			start: nil,
			end:   nil,
		},
	}

	for testName, testData := range labelQueryTimeParamTests {
		t.Run(testName, func(t *testing.T) {
			ctx := user.InjectOrgID(context.Background(), "tenant-0")

			labelValuesHTTPReq := makeLabelValuesHTTPRequest(ctx, testData.start, testData.end)
			httpgrpcReq, err := httpgrpc.FromHTTPRequest(labelValuesHTTPReq)
			require.NoError(t, err)

			additionalQueueDimensions, err := frontend.extractTouchedQueryComponentsForHTTPRequest(
				ctx, httpgrpcReq, now,
			)
			require.NoError(t, err)
			require.Len(t, additionalQueueDimensions, 1)

		})
	}

	t.Run("malformed httpgrpc requests fail decoding", func(t *testing.T) {
		reqFailsHTTPDecode := &httpgrpc.HTTPRequest{Method: ";"}

		_, errHTTPDecode := frontend.extractTouchedQueryComponentsForHTTPRequest(context.Background(), reqFailsHTTPDecode, time.Now())
		require.Error(t, errHTTPDecode)
		require.Contains(t, errHTTPDecode.Error(), "net/http")
	})
}

func newTestCodec() querymiddleware.Codec {
	return querymiddleware.NewCodec(prometheus.NewPedanticRegistry(), 0*time.Minute, "json", nil, &api.ConsistencyInjector{}, log.NewNopLogger())
}

func BenchmarkProtobufResponseStreamShouldAbortReading(b *testing.B) {
	// It's important to use a context from WithCancel here as other kinds of contexts (eg. context.Background())
	// have different performance characteristics for Err() and Done().
	requestContext, cancelRequest := context.WithCancel(context.Background())
	defer cancelRequest()

	stream := &ProtobufResponseStream{
		notifyClosed:   make(chan struct{}),
		isClosed:       atomic.NewBool(false),
		requestContext: requestContext,
	}

	callContext, cancelCall := context.WithCancel(context.Background())
	defer cancelCall()

	for b.Loop() {
		err := stream.shouldAbortReading(callContext)

		if err != nil {
			require.NoError(b, err, "shouldNotAbortReading should not return an error")
		}
	}
}

const (
	maxInflightRequestsMetric   = "cortex_query_frontend_max_inflight_requests"
	maxInflightRequestAgeMetric = "cortex_query_frontend_max_inflight_request_age_seconds"
)

func maxInflightRequestsExpected(series string) string {
	return `
		# HELP cortex_query_frontend_max_inflight_requests Peak number of concurrent in-flight requests for a tenant since the last metric collection (reset on each scrape). The type label is "http" for requests entering the query-frontend, or "dispatched" for the sub-requests sent on to query-schedulers.
		# TYPE cortex_query_frontend_max_inflight_requests gauge
	` + series
}

func enableMaxInflightMetrics(cfg *Config) {
	cfg.MaxInflightMetricsEnabled = true
}

func TestFrontend_MaxInflightDispatchedMetricsDisabledByDefault(t *testing.T) {
	const userID = "test"

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	require.False(t, cfg.MaxInflightMetricsEnabled)

	reg := prometheus.NewPedanticRegistry()
	f, _ := setupFrontend(t, reg, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		go func() {
			_ = sendResponseWithDelay(f, 0, userID, msg.QueryID, &httpgrpc.HTTPResponse{Code: 200})
		}()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})

	req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
	_, respBody, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)
	require.NoError(t, respBody.Close())

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""),
		maxInflightRequestsMetric, maxInflightRequestAgeMetric))
}

func TestFrontend_MaxInflightDispatchedMetrics(t *testing.T) {
	const (
		userID      = "test"
		concurrency = 3
	)

	started := make(chan struct{}, concurrency)
	release := make(chan struct{})

	reg := prometheus.NewPedanticRegistry()
	f, _ := setupFrontendWithConfig(t, reg, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		// Only enqueues are held. The frontend also sends CANCEL messages here, and one of
		// those must not park a goroutine on the started channel for the rest of the run.
		if msg.Type == schedulerpb.ENQUEUE {
			go func() {
				// Hold the query in flight until the test has gathered the peak.
				started <- struct{}{}
				<-release
				_ = sendResponseWithDelay(f, 0, userID, msg.QueryID, &httpgrpc.HTTPResponse{Code: 200})
			}()
		}
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}, enableMaxInflightMetrics)

	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()

			req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
			_, respBody, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
			if assert.NoError(t, err) {
				assert.NoError(t, respBody.Close())
			}
		}()
	}

	for range concurrency {
		<-started
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(maxInflightRequestsExpected(
		maxInflightRequestsMetric+`{type="dispatched",user="`+userID+`"} 3`+"\n",
	)), maxInflightRequestsMetric))

	count, err := testutil.GatherAndCount(reg, maxInflightRequestAgeMetric)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	close(release)
	wg.Wait()

	// The window the queries spanned still reports the peak, and the next one is empty
	// because nothing is left in flight.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(maxInflightRequestsExpected(
		maxInflightRequestsMetric+`{type="dispatched",user="`+userID+`"} 3`+"\n",
	)), maxInflightRequestsMetric))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""),
		maxInflightRequestsMetric, maxInflightRequestAgeMetric))
}

// The middleware chain closes the response body more than once, which used to drive
// cortex_query_frontend_queries_in_progress permanently negative. The in-flight tracker must
// not have the same flaw: an extra close must leave the tenant at zero in flight, not below
// it. A tenant stuck below zero would never be pruned, so the empty gather at the end is
// what proves the count landed exactly on zero.
func TestFrontend_MaxInflightDispatchedMetrics_ClosingResponseBodyMoreThanOnce(t *testing.T) {
	const userID = "test"

	reg := prometheus.NewPedanticRegistry()
	f, _ := setupFrontendWithConfig(t, reg, func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		go func() {
			_ = sendResponseWithDelay(f, 0, userID, msg.QueryID, &httpgrpc.HTTPResponse{Code: 200})
		}()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}, enableMaxInflightMetrics)

	req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
	_, respBody, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
	require.NoError(t, err)

	require.NoError(t, respBody.Close())
	require.NoError(t, respBody.Close())

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(maxInflightRequestsExpected(
		maxInflightRequestsMetric+`{type="dispatched",user="`+userID+`"} 1`+"\n",
	)), maxInflightRequestsMetric))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""),
		maxInflightRequestsMetric, maxInflightRequestAgeMetric))
}

// A dispatched query that never succeeds must not leak its in-flight entry. As in the
// handler tests, the empty second gather is the real assertion: the tenant is only dropped
// once its in-flight count is back to zero.
func TestFrontend_MaxInflightDispatchedMetrics_NoLeakWhenQueryFails(t *testing.T) {
	const userID = "test"

	// On the httpgrpc path a scheduler rejection is not returned as a Go error: the frontend
	// synthesises a 429 or 500 HTTP response and hands the caller a body to close, exactly as
	// it does for a successful query. So untracking a rejected query depends on the caller
	// closing that body, which is what this test does.
	tests := map[string]schedulerpb.SchedulerToFrontendStatus{
		"scheduler rejects the query": schedulerpb.TOO_MANY_REQUESTS_PER_TENANT,
		"scheduler reports an error":  schedulerpb.ERROR,
		"scheduler is shutting down":  schedulerpb.SHUTTING_DOWN,
	}

	for name, status := range tests {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			f, _ := setupFrontendWithConfig(t, reg, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				return &schedulerpb.SchedulerToFrontend{Status: status}
			}, enableMaxInflightMetrics)

			req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
			resp, respBody, err := f.RoundTripGRPC(user.InjectOrgID(context.Background(), userID), req)
			if err == nil {
				require.NotEqual(t, int32(http.StatusOK), resp.Code, "the query was expected to fail")
				require.NoError(t, respBody.Close())
			}

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(maxInflightRequestsExpected(
				maxInflightRequestsMetric+`{type="dispatched",user="`+userID+`"} 1`+"\n",
			)), maxInflightRequestsMetric))
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""),
				maxInflightRequestsMetric, maxInflightRequestAgeMetric))

			// The pre-existing gauge must agree that nothing is left in flight.
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
				# HELP cortex_query_frontend_queries_in_progress Number of queries in progress handled by this frontend.
				# TYPE cortex_query_frontend_queries_in_progress gauge
				cortex_query_frontend_queries_in_progress 0
			`), "cortex_query_frontend_queries_in_progress"))
		})
	}
}

func TestFrontend_MaxInflightDispatchedMetrics_NoLeakWhenCallerCancels(t *testing.T) {
	const userID = "test"

	enqueued := make(chan struct{})
	// The reply func runs once per enqueue attempt, so the signal has to tolerate repeats.
	var signalOnce sync.Once
	reg := prometheus.NewPedanticRegistry()
	f, _ := setupFrontendWithConfig(t, reg, func(*Frontend, *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		// Accept the query but never send a response, so the caller's cancellation is what
		// unwinds it.
		signalOnce.Do(func() { close(enqueued) })
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}, enableMaxInflightMetrics)

	ctx, cancel := context.WithCancel(user.InjectOrgID(context.Background(), userID))
	done := make(chan struct{})
	go func() {
		defer close(done)

		req := &httpgrpc.HTTPRequest{Url: "/api/v1/query_range?start=946684800&end=946771200&step=60&query=up{}"}
		_, _, err := f.RoundTripGRPC(ctx, req)
		assert.Error(t, err)
	}()

	<-enqueued
	cancel()
	<-done

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(maxInflightRequestsExpected(
		maxInflightRequestsMetric+`{type="dispatched",user="`+userID+`"} 1`+"\n",
	)), maxInflightRequestsMetric))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""),
		maxInflightRequestsMetric, maxInflightRequestAgeMetric))
}
func TestFrontendCreateNewRequestRootQueryID(t *testing.T) {
	const rootQueryID = "3f2b7c14-9d5a-4e61-8b0f-6a2c9d4e7f10"

	t.Run("root query ID in context", func(t *testing.T) {
		f, _ := setupFrontend(t, nil, nil)

		ctx := rootqueryid.ContextWithID(user.InjectOrgID(t.Context(), "test"), rootQueryID)

		freq, _, cancel, err := f.createNewRequest(ctx)
		require.NoError(t, err)
		defer cancel(errExecutingQueryRoundTripFinished)

		require.Equal(t, rootQueryID, freq.rootQueryID)
	})

	t.Run("no root query ID in context", func(t *testing.T) {
		f, _ := setupFrontend(t, nil, nil)

		// Requests that don't come through the transport handler carry no root query ID, which in
		// practice means query stats are disabled. The root query ID stays empty, which downstream
		// treats as unknown. In particular it must not fall back to the sub-request query ID.
		freq, _, cancel, err := f.createNewRequest(user.InjectOrgID(t.Context(), "test"))
		require.NoError(t, err)
		defer cancel(errExecutingQueryRoundTripFinished)

		require.Zero(t, freq.rootQueryID)
		require.NotEqual(t, freq.queryID, freq.rootQueryID)
	})
}

func TestFrontendToSchedulerEnqueueRequestRootQueryID(t *testing.T) {
	const rootQueryID = "3f2b7c14-9d5a-4e61-8b0f-6a2c9d4e7f10"

	adapter := &frontendToSchedulerAdapter{}

	for name, expectedRootQueryID := range map[string]string{
		"root query ID known":   rootQueryID,
		"root query ID unknown": "",
	} {
		t.Run(name, func(t *testing.T) {
			freq := &frontendRequest{
				queryID:     117,
				rootQueryID: expectedRootQueryID,
				userID:      "test",
				ctx:         t.Context(),
				httpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
			}

			msg, err := adapter.frontendToSchedulerEnqueueRequest(freq, "frontend-12345")
			require.NoError(t, err)
			require.Equal(t, uint64(117), msg.QueryID)
			require.Equal(t, expectedRootQueryID, msg.RootQueryID)
		})
	}
}
