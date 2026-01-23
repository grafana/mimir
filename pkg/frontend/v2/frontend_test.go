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
	"time"

	"github.com/go-kit/log"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/servicediscovery"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
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
	utiltest "github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	utiltest.VerifyNoLeakTestMain(m)
}

const testFrontendWorkerConcurrency = 5

func setupFrontend(t testing.TB, reg prometheus.Registerer, schedulerReplyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend) (*Frontend, *mockScheduler) {
	return setupFrontendWithConcurrencyAndServerOptions(t, reg, schedulerReplyFunc, testFrontendWorkerConcurrency, log.NewLogfmtLogger(os.Stdout))
}

func setupFrontendWithConcurrencyAndServerOptions(t testing.TB, reg prometheus.Registerer, schedulerReplyFunc func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend, concurrency int, logger log.Logger, opts ...grpc.ServerOption) (*Frontend, *mockScheduler) {
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

	codec := newTestCodec()

	f, err := NewFrontend(cfg, limits{queryIngestersWithin: 13 * time.Hour}, logger, reg, codec)
	require.NoError(t, err)

	frontendv2pb.RegisterFrontendForQuerierServer(frontendServer, f)

	ms := newMockScheduler(t, f, schedulerReplyFunc)
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
			resp, err := f.DoProtobufRequest(ctx, req, time.Now(), time.Now())
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
			ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))

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

type mockScheduler struct {
	t testing.TB
	f *Frontend

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
	}, frontendConcurrency, log.NewLogfmtLogger(os.Stdout), grpc.KeepaliveParams(keepalive.ServerParameters{
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
	return querymiddleware.NewCodec(prometheus.NewPedanticRegistry(), 0*time.Minute, "json", nil, &api.ConsistencyInjector{})
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
