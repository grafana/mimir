// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/scheduler_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/cancellation"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	jaegerpropagator "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb" //lint:ignore faillint we can't avoid using this given that's where the Protobuf definition lives
	"github.com/grafana/mimir/pkg/queue"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/promtest"
	"github.com/grafana/mimir/pkg/util/rootqueryid"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

const testMaxOutstandingPerTenant = 5

var (
	spanExporter              = tracetest.NewInMemoryExporter()
	drainingCancellationError = cancellation.NewError(errors.New("draining"))
)

func init() {
	// Set a tracer provider with in memory span exporter so we can check the spans later.
	otel.SetTracerProvider(
		tracesdk.NewTracerProvider(
			tracesdk.WithSpanProcessor(tracesdk.NewSimpleSpanProcessor(spanExporter)),
		),
	)

	// Setup text propagation as it would be done in the tracing. package.

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator([]propagation.TextMapPropagator{
		// w3c Propagator is the default propagator for opentelemetry
		propagation.TraceContext{}, propagation.Baggage{},
		// jaeger Propagator is for opentracing backwards compatibility
		jaegerpropagator.Jaeger{},
	}...))
}

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

func setupScheduler(t *testing.T, reg prometheus.Registerer, opts ...func(*Config)) (*Scheduler, schedulerpb.SchedulerForFrontendClient, schedulerpb.SchedulerForQuerierClient) {
	return setupSchedulerWithLogger(t, reg, log.NewNopLogger(), opts...)
}

func setupSchedulerWithLogger(t *testing.T, reg prometheus.Registerer, logger log.Logger, opts ...func(*Config)) (*Scheduler, schedulerpb.SchedulerForFrontendClient, schedulerpb.SchedulerForQuerierClient) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = testMaxOutstandingPerTenant
	for _, opt := range opts {
		opt(&cfg)
	}

	s, err := NewScheduler(cfg, &limits{queriers: 2}, logger, reg)
	require.NoError(t, err)

	// WaitForHandlers makes Stop wait for the FrontendLoop handlers that a test leaves open.
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	schedulerpb.RegisterSchedulerForFrontendServer(server, s)
	schedulerpb.RegisterSchedulerForQuerierServer(server, s)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() {
		if s.State() != services.Terminated { // Check if we're not already terminated to allow tests to stop it themselves
			_ = services.StopAndAwaitTerminated(context.Background(), s)
		}
	})

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = l.Close()
	})

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	c, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	return s, schedulerpb.NewSchedulerForFrontendClient(c), schedulerpb.NewSchedulerForQuerierClient(c)
}

// This function drains the schedulers inflight, and drains the requestQueue's tree.
// It is only intended for use where you don't actually care about the requests and just need them emptied, and you
// don't care about consistency or waiting for a proper cancellation return.
func drainScheduler(t *testing.T, s *Scheduler) {
	s.inflightRequestsMu.Lock()
	defer s.inflightRequestsMu.Unlock()

	for key := range s.schedulerInflightRequests {
		req := s.schedulerInflightRequests[key]
		if req != nil {
			req.CancelFunc(drainingCancellationError)
		}

		delete(s.schedulerInflightRequests, key)
		s.schedulerInflightRequestCount.Store(int64(len(s.schedulerInflightRequests)))
	}

	lastTenantIndex := queue.FirstTenant()
	querierID := "emptying-consumer"

	querierWorkerConn := queue.NewUnregisteredConsumerWorkerConn(context.Background(), querierID)
	require.NoError(t, s.queue.AwaitRegisterQuerierWorkerConn(querierWorkerConn))
	defer s.queue.SubmitUnregisterQuerierWorkerConn(querierWorkerConn)

	consumer := func(request queue.Item) error {
		return nil
	}

	for {
		if s.queue.IsEmpty() {
			return
		}

		idx, err := queueConsume(s.queue, querierWorkerConn, lastTenantIndex, consumer)
		require.NoError(t, err)
		lastTenantIndex = idx
	}
}

type consumeRequest func(request queue.Item) error

func queueConsume(
	q *schedulerQueue, querierWorkerConn *queue.ConsumerWorkerConn, lastTenantIdx queue.TenantIndex, consumeFunc consumeRequest,
) (queue.TenantIndex, error) {
	dequeueReq := queue.NewConsumerWorkerDequeueRequest(querierWorkerConn, lastTenantIdx)
	request, idx, err := q.AwaitRequestForQuerier(dequeueReq)
	if err != nil {
		return lastTenantIdx, err
	}
	lastTenantIdx = idx

	if consumeFunc != nil {
		err = consumeFunc(request)
	}
	return lastTenantIdx, err
}

func TestSchedulerBasicEnqueue_HTTPPayload(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:         schedulerpb.ENQUEUE,
		QueryID:      1,
		UserID:       "test",
		Payload:      &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
		StatsEnabled: true,
	})

	{
		querierLoop, err := querierClient.QuerierLoop(context.Background())
		require.NoError(t, err)
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

		msg, err := querierLoop.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(1), msg.QueryID)
		require.Equal(t, "frontend-12345", msg.FrontendAddress)
		require.Equal(t, "GET", msg.GetHttpRequest().Method)
		require.Equal(t, "/hello", msg.GetHttpRequest().Url)
		require.Nil(t, msg.GetProtobufRequest())
		require.True(t, msg.StatsEnabled)
		require.Greater(t, msg.QueueTimeNanos, int64(0))
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{}))
	}

	verifyNoPendingRequestsLeft(t, scheduler)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_frontend_error_forwards_total Total number of times the query-scheduler tried to send a request error to the query-frontend, by outcome.
		# TYPE cortex_query_scheduler_frontend_error_forwards_total counter
		cortex_query_scheduler_frontend_error_forwards_total{outcome="frontend_disconnected"} 0
		cortex_query_scheduler_frontend_error_forwards_total{outcome="frontend_not_waiting"} 0
		cortex_query_scheduler_frontend_error_forwards_total{outcome="other"} 0
		cortex_query_scheduler_frontend_error_forwards_total{outcome="success"} 0
		cortex_query_scheduler_frontend_error_forwards_total{outcome="unavailable"} 0
	`), "cortex_query_scheduler_cancelled_requests_total", "cortex_query_scheduler_frontend_error_forwards_total", "cortex_query_scheduler_querier_request_failures_total"))
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerBasicEnqueue_ProtobufPayload(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	request := &schedulerpb.ProtobufRequest{Payload: &types.Any{TypeUrl: "http://my.types/some_request", Value: []byte("hello world")}}
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:         schedulerpb.ENQUEUE,
		QueryID:      1,
		UserID:       "test",
		Payload:      &schedulerpb.FrontendToScheduler_ProtobufRequest{ProtobufRequest: request},
		StatsEnabled: true,
	})

	{
		querierLoop, err := querierClient.QuerierLoop(context.Background())
		require.NoError(t, err)
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

		msg, err := querierLoop.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(1), msg.QueryID)
		require.Equal(t, "frontend-12345", msg.FrontendAddress)
		require.Equal(t, request, msg.GetProtobufRequest())
		require.Nil(t, msg.GetHttpRequest())
		require.True(t, msg.StatsEnabled)
		require.Greater(t, msg.QueueTimeNanos, int64(0))
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{}))
	}

	verifyNoPendingRequestsLeft(t, scheduler)
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerRootQueryIDPropagatedToQuerier(t *testing.T) {
	const rootQueryID = "3f2b7c14-9d5a-4e61-8b0f-6a2c9d4e7f10"

	testCases := map[string]struct {
		rootQueryID         string
		expectedRootQueryID string
		expectedLogFields   []any
	}{
		"frontend reports a root query ID": {
			rootQueryID:         rootQueryID,
			expectedRootQueryID: rootQueryID,
			expectedLogFields:   []any{"user", "test", "query_id", uint64(1), "root_query_id", rootQueryID},
		},
		"frontend reports no root query ID": {
			rootQueryID:         "",
			expectedRootQueryID: "",
			expectedLogFields:   []any{"user", "test", "query_id", uint64(1)},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			scheduler, frontendClient, querierClient := setupScheduler(t, nil)

			frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
			frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
				Type:        schedulerpb.ENQUEUE,
				QueryID:     1,
				RootQueryID: testCase.rootQueryID,
				UserID:      "test",
				Payload:     &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
			})

			querierLoop, err := querierClient.QuerierLoop(t.Context())
			require.NoError(t, err)
			require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

			msg, err := querierLoop.Recv()
			require.NoError(t, err)
			require.Equal(t, uint64(1), msg.QueryID)
			require.Equal(t, testCase.expectedRootQueryID, msg.RootQueryID)

			// An empty root query ID means unknown, so it must be left out of the log fields
			// rather than reported as a real query.
			schedulerReq := &SchedulerRequest{UserID: "test", QueryID: 1, RootQueryID: testCase.rootQueryID}
			require.Equal(t, testCase.expectedLogFields, schedulerReq.LogFields())

			require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{}))

			verifyNoPendingRequestsLeft(t, scheduler)
			verifyQueryComponentUtilizationLeft(t, scheduler)
		})
	}
}

func TestSchedulerLogsRejectedSubRequestWithRootQueryID(t *testing.T) {
	const rootQueryID = "3f2b7c14-9d5a-4e61-8b0f-6a2c9d4e7f10"

	testCases := map[string]struct {
		rootQueryID string
		// expectRootQueryIDLogged is false when the frontend reported no root query ID, in which
		// case the field must be absent rather than reported as an empty value.
		expectRootQueryIDLogged bool
	}{
		"frontend reports a root query ID":  {rootQueryID: rootQueryID, expectRootQueryIDLogged: true},
		"frontend reports no root query ID": {rootQueryID: "", expectRootQueryIDLogged: false},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			logs := &concurrency.SyncBuffer{}
			scheduler, frontendClient, _ := setupSchedulerWithLogger(t, nil, log.NewLogfmtLogger(logs))

			t.Cleanup(func() {
				drainScheduler(t, scheduler)
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
			})

			// Fill the tenant's queue up to the limit. These are admitted, so they must not log.
			for i := 0; i < testMaxOutstandingPerTenant; i++ {
				fl := initFrontendLoop(t, frontendClient, fmt.Sprintf("frontend-%d", i))
				require.NoError(t, fl.Send(&schedulerpb.FrontendToScheduler{
					Type:        schedulerpb.ENQUEUE,
					QueryID:     uint64(i),
					RootQueryID: testCase.rootQueryID,
					UserID:      "test",
					Payload:     &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{}},
				}))

				msg, err := fl.Recv()
				require.NoError(t, err)
				require.Equal(t, schedulerpb.OK, msg.Status)
			}

			require.NotContains(t, logs.String(), "too many outstanding requests", "admitted requests must not log a rejection")

			// One more for the same tenant is rejected, and that rejection must be logged.
			fl := initFrontendLoop(t, frontendClient, "extra-frontend")
			require.NoError(t, fl.Send(&schedulerpb.FrontendToScheduler{
				Type:        schedulerpb.ENQUEUE,
				QueryID:     9999,
				RootQueryID: testCase.rootQueryID,
				UserID:      "test",
				Payload:     &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
			}))

			msg, err := fl.Recv()
			require.NoError(t, err)
			require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, msg.Status)

			require.Eventually(t, func() bool {
				return strings.Contains(logs.String(), "rejected request because the tenant has too many outstanding requests")
			}, time.Second, 10*time.Millisecond, "expected the rejection to be logged, got: %s", logs.String())

			rejection := logs.String()
			require.Contains(t, rejection, "query_id=9999")
			require.Contains(t, rejection, "user=test")

			if testCase.expectRootQueryIDLogged {
				require.Contains(t, rejection, "root_query_id="+rootQueryID)
			} else {
				require.NotContains(t, rejection, "root_query_id")
			}
		})
	}
}

func TestSchedulerEnqueueWithCancel(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.CANCEL,
		QueryID: 1,
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
	verifyCancelledRequests(t, reg, "queued", "frontend_cancelled")
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerEnqueueByMultipleFrontendsWithCancel(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop1 := initFrontendLoop(t, frontendClient, "frontend-1")
	frontendLoop2 := initFrontendLoop(t, frontendClient, "frontend-2")

	frontendToScheduler(t, frontendLoop1, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello1"}},
	})

	frontendToScheduler(t, frontendLoop2, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello2"}},
	})

	// Cancel first query by first frontend.
	frontendToScheduler(t, frontendLoop1, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.CANCEL,
		QueryID: 1,
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	// Let's verify that we can receive query 1 from frontend-2.
	msg, err := querierLoop.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), msg.QueryID)
	require.Equal(t, "frontend-2", msg.FrontendAddress)
	// Must notify scheduler back about finished processing, or it will not send more requests (nor remove "current" request from pending ones).
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{}))

	// But nothing else.
	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerEnqueueWithFrontendDisconnect(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	// Wait until the frontend has connected to the scheduler.
	test.Poll(t, time.Second, float64(1), func() interface{} {
		return testutil.ToFloat64(scheduler.connectedFrontendClients)
	})

	// Disconnect frontend.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](frontendLoop))

	// Wait until the frontend has disconnected.
	test.Poll(t, time.Second, float64(0), func() interface{} {
		return testutil.ToFloat64(scheduler.connectedFrontendClients)
	})

	querierLoop := initQuerierLoop(t, querierClient, "querier-1")

	verifyQuerierDoesntReceiveRequest(t, querierLoop, 500*time.Millisecond)
	verifyNoPendingRequestsLeft(t, scheduler)
	verifyCancelledRequests(t, reg, "queued", "frontend_disconnected")
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestCancelRequestInProgress_QuerierFinishesBeforeObservingCancellation(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// At this point, scheduler assumes that querier is processing the request (until it receives empty QuerierToScheduler message back).
	// Simulate frontend disconnect.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](frontendLoop))

	// Add a little sleep to make sure that scheduler notices frontend disconnect.
	time.Sleep(500 * time.Millisecond)

	// Report back end of request processing. This should return error, since the QuerierLoop call has finished on scheduler.
	// Note: testing on querierLoop.Context() cancellation didn't work :(
	err = querierLoop.Send(&schedulerpb.QuerierToScheduler{})
	require.Error(t, err)

	verifyNoPendingRequestsLeft(t, scheduler)
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestCancelRequestInProgress_QuerierObservesCancellation(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)

	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// At this point, scheduler assumes that querier is processing the request (until it receives empty QuerierToScheduler message back).
	// Simulate frontend disconnect.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](frontendLoop))

	// Add a little sleep to make sure that scheduler notices frontend disconnect.
	time.Sleep(500 * time.Millisecond)

	// Wait for querier to receive notification that query was cancelled.
	_, err = querierLoop.Recv()
	require.Equal(t, codes.Canceled, status.Code(err))

	verifyNoPendingRequestsLeft(t, scheduler)
	verifyCancelledRequests(t, reg, "dispatched", "frontend_disconnected")
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestTracingContext(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")

	req := &schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.ENQUEUE,
		QueryID:         1,
		UserID:          "test",
		Payload:         &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
		FrontendAddress: "frontend-12345",
	}

	ctx, sp := tracer.Start(context.Background(), "client")
	defer sp.End()
	otel.GetTextMapPropagator().Inject(ctx, (*httpgrpcutil.HttpgrpcHeadersCarrier)(req.GetHttpRequest()))

	frontendToScheduler(t, frontendLoop, req)

	scheduler.inflightRequestsMu.Lock()
	defer scheduler.inflightRequestsMu.Unlock()

	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	require.Equal(t, 1, len(scheduler.schedulerInflightRequests))

	for _, r := range scheduler.schedulerInflightRequests {
		require.NotNil(t, r.ParentSpanContext)
		require.True(t, r.ParentSpanContext.IsValid())
		require.Equal(t, sp.SpanContext().TraceID().String(), r.ParentSpanContext.TraceID().String())
	}
}

func TestSchedulerShutdown_FrontendLoop(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")

	// Stop the scheduler. This will disable receiving new requests from frontends.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))

	// We can still send request to scheduler, but we get shutdown error back.
	require.NoError(t, frontendLoop.Send(&schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	}))

	msg, err := frontendLoop.Recv()
	require.NoError(t, err)
	require.Equal(t, schedulerpb.SHUTTING_DOWN, msg.Status)
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerShutdown_QuerierLoop(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	// Scheduler now has 1 query. Let's connect querier and fetch it.

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	drainScheduler(t, scheduler)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))

	// Send a message to unblock the scheduler loop if it's still running, if it's already exited we'll get an io.EOF error
	err = querierLoop.Send(&schedulerpb.QuerierToScheduler{})
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}

	// This should return with error, since scheduler has cancelled any outstanding dequeue requests.
	_, err = querierLoop.Recv()
	status, valid := grpcutil.ErrorToStatus(err)
	require.True(t, valid)
	require.Equal(t, status.Message(), drainingCancellationError.Error())
}

func TestSchedulerShutdown_PendingRequests(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 2,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	// Scheduler now has 2 queries. Let's connect querier and fetch it.
	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	req, err := querierLoop.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(1), req.QueryID)

	// Stop the scheduler which shouldn't exit immediately
	scheduler.StopAsync()

	// Unblock scheduler loop, to find next request.
	err = querierLoop.Send(&schedulerpb.QuerierToScheduler{})
	require.NoError(t, err)

	// This should return the second query
	req, err = querierLoop.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(2), req.QueryID)

	// The queue is empty and there's no inflight requests
	require.Equal(t, true, scheduler.queue.IsEmpty())

	// This should error because the queue is stopped (we can't check the error exactly because its wrapped by grpc)
	err = querierLoop.Send(&schedulerpb.QuerierToScheduler{})
	require.NoError(t, err)
	_, err = querierLoop.Recv()
	require.Error(t, err)

	// Ensure the scheduler correctly terminates after the queue is empty and all inflight requests have been returned
	require.NoError(t, scheduler.AwaitTerminated(context.Background()))
}

// Really silly short test to catch potential flakiness in the shutdown routines (such as deadlocks in channel select).
func TestSchedulerShutdown_Empty(t *testing.T) {
	scheduler, _, _ := setupScheduler(t, nil)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
}

func TestSchedulerMaxOutstandingRequests(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t, nil)

	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	for i := 0; i < testMaxOutstandingPerTenant; i++ {
		// coming from different frontends
		fl := initFrontendLoop(t, frontendClient, fmt.Sprintf("frontend-%d", i))
		require.NoError(t, fl.Send(&schedulerpb.FrontendToScheduler{
			Type:    schedulerpb.ENQUEUE,
			QueryID: uint64(i),
			UserID:  "test", // for same user.
			Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{}},
		}))

		msg, err := fl.Recv()
		require.NoError(t, err)
		require.Equal(t, schedulerpb.OK, msg.Status)
	}

	// One more query from the same user will trigger an error.
	fl := initFrontendLoop(t, frontendClient, "extra-frontend")

	const rejectedRootQueryID = "b81d4fae-7dec-41d1-b2fb-00a0c91e6bf6"

	req := schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     0,
		RootQueryID: rejectedRootQueryID,
		UserID:      "test",
		Payload:     &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	}

	// Inject span context to the request so we can check handling of max outstanding requests.
	spanExporter.Reset()
	ctx, sp := tracer.Start(context.Background(), "client")
	defer sp.End()
	otel.GetTextMapPropagator().Inject(ctx, (*httpgrpcutil.HttpgrpcHeadersCarrier)(req.GetHttpRequest()))

	require.NoError(t, fl.Send(&req))

	msg, err := fl.Recv()
	require.NoError(t, err)
	require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, msg.Status)

	spans := spanExporter.GetSpans()
	require.Greater(t, len(spans), 0, "expected at least one span even if rejected by queue full")

	// The "queued" span is never ended for a rejected request, so it is never exported. The
	// "enqueue" span is, which is why the root query ID has to be on that one.
	require.Equal(t, []string{rejectedRootQueryID}, enqueueSpanRootQueryIDsForTest(),
		"the rejected request's enqueue span must carry the root query ID")
}

func TestSchedulerForwardsErrorToFrontend_HTTPPayload(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)
	fm, frontendAddress := newFrontendMock(t)

	// After preparations, start frontend and querier.
	frontendLoop := initFrontendLoop(t, frontendClient, frontendAddress)
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 100,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	// Scheduler now has 1 query. We now connect querier, fetch the request, and then close the connection.
	// This will make scheduler report error back to frontend.

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// Querier now disconnects, without sending empty message back.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToQuerier](querierLoop))

	// Verify that frontend was notified about request.
	test.Poll(t, 2*time.Second, true, func() interface{} {
		httpResp, grpcResp := fm.getRequest(100)
		require.Nil(t, grpcResp, "should not get a gRPC-style response for a HTTP payload")
		if httpResp == nil {
			return false
		}

		require.Equal(t, int32(http.StatusInternalServerError), httpResp.Code)
		return true
	})
	verifyErrorForwardMetrics(t, reg, "success")
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerForwardsErrorToFrontend_ProtobufPayload(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)
	fm, frontendAddress := newFrontendMock(t)

	// After preparations, start frontend and querier.
	frontendLoop := initFrontendLoop(t, frontendClient, frontendAddress)
	request := &schedulerpb.ProtobufRequest{Payload: &types.Any{TypeUrl: "http://my.types/some_request", Value: []byte("hello world")}}
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 100,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_ProtobufRequest{ProtobufRequest: request},
	})

	// Scheduler now has 1 query. We now connect querier, fetch the request, and then close the connection.
	// This will make scheduler report error back to frontend.

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// Querier now disconnects, without sending empty message back.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToQuerier](querierLoop))

	// Verify that frontend was notified about request.
	test.Poll(t, 2*time.Second, true, func() interface{} {
		httpResp, grpcResp := fm.getRequest(100)
		require.Nil(t, httpResp, "should not get a HTTP-style response for a gRPC payload")
		if grpcResp == nil {
			return false
		}

		expected := []*frontendv2pb.QueryResultStreamRequest{
			{
				QueryID: 100,
				Data: &frontendv2pb.QueryResultStreamRequest_Error{
					Error: &querierpb.Error{
						Type:    mimirpb.QUERY_ERROR_TYPE_INTERNAL,
						Message: "failed to receive response from querier 'querier-1': EOF",
					},
				},
			},
		}

		require.Equal(t, expected, grpcResp)
		return true
	})
	verifyErrorForwardMetrics(t, reg, "success")

	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerForwardsErrorToFrontend_FrontendUnreachable(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)

	// No query-frontend listens on "frontend-12345".
	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 100,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue the query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// Querier now disconnects, without sending empty message back.
	// The error forward to the query-frontend fails.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToQuerier](querierLoop))

	verifyErrorForwardMetrics(t, reg, "unavailable")
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerForwardsErrorToFrontend_FrontendNoLongerWaits(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)
	frontendAddress := newRejectingFrontend(t, status.Error(codes.FailedPrecondition, "query 100 not found, cancelled or response already received"))

	frontendLoop := initFrontendLoop(t, frontendClient, frontendAddress)
	request := &schedulerpb.ProtobufRequest{Payload: &types.Any{TypeUrl: "http://my.types/some_request", Value: []byte("hello world")}}
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 100,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_ProtobufRequest{ProtobufRequest: request},
	})

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue the query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	// Querier now disconnects, without sending empty message back.
	// The query-frontend rejects the error, because it no longer waits for the query.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToQuerier](querierLoop))

	verifyErrorForwardMetrics(t, reg, "frontend_not_waiting")
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestFrontendErrorForwardOutcome(t *testing.T) {
	frontendCancelled, cancelFrontendCancelled := context.WithCancelCause(t.Context())
	cancelFrontendCancelled(cancellation.NewError(errors.New("frontend cancelled query")))
	frontendDisconnected, cancelFrontendDisconnected := context.WithCancelCause(t.Context())
	cancelFrontendDisconnected(errFrontendDisconnected)

	tests := map[string]struct {
		ctx      context.Context
		err      error
		expected string
	}{
		"query-frontend no longer has the query": {
			ctx:      t.Context(),
			err:      status.Error(codes.FailedPrecondition, "query 1 not found, cancelled or response already received"),
			expected: "frontend_not_waiting",
		},
		"query-frontend cancelled the query": {
			ctx:      frontendCancelled,
			err:      status.Error(codes.Canceled, "context canceled"),
			expected: "frontend_not_waiting",
		},
		"query-frontend disconnected": {
			ctx:      frontendDisconnected,
			err:      status.Error(codes.Canceled, "context canceled"),
			expected: "frontend_disconnected",
		},
		"cancelled with a live request context": {
			ctx:      t.Context(),
			err:      status.Error(codes.Canceled, "context canceled: stream closed"),
			expected: "other",
		},
		"query-frontend unreachable": {
			ctx:      t.Context(),
			err:      status.Error(codes.Unavailable, "connection refused"),
			expected: "unavailable",
		},
		"deadline exceeded": {
			ctx:      t.Context(),
			err:      status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
			expected: "other",
		},
		"status lost at send": {
			ctx:      t.Context(),
			err:      io.EOF,
			expected: "other",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, frontendErrorForwardOutcome(tc.ctx, tc.err))
		})
	}
}

func TestSchedulerForwardErrorToFrontendWithCancelledRequestContext(t *testing.T) {
	// The forward makes no call, so the outcome does not depend on the connection state.
	// The outcome comes from the cause of the cancel.
	causes := map[string]struct {
		cause    error
		expected string
	}{
		"query-frontend disconnected": {
			cause:    errFrontendDisconnected,
			expected: "frontend_disconnected",
		},
		"query-frontend cancelled the query": {
			cause:    cancellation.NewError(errors.New("frontend cancelled query")),
			expected: "frontend_not_waiting",
		},
	}
	requests := map[string]func(req *SchedulerRequest){
		"HTTP request":     func(req *SchedulerRequest) { req.HttpRequest = &httpgrpc.HTTPRequest{} },
		"Protobuf request": func(req *SchedulerRequest) { req.ProtobufRequest = &schedulerpb.ProtobufRequest{} },
	}

	for causeName, tc := range causes {
		for requestName, setRequest := range requests {
			t.Run(causeName+", "+requestName, func(t *testing.T) {
				s, _, _ := setupScheduler(t, prometheus.NewPedanticRegistry())

				ctx, cancel := context.WithCancelCause(t.Context())
				cancel(tc.cause)
				// No query-frontend listens on this address.
				req := &SchedulerRequest{FrontendAddr: "127.0.0.1:1", UserID: "test", QueryID: 1}
				setRequest(req)

				s.forwardErrorToFrontend(ctx, req, errors.New("querier failed"))

				for _, outcome := range []string{"frontend_disconnected", "frontend_not_waiting", "other", "success", "unavailable"} {
					expected := 0.0
					if outcome == tc.expected {
						expected = 1
					}
					require.Equal(t, expected, testutil.ToFloat64(s.frontendErrorForwards.WithLabelValues(outcome)), outcome)
				}
			})
		}
	}
}

func TestSchedulerRequestErrorMetricsCleanup(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, _, _ := setupScheduler(t, reg)
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	scheduler.cancelledRequests.WithLabelValues("test", "queued", "frontend_cancelled").Inc()
	scheduler.cancelledRequests.WithLabelValues("test", "dispatched", "frontend_disconnected").Inc()
	scheduler.cancelledRequests.WithLabelValues("another", "queued", "frontend_cancelled").Inc()
	scheduler.querierRequestFailures.WithLabelValues("test").Inc()
	scheduler.querierRequestFailures.WithLabelValues("another").Inc()

	scheduler.cleanupMetricsForInactiveUser("test")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_cancelled_requests_total Total number of query requests that were cancelled after enqueuing, by stage and reason.
		# TYPE cortex_query_scheduler_cancelled_requests_total counter
		cortex_query_scheduler_cancelled_requests_total{reason="frontend_cancelled",stage="queued",user="another"} 1
		# HELP cortex_query_scheduler_querier_request_failures_total Total number of requests that failed because the query-scheduler could not send them to a querier or receive the querier's reply.
		# TYPE cortex_query_scheduler_querier_request_failures_total counter
		cortex_query_scheduler_querier_request_failures_total{user="another"} 1
	`), "cortex_query_scheduler_cancelled_requests_total", "cortex_query_scheduler_querier_request_failures_total"))
}

func TestSchedulerQueueMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	scheduler, frontendClient, _ := setupScheduler(t, reg)

	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "another",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{user="another"} 1
		cortex_query_scheduler_queue_length{user="test"} 1
	`), "cortex_query_scheduler_queue_length"))

	scheduler.cleanupMetricsForInactiveUser("test")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{user="another"} 1
	`), "cortex_query_scheduler_queue_length"))
}

func TestSchedulerMaxQueueLengthMetric(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	scheduler, frontendClient, _ := setupScheduler(t, reg, func(c *Config) {
		c.MaxQueueLengthMetricEnabled = true
	})

	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	require.NotNil(t, scheduler.maxQueueLength, "metric should be constructed when enabled")

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 1,
		UserID:  "another",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_max_queue_length Maximum number of queries observed in a tenant's queue since the last metric collection (reset on each scrape). Captures the true peak queue depth between scrapes.
		# TYPE cortex_query_scheduler_max_queue_length gauge
		cortex_query_scheduler_max_queue_length{user="another"} 1
		cortex_query_scheduler_max_queue_length{user="test"} 1
	`), "cortex_query_scheduler_max_queue_length"))

	scheduler.cleanupMetricsForInactiveUser("test")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_max_queue_length Maximum number of queries observed in a tenant's queue since the last metric collection (reset on each scrape). Captures the true peak queue depth between scrapes.
		# TYPE cortex_query_scheduler_max_queue_length gauge
		cortex_query_scheduler_max_queue_length{user="another"} 1
	`), "cortex_query_scheduler_max_queue_length"))
}

func TestSchedulerMaxQueueLengthMetricDisabled(t *testing.T) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = testMaxOutstandingPerTenant
	cfg.MaxQueueLengthMetricEnabled = false

	reg := prometheus.NewPedanticRegistry()
	s, err := NewScheduler(cfg, &limits{queriers: 2}, log.NewNopLogger(), reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), s))
	})

	require.Nil(t, s.maxQueueLength, "tracker must not be constructed when the metric is disabled")
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_max_queue_length"))
}

func TestSchedulerQuerierMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupScheduler(t, reg)

	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:         schedulerpb.ENQUEUE,
		QueryID:      1,
		UserID:       "test",
		Payload:      &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
		StatsEnabled: true,
	})

	require.Eventually(t, func() bool {
		// No querier is connected yet, so the request stays waiting in the queue and
		// queueMaxWait reports its wait time.
		queueMaxWait := testutil.ToFloat64(scheduler.queueMaxWait)
		return queueMaxWait > 0
	}, time.Second, 10*time.Millisecond, "expected cortex_query_scheduler_queue_max_wait_seconds metric to be greater than zero when a request is waiting in the queue")

	ctx, cancel := context.WithCancel(context.Background())
	querierLoop, err := querierClient.QuerierLoop(ctx)
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	require.Eventually(t, func() bool {
		err := testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_query_scheduler_connected_querier_clients Number of querier worker clients currently connected to the query-scheduler.
			# TYPE cortex_query_scheduler_connected_querier_clients gauge
			cortex_query_scheduler_connected_querier_clients 1
		`), "cortex_query_scheduler_connected_querier_clients")

		return err == nil
	}, time.Second, 10*time.Millisecond, "expected cortex_query_scheduler_connected_querier_clients metric to be incremented after querier connected")

	require.Eventually(t, func() bool {
		// The querier has picked up the request; it is now being processed rather than waiting,
		// so it is excluded from queueMaxWait even though it is still inflight.
		queueMaxWait := testutil.ToFloat64(scheduler.queueMaxWait)
		return queueMaxWait == 0
	}, time.Second, 10*time.Millisecond, "expected cortex_query_scheduler_queue_max_wait_seconds metric to be zero once the only inflight request has been dispatched to a querier")

	cancel()
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToQuerier](querierLoop))

	require.Eventually(t, func() bool {
		err := testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_query_scheduler_connected_querier_clients Number of querier worker clients currently connected to the query-scheduler.
			# TYPE cortex_query_scheduler_connected_querier_clients gauge
			cortex_query_scheduler_connected_querier_clients 0
		`), "cortex_query_scheduler_connected_querier_clients")

		return err == nil
	}, time.Second, 10*time.Millisecond, "expected cortex_query_scheduler_connected_querier_clients metric to be decremented after querier disconnected")

	require.Eventually(t, func() bool {
		// the queue is empty, queueMaxWait reports 0
		queueMaxWait := testutil.ToFloat64(scheduler.queueMaxWait)
		return queueMaxWait == 0
	}, time.Second, 10*time.Millisecond, "expected cortex_query_scheduler_queue_max_wait_seconds metric to be zero when the queue is empty")

	require.NoError(t, promtest.HasNativeHistogram(reg, "cortex_query_scheduler_queue_duration_seconds"))
	require.NoError(t, promtest.HasSampleCount(reg, "cortex_query_scheduler_queue_duration_seconds", 1))
}

func initFrontendLoop(t *testing.T, client schedulerpb.SchedulerForFrontendClient, frontendAddr string) schedulerpb.SchedulerForFrontend_FrontendLoopClient {
	loop, _ := initFrontendLoopWithResponse(t, client, frontendAddr)
	return loop
}

// initFrontendLoopWithResponse is like initFrontendLoop, but also returns the scheduler's response
// to INIT so callers can inspect fields on it (e.g. QueueReservationSupported).
func initFrontendLoopWithResponse(t *testing.T, client schedulerpb.SchedulerForFrontendClient, frontendAddr string) (schedulerpb.SchedulerForFrontend_FrontendLoopClient, *schedulerpb.SchedulerToFrontend) {
	loop, err := client.FrontendLoop(context.Background())
	require.NoError(t, err)

	require.NoError(t, loop.Send(&schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.INIT,
		FrontendAddress: frontendAddr,
	}))

	// Scheduler acks INIT by sending OK back.
	resp, err := loop.Recv()
	require.NoError(t, err)
	require.Equal(t, schedulerpb.OK, resp.Status)

	return loop, resp
}

func initQuerierLoop(t *testing.T, querierClient schedulerpb.SchedulerForQuerierClient, querier string) schedulerpb.SchedulerForQuerier_QuerierLoopClient {
	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: querier}))

	return querierLoop
}
func frontendToScheduler(t *testing.T, frontendLoop schedulerpb.SchedulerForFrontend_FrontendLoopClient, req *schedulerpb.FrontendToScheduler) {
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, frontendLoop, req).Status)
}

// If this verification succeeds, there will be leaked goroutine left behind. It will be cleaned once grpc server is shut down.
func verifyQuerierDoesntReceiveRequest(t *testing.T, querierLoop schedulerpb.SchedulerForQuerier_QuerierLoopClient, timeout time.Duration) {
	ch := make(chan interface{}, 1)

	go func() {
		m, e := querierLoop.Recv()
		if e != nil {
			ch <- e
		} else {
			ch <- m
		}
	}()

	select {
	case val := <-ch:
		require.Failf(t, "expected timeout", "got %v", val)
	case <-time.After(timeout):
		return
	}
}

func verifyNoPendingRequestsLeft(t *testing.T, scheduler *Scheduler) {
	test.Poll(t, 1*time.Second, 0, func() interface{} {
		scheduler.inflightRequestsMu.Lock()
		defer scheduler.inflightRequestsMu.Unlock()
		return len(scheduler.schedulerInflightRequests)
	})
}

func verifyCancelledRequests(t *testing.T, reg prometheus.Gatherer, stage, reason string) {
	test.Poll(t, time.Second, nil, func() interface{} {
		return testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_query_scheduler_cancelled_requests_total Total number of query requests that were cancelled after enqueuing, by stage and reason.
			# TYPE cortex_query_scheduler_cancelled_requests_total counter
			cortex_query_scheduler_cancelled_requests_total{reason="%s",stage="%s",user="test"} 1
		`, reason, stage)), "cortex_query_scheduler_cancelled_requests_total")
	})
}

// verifyErrorForwardMetrics expects exactly one querier request failure, forwarded with the given outcome.
func verifyErrorForwardMetrics(t *testing.T, reg prometheus.Gatherer, outcome string) {
	expected := map[string]int{"frontend_disconnected": 0, "frontend_not_waiting": 0, "other": 0, "success": 0, "unavailable": 0}
	expected[outcome] = 1

	test.Poll(t, time.Second, nil, func() interface{} {
		return testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_query_scheduler_frontend_error_forwards_total Total number of times the query-scheduler tried to send a request error to the query-frontend, by outcome.
			# TYPE cortex_query_scheduler_frontend_error_forwards_total counter
			cortex_query_scheduler_frontend_error_forwards_total{outcome="frontend_disconnected"} %d
			cortex_query_scheduler_frontend_error_forwards_total{outcome="frontend_not_waiting"} %d
			cortex_query_scheduler_frontend_error_forwards_total{outcome="other"} %d
			cortex_query_scheduler_frontend_error_forwards_total{outcome="success"} %d
			cortex_query_scheduler_frontend_error_forwards_total{outcome="unavailable"} %d
			# HELP cortex_query_scheduler_querier_request_failures_total Total number of requests that failed because the query-scheduler could not send them to a querier or receive the querier's reply.
			# TYPE cortex_query_scheduler_querier_request_failures_total counter
			cortex_query_scheduler_querier_request_failures_total{user="test"} 1
		`, expected["frontend_disconnected"], expected["frontend_not_waiting"], expected["other"], expected["success"], expected["unavailable"])), "cortex_query_scheduler_frontend_error_forwards_total", "cortex_query_scheduler_querier_request_failures_total")
	})
}

func verifyQueryComponentUtilizationLeft(t *testing.T, scheduler *Scheduler) {
	scheduler.StopAsync()
	test.Poll(t, 2*time.Second, services.Terminated, func() interface{} {
		return scheduler.State()
	})
	require.Zero(t, scheduler.queue.queryComponentUtilization.GetForComponent(Ingester))
	require.Zero(t, scheduler.queue.queryComponentUtilization.GetForComponent(StoreGateway))
}

type limits struct {
	queriers int
}

func (l limits) MaxQueriersPerUser(_ string) int {
	return l.queriers
}

type frontendMock struct {
	mu            sync.Mutex
	httpResponses map[uint64]*httpgrpc.HTTPResponse
	grpcResponses map[uint64][]*frontendv2pb.QueryResultStreamRequest
}

func newFrontendMock(t *testing.T) (*frontendMock, string) {
	mock := &frontendMock{
		httpResponses: map[uint64]*httpgrpc.HTTPResponse{},
		grpcResponses: map[uint64][]*frontendv2pb.QueryResultStreamRequest{},
	}

	frontendGrpcServer := grpc.NewServer(
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
	)

	frontendv2pb.RegisterFrontendForQuerierServer(frontendGrpcServer, mock)

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	address := l.Addr().String()

	go func() {
		_ = frontendGrpcServer.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	return mock, address
}

func (f *frontendMock) QueryResult(ctx context.Context, request *frontendv2pb.QueryResultRequest) (*frontendv2pb.QueryResultResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, err := user.ExtractOrgID(ctx)
	if err != nil {
		panic(err)
	}

	f.httpResponses[request.QueryID] = request.HttpResponse
	return &frontendv2pb.QueryResultResponse{}, nil
}

func (f *frontendMock) QueryResultStream(stream frontendv2pb.FrontendForQuerier_QueryResultStreamServer) error {
	defer stream.SendAndClose(&frontendv2pb.QueryResultResponse{})

	_, err := user.ExtractOrgID(stream.Context())
	if err != nil {
		panic(err)
	}

	var msgs []*frontendv2pb.QueryResultStreamRequest
	var queryID uint64

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(msgs) == 0 {
					panic("expected at least one message")
				}

				f.mu.Lock()
				f.grpcResponses[queryID] = msgs
				f.mu.Unlock()
				return nil
			}

			return err
		}

		queryID = msg.QueryID
		msg.BufferHolder = mimirpb.BufferHolder{} // Clear the reference to the underlying buffer to make assertions easier.
		msgs = append(msgs, msg)
	}
}

func (f *frontendMock) getRequest(queryID uint64) (*httpgrpc.HTTPResponse, []*frontendv2pb.QueryResultStreamRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.httpResponses[queryID], f.grpcResponses[queryID]
}

func sendFrontendMessage(t *testing.T, fl schedulerpb.SchedulerForFrontend_FrontendLoopClient, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
	t.Helper()
	require.NoError(t, fl.Send(msg))
	resp, err := fl.Recv()
	require.NoError(t, err)
	return resp
}

func reserveMessage(reservationID string, userID string, slots int) *schedulerpb.FrontendToScheduler {
	return &schedulerpb.FrontendToScheduler{
		Type:          schedulerpb.RESERVE,
		ReservationID: reservationID,
		UserID:        userID,
		Slots:         uint64(slots),
	}
}

func completeMessage(reservationID string) *schedulerpb.FrontendToScheduler {
	return &schedulerpb.FrontendToScheduler{Type: schedulerpb.COMPLETE_RESERVATION, ReservationID: reservationID}
}

func abortMessage(reservationID string) *schedulerpb.FrontendToScheduler {
	return &schedulerpb.FrontendToScheduler{Type: schedulerpb.CANCEL, QueryID: 0, ReservationID: reservationID}
}

// reservedEnqueue returns a reserved ENQUEUE for queryID in the reservation that reservationID
// identifies.
func reservedEnqueue(reservationID string, userID string, queryID uint64) *schedulerpb.FrontendToScheduler {
	return &schedulerpb.FrontendToScheduler{
		Type:          schedulerpb.ENQUEUE,
		ReservationID: reservationID,
		UserID:        userID,
		QueryID:       queryID,
		Payload:       &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{}},
	}
}

// testQuerier is a querier that the test drives one step at a time.
type testQuerier struct {
	loop schedulerpb.SchedulerForQuerier_QuerierLoopClient
}

// receive returns the query ID of the next request that the scheduler sends to the querier.
func (q testQuerier) receive(t *testing.T) uint64 {
	t.Helper()
	msg, err := q.loop.Recv()
	require.NoError(t, err)
	return msg.QueryID
}

// finish tells the scheduler that the querier finished its current request.
func (q testQuerier) finish(t *testing.T) {
	t.Helper()
	require.NoError(t, q.loop.Send(&schedulerpb.QuerierToScheduler{}))
}

func setupReservationScheduler(t *testing.T, opts ...func(*Config)) (*Scheduler, schedulerpb.SchedulerForFrontendClient, schedulerpb.SchedulerForQuerierClient, *prometheus.Registry) {
	return setupReservationSchedulerWithLogger(t, log.NewNopLogger(), opts...)
}

func setupReservationSchedulerWithLogger(t *testing.T, logger log.Logger, opts ...func(*Config)) (*Scheduler, schedulerpb.SchedulerForFrontendClient, schedulerpb.SchedulerForQuerierClient, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, querierClient := setupSchedulerWithLogger(t, reg, logger, append([]func(*Config){func(cfg *Config) {
		cfg.QueueReservationEnabled = true
	}}, opts...)...)
	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})
	return scheduler, frontendClient, querierClient, reg
}

const (
	closedReservationsHeader = `
		# HELP cortex_query_scheduler_reservations_closed_total Total number of queue reservations that closed, by outcome: completed, aborted, timed_out or frontend_disconnected.
		# TYPE cortex_query_scheduler_reservations_closed_total counter
`
	rejectedReservationsHeader = `
		# HELP cortex_query_scheduler_reservations_rejected_total Total number of queue reservations that the query-scheduler rejected, by reason: disabled, invalid, slots_above_limit, duplicate or per_tenant_limit. Only per_tenant_limit replies with the retryable 429. The user label is empty for a tenant ID that is not valid.
		# TYPE cortex_query_scheduler_reservations_rejected_total counter
`
	idleSlotsHeader = `
		# HELP cortex_query_scheduler_reservation_idle_slots Number of queue slots that open reservations hold and no queued sub-request uses. These slots count towards the per-tenant queue cap.
		# TYPE cortex_query_scheduler_reservation_idle_slots gauge
`
)

// requireMetric compares the metric name in reg with expected. An empty expected means that the
// metric has no series.
func requireMetric(t *testing.T, reg prometheus.Gatherer, name, header string, series ...string) {
	t.Helper()
	expected := ""
	if len(series) > 0 {
		expected = header + strings.Join(series, "\n") + "\n"
	}
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), name))
}

func requireClosedReservations(t *testing.T, reg prometheus.Gatherer, series ...string) {
	t.Helper()
	requireMetric(t, reg, "cortex_query_scheduler_reservations_closed_total", closedReservationsHeader, series...)
}

func requireIdleSlots(t *testing.T, reg prometheus.Gatherer, series ...string) {
	t.Helper()
	requireMetric(t, reg, "cortex_query_scheduler_reservation_idle_slots", idleSlotsHeader, series...)
}

func TestSchedulerQueueReservation_SlotsAreReusedAcrossAllSubRequests(t *testing.T) {
	const (
		slots            = 4
		totalSubRequests = 64
	)
	// The cap equals slots, so the reservation holds the whole cap. Each sub-request after the first
	// slots sub-requests must reuse a returned slot.
	_, frontendClient, querierClient, reg := setupReservationScheduler(t, func(cfg *Config) {
		cfg.MaxOutstandingPerTenant = slots
	})
	fl, initMsg := initFrontendLoopWithResponse(t, frontendClient, "frontend-1")
	require.True(t, initMsg.QueueReservationSupported)

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", slots)).Status)
	for queryID := uint64(1); queryID <= slots; queryID++ {
		require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", queryID)).Status)
	}

	// The frontend sends the next sub-request only after a querier took one, as its parallelism
	// limiter does. The dispatcher returns the slot before it handles the next ENQUEUE.
	querier := testQuerier{loop: initQuerierLoop(t, querierClient, "querier-1")}
	for queryID := uint64(slots + 1); queryID <= totalSubRequests; queryID++ {
		require.Equal(t, queryID-slots, querier.receive(t))
		querier.finish(t)
		require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", queryID)).Status, "query %d", queryID)
	}
	for queryID := uint64(totalSubRequests - slots + 1); queryID <= totalSubRequests; queryID++ {
		require.Equal(t, queryID, querier.receive(t))
		querier.finish(t)
	}

	// The frontend completes the reservation after its last sub-request. A later ENQUEUE gets
	// ERROR, and a second COMPLETE_RESERVATION is a no-op.
	requireClosedReservations(t, reg)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, completeMessage("reservation-1")).Status)
	requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="completed",user="test"} 1`)
	requireIdleSlots(t, reg)
	late := sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", totalSubRequests+1))
	require.Equal(t, schedulerpb.ERROR, late.Status)
	require.Equal(t, queue.ErrReservationNotFound.Error(), late.Error)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, completeMessage("reservation-1")).Status)
	requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="completed",user="test"} 1`)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_discarded_requests_total"))
	require.NoError(t, promtest.HasSampleCount(reg, "cortex_query_scheduler_reservation_slots", 1))
}

func TestSchedulerQueueReservation_UnusableReservation(t *testing.T) {
	scheduler, frontendClient, _, reg := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	other := initFrontendLoop(t, frontendClient, "frontend-2")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 1)).Status)

	// A reserved ENQUEUE carries its own root query ID, as a plain one does.
	first := reservedEnqueue("reservation-1", "test", 1)
	first.RootQueryID = "root-1"
	spanExporter.Reset()
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, first).Status)
	require.Equal(t, "root-1", pendingRequestForTest(t, scheduler, 1).RootQueryID)
	require.Equal(t, []string{"root-1"}, enqueueSpanRootQueryIDsForTest(), "the enqueue span must carry the ENQUEUE's root query ID")

	testCases := map[string]struct {
		loop          schedulerpb.SchedulerForFrontend_FrontendLoopClient
		msg           *schedulerpb.FrontendToScheduler
		expectedError error
	}{
		// The frontend keeps at most slots sub-requests queued. The per-tenant limit has room.
		"no idle slot": {loop: fl, msg: reservedEnqueue("reservation-1", "test", 2), expectedError: queue.ErrReservationNoIdleSlot},
		// A scheduler restart loses the reservation, so the frontend's ID is unknown here.
		"unknown reservation": {loop: fl, msg: reservedEnqueue("reservation-unknown", "test", 3), expectedError: queue.ErrReservationNotFound},
		"other tenant":        {loop: fl, msg: reservedEnqueue("reservation-1", "other", 4), expectedError: queue.ErrReservationOtherTenant},
		// Reservation IDs belong to the frontend that allocated them.
		"other frontend": {loop: other, msg: reservedEnqueue("reservation-1", "test", 5), expectedError: queue.ErrReservationNotFound},
	}
	for name, testCase := range testCases {
		resp := sendFrontendMessage(t, testCase.loop, testCase.msg)
		require.Equal(t, schedulerpb.ERROR, resp.Status, name)
		require.Equal(t, testCase.expectedError.Error(), resp.Error, name)
	}
	for _, queryID := range []uint64{2, 3, 4} {
		require.False(t, isPendingForTest(scheduler, queryID), "query %d", queryID)
	}
	require.True(t, isPendingForTest(scheduler, 1))

	// A rejected sub-request is not a discarded request, and the reservation stays open.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_discarded_requests_total"))
	requireClosedReservations(t, reg)
}

func TestSchedulerQueueReservation_FrontendDisconnect(t *testing.T) {
	scheduler, frontendClient, _, reg := setupReservationScheduler(t)
	leaving := initFrontendLoop(t, frontendClient, "frontend-1")
	staying := initFrontendLoop(t, frontendClient, "frontend-2")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, leaving, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, leaving, reserveMessage("reservation-2", "test", 1)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, leaving, reserveMessage("reservation-3", "other", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, leaving, reservedEnqueue("reservation-1", "test", 10)).Status)
	inserted := pendingRequestForTest(t, scheduler, 10)
	require.NoError(t, inserted.Ctx.Err())
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, staying, reserveMessage("reservation-1", "test", 1)).Status)
	requireIdleSlots(t, reg,
		`cortex_query_scheduler_reservation_idle_slots{user="other"} 2`,
		`cortex_query_scheduler_reservation_idle_slots{user="test"} 3`,
	)

	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](leaving))

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(scheduler.reservationsClosed.WithLabelValues("test", string(queue.ReservationOwnerDisconnected))) == 2 &&
			testutil.ToFloat64(scheduler.reservationsClosed.WithLabelValues("other", string(queue.ReservationOwnerDisconnected))) == 1
	}, 5*time.Second, 10*time.Millisecond, "the end of the frontend's last stream must close every reservation of the frontend")
	requireIdleSlots(t, reg, `cortex_query_scheduler_reservation_idle_slots{user="test"} 1`)
	require.Equal(t, 1.0, testutil.ToFloat64(scheduler.connectedFrontendClients))
	require.ErrorIs(t, context.Cause(inserted.Ctx), errFrontendDisconnected, "the disconnect must cancel the sub-requests of the frontend")

	// The cancelled sub-request stays queued until a querier skips it, and frontend-2 holds 1 slot.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, staying, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant-2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, staying, reserveMessage("reservation-3", "other", testMaxOutstandingPerTenant)).Status)
}

func TestSchedulerQueueReservation_NewStreamAfterTheLastStreamEndsSeesTheReleasedSlots(t *testing.T) {
	scheduler, frontendClient, _, _ := setupReservationScheduler(t)
	first := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, first, reserveMessage("reservation-1", "test", testMaxOutstandingPerTenant)).Status)

	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](first))
	// The scheduler deletes the frontend and releases its slots under one lock, and the metric
	// reads the frontends under that lock.
	test.Poll(t, 5*time.Second, float64(0), func() interface{} {
		return testutil.ToFloat64(scheduler.connectedFrontendClients)
	})

	second := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, second, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant)).Status)
}

func TestSchedulerQueueReservation_StreamCloseKeepsTheReservationWhileTheFrontendHasAnotherStream(t *testing.T) {
	scheduler, frontendClient, _, reg := setupReservationScheduler(t)
	opener := initFrontendLoop(t, frontendClient, "frontend-1")
	other := initFrontendLoop(t, frontendClient, "frontend-1")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, opener, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, opener, reservedEnqueue("reservation-1", "test", 10)).Status)
	inserted := pendingRequestForTest(t, scheduler, 10)

	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](opener))
	test.Poll(t, 5*time.Second, float64(1), func() interface{} {
		return testutil.ToFloat64(scheduler.connectedFrontendClients)
	})
	requireIdleSlots(t, reg, `cortex_query_scheduler_reservation_idle_slots{user="test"} 1`)
	require.NoError(t, inserted.Ctx.Err())

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, other, reservedEnqueue("reservation-1", "test", 11)).Status)
	requireIdleSlots(t, reg)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, other, completeMessage("reservation-1")).Status)
	requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="completed",user="test"} 1`)
}

func TestSchedulerQueueReservation_AbortReleasesTheIdleSlotsAndKeepsTheQueuedSubRequests(t *testing.T) {
	scheduler, frontendClient, _, reg := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 3)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", 10)).Status)
	inserted := pendingRequestForTest(t, scheduler, 10)

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, abortMessage("reservation-1")).Status)
	requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="aborted",user="test"} 1`)
	requireIdleSlots(t, reg)
	// The frontend cancels queued sub-requests with a CANCEL per query when it needs to.
	require.NoError(t, inserted.Ctx.Err())
	require.True(t, isPendingForTest(scheduler, 10))

	// The queued sub-request keeps its slot, and the abort released the others.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant-1)).Status)

	// An abort of a closed or unknown reservation is a no-op.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, abortMessage("reservation-1")).Status)
	requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="aborted",user="test"} 1`)
}

func TestSchedulerQueueReservation_ReaperClosesOnlyAnIdleReservation(t *testing.T) {
	const timeout = 50 * time.Millisecond
	scheduler, frontendClient, querierClient, reg := setupReservationScheduler(t, func(cfg *Config) {
		cfg.QueueReservationTimeout = timeout
	})
	fl := initFrontendLoop(t, frontendClient, "frontend-1")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", 10)).Status)

	// The queued sub-request keeps the reservation open for many timeouts.
	time.Sleep(10 * timeout)
	requireClosedReservations(t, reg)
	requireIdleSlots(t, reg, `cortex_query_scheduler_reservation_idle_slots{user="test"} 1`)

	// A running sub-request also keeps it open.
	querier := testQuerier{loop: initQuerierLoop(t, querierClient, "querier-1")}
	require.Equal(t, uint64(10), querier.receive(t))
	time.Sleep(10 * timeout)
	requireClosedReservations(t, reg)
	requireIdleSlots(t, reg, `cortex_query_scheduler_reservation_idle_slots{user="test"} 2`)

	// The finish starts the idle time.
	querier.finish(t)
	require.Eventually(t, func() bool {
		return testutil.CollectAndCount(scheduler.reservationsClosed) == 1
	}, 5*time.Second, 10*time.Millisecond, "the reaper must close the idle reservation")
	requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="timed_out",user="test"} 1`)
	requireIdleSlots(t, reg)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant)).Status)
}

func TestSchedulerQueueReservation_QuerierLoopFinishesEachSubRequestOnce(t *testing.T) {
	// The timeout is long enough for the test to send its next ENQUEUE before the reaper sees the
	// reservation idle.
	const timeout = 300 * time.Millisecond
	scheduler, frontendClient, querierClient, reg := setupReservationScheduler(t, func(cfg *Config) {
		cfg.QueueReservationTimeout = timeout
	})
	fl := initFrontendLoop(t, frontendClient, "frontend-1")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 3)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", 1)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", 2)).Status)
	// QuerierLoop skips the cancelled sub-request at its dequeue, and forwards the other one.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, &schedulerpb.FrontendToScheduler{Type: schedulerpb.CANCEL, QueryID: 1}).Status)

	querier := testQuerier{loop: initQuerierLoop(t, querierClient, "querier-1")}
	require.Equal(t, uint64(2), querier.receive(t))
	querier.finish(t)

	// Sub-request 3 runs until the test finishes it. A second finish of 1 or 2 would cancel its
	// inflight count, and then the reaper would close the reservation while 3 runs.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", 3)).Status)
	require.Equal(t, uint64(3), querier.receive(t))
	time.Sleep(4 * timeout)
	requireClosedReservations(t, reg)

	// Without a finish of 1 or 2, the inflight count would stay above 0 after 3 finishes.
	querier.finish(t)
	require.Eventually(t, func() bool {
		return testutil.CollectAndCount(scheduler.reservationsClosed) == 1
	}, 5*time.Second, 10*time.Millisecond, "the reaper must close the reservation after its last sub-request finished")
	requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="timed_out",user="test"} 1`)
}

func TestSchedulerQueueReservation_RejectsInvalidReserve(t *testing.T) {
	_, invalidTenantErr := tenant.TenantIDsFromOrgID("bad\xff")
	require.Error(t, invalidTenantErr)

	testCases := map[string]struct {
		queueReservationDisabled bool
		reserveFirst             bool
		reservationID            string
		userID                   string
		slots                    uint64
		invalidTenantID          bool
		expectedError            string
		expectedReason           string
	}{
		"queue reservation disabled": {
			queueReservationDisabled: true,
			reservationID:            "reservation-1", userID: "test", slots: 1,
			expectedError: errQueueReservationDisabled.Error(), expectedReason: reservationRejectedDisabled,
		},
		"zero slots": {
			reservationID: "reservation-1", userID: "test", slots: 0,
			expectedError: "invalid slot count", expectedReason: reservationRejectedInvalid,
		},
		"slots above math.MaxInt": {
			reservationID: "reservation-1", userID: "test", slots: math.MaxUint64,
			expectedError: "invalid slot count", expectedReason: reservationRejectedInvalid,
		},
		// A CANCEL with an empty reservationID cannot abort a reservation. So RESERVE must not
		// open one.
		"empty reservation ID": {
			reservationID: "", userID: "test", slots: 1,
			expectedError: "invalid reservation ID", expectedReason: reservationRejectedInvalid,
		},
		// A tenant ID that is not valid UTF-8 would panic as a metric label value.
		"tenant ID that is not valid UTF-8": {
			reservationID: "reservation-1", userID: "bad\xff", slots: 1, invalidTenantID: true,
			expectedError: invalidTenantErr.Error(), expectedReason: reservationRejectedInvalid,
		},
		// The slots can never fit, so the reply must not be the retryable 429.
		"slots above the per-tenant limit": {
			reservationID: "reservation-1", userID: "test", slots: testMaxOutstandingPerTenant + 1,
			expectedError: queue.ErrReservationExceedsCap.Error(), expectedReason: reservationRejectedSlotsAboveLimit,
		},
		"duplicate reservation ID": {
			reserveFirst:  true,
			reservationID: "reservation-1", userID: "test", slots: 1,
			expectedError: queue.ErrReservationExists.Error(), expectedReason: reservationRejectedDuplicate,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			_, frontendClient, _, reg := setupReservationScheduler(t, func(cfg *Config) {
				cfg.QueueReservationEnabled = !testCase.queueReservationDisabled
			})
			fl, initMsg := initFrontendLoopWithResponse(t, frontendClient, "frontend-1")
			require.Equal(t, !testCase.queueReservationDisabled, initMsg.QueueReservationSupported)
			var expectedIdle []string
			if testCase.reserveFirst {
				require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage(testCase.reservationID, testCase.userID, 2)).Status)
				expectedIdle = []string{`cortex_query_scheduler_reservation_idle_slots{user="test"} 2`}
			}

			resp := sendFrontendMessage(t, fl, &schedulerpb.FrontendToScheduler{
				Type:          schedulerpb.RESERVE,
				ReservationID: testCase.reservationID,
				UserID:        testCase.userID,
				Slots:         testCase.slots,
			})
			require.Equal(t, schedulerpb.ERROR, resp.Status)
			require.Equal(t, testCase.expectedError, resp.Error)

			expectedUser := testCase.userID
			if testCase.invalidTenantID {
				expectedUser = ""
			}
			requireMetric(t, reg, "cortex_query_scheduler_reservations_rejected_total", rejectedReservationsHeader,
				fmt.Sprintf(`cortex_query_scheduler_reservations_rejected_total{reason=%q,user=%q} 1`, testCase.expectedReason, expectedUser))
			// A rejected RESERVE reserves no slot.
			requireIdleSlots(t, reg, expectedIdle...)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_discarded_requests_total"))
		})
	}
}

func TestSchedulerQueueReservation_RejectedReserveLogsOnce(t *testing.T) {
	logs := &concurrency.SyncBuffer{}
	_, frontendClient, _, _ := setupReservationSchedulerWithLogger(t, log.NewLogfmtLogger(logs))
	fl := initFrontendLoop(t, frontendClient, "frontend-1")

	// One queued sub-request and two idle slots, so that the log line has distinct counts.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 3)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", 10)).Status)

	const message = "rejected reservation because the tenant has too many outstanding requests"
	require.NotContains(t, logs.String(), message, "an accepted reservation must not log a rejection")

	require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", 4)).Status)

	require.Equal(t, 1, strings.Count(logs.String(), message), "a rejected reservation must log one line")
	var rejection string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, message) {
			rejection = line
		}
	}
	require.Equal(t,
		fmt.Sprintf(`level=warn msg="%s" tenant=test addr=frontend-1 reservation_id=reservation-2 requested=4 queued=1 idle=2 max_tenant_queue_size=%d`, message, testMaxOutstandingPerTenant),
		rejection)
}

func TestSchedulerQueueReservation_Metrics(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		scheduler, frontendClient, _, reg := setupReservationScheduler(t)
		fl := initFrontendLoop(t, frontendClient, "frontend-1")

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_query_scheduler_queue_reservation_enabled 1 when queue reservation is enabled on this query-scheduler, 0 otherwise.
			# TYPE cortex_query_scheduler_queue_reservation_enabled gauge
			cortex_query_scheduler_queue_reservation_enabled 1
		`), "cortex_query_scheduler_queue_reservation_enabled"))

		require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
		requireIdleSlots(t, reg, `cortex_query_scheduler_reservation_idle_slots{user="test"} 2`)
		require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", 4)).Status)
		require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, abortMessage("reservation-1")).Status)

		// A rejected reservation is not a discarded request.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_discarded_requests_total"))
		requireMetric(t, reg, "cortex_query_scheduler_reservations_rejected_total", rejectedReservationsHeader,
			`cortex_query_scheduler_reservations_rejected_total{reason="per_tenant_limit",user="test"} 1`)
		require.NoError(t, promtest.HasSampleCount(reg, "cortex_query_scheduler_reservation_slots", 1))
		requireClosedReservations(t, reg, `cortex_query_scheduler_reservations_closed_total{outcome="aborted",user="test"} 1`)

		scheduler.cleanupMetricsForInactiveUser("test")

		for _, name := range []string{
			"cortex_query_scheduler_discarded_requests_total",
			"cortex_query_scheduler_reservations_rejected_total",
			"cortex_query_scheduler_reservations_closed_total",
		} {
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), name), name)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		_, _, _, reg := setupReservationScheduler(t, func(cfg *Config) {
			cfg.QueueReservationEnabled = false
		})
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_query_scheduler_queue_reservation_enabled 1 when queue reservation is enabled on this query-scheduler, 0 otherwise.
			# TYPE cortex_query_scheduler_queue_reservation_enabled gauge
			cortex_query_scheduler_queue_reservation_enabled 0
		`), "cortex_query_scheduler_queue_reservation_enabled"))
	})
}

func TestSchedulerQueueReservation_CleanupKeepsTheIdleSlotsOfAnOpenReservation(t *testing.T) {
	scheduler, frontendClient, _, reg := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)

	scheduler.cleanupMetricsForInactiveUser("test")

	requireIdleSlots(t, reg, `cortex_query_scheduler_reservation_idle_slots{user="test"} 2`)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, abortMessage("reservation-1")).Status)
	requireIdleSlots(t, reg)
}

// enqueueSpanRootQueryIDsForTest returns the root query ID attributes of the exported "enqueue"
// spans.
func enqueueSpanRootQueryIDsForTest() []string {
	var ids []string
	for _, span := range spanExporter.GetSpans() {
		if span.Name != "enqueue" {
			continue
		}
		for _, attr := range span.Attributes {
			if attr.Key == rootqueryid.FieldName {
				ids = append(ids, attr.Value.AsString())
			}
		}
	}
	return ids
}

func TestConfig_Validate_RejectsNonPositiveQueueReservationTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		cfg := Config{}
		flagext.DefaultValues(&cfg)
		cfg.QueueReservationEnabled = true
		cfg.QueueReservationTimeout = timeout
		require.ErrorIs(t, cfg.Validate(), errInvalidQueueReservationTimeout, "timeout %s", timeout)

		cfg.QueueReservationEnabled = false
		require.NoError(t, cfg.Validate(), "the timeout is not used when queue reservation is disabled")
	}
}

func TestQueueErrorResponse(t *testing.T) {
	testCases := map[string]struct {
		err           error
		expectedError string
		expectedCode  schedulerpb.SchedulerToFrontendStatus
	}{
		"no error":                  {err: nil, expectedCode: schedulerpb.OK},
		"queue stopped":             {err: queue.ErrStopped, expectedCode: schedulerpb.SHUTTING_DOWN},
		"too many requests":         {err: queue.ErrTooManyRequests, expectedCode: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT},
		"rejected reservation":      {err: &queue.ReservationRejectedError{}, expectedCode: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT},
		"any other error":           {err: errors.New("boom"), expectedCode: schedulerpb.ERROR, expectedError: "boom"},
		"wrapped queue stopped":     {err: fmt.Errorf("wrapped: %w", queue.ErrStopped), expectedCode: schedulerpb.SHUTTING_DOWN},
		"wrapped too many requests": {err: fmt.Errorf("wrapped: %w", queue.ErrTooManyRequests), expectedCode: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			resp := queueErrorResponse(testCase.err)
			require.Equal(t, testCase.expectedCode, resp.Status)
			require.Equal(t, testCase.expectedError, resp.Error)
		})
	}
}

func pendingRequestForTest(t *testing.T, scheduler *Scheduler, queryID uint64) *SchedulerRequest {
	t.Helper()
	scheduler.inflightRequestsMu.Lock()
	defer scheduler.inflightRequestsMu.Unlock()
	req := scheduler.schedulerInflightRequests[NewSchedulerRequestKey("frontend-1", queryID)]
	require.NotNil(t, req)
	return req
}

func isPendingForTest(scheduler *Scheduler, queryID uint64) bool {
	scheduler.inflightRequestsMu.Lock()
	defer scheduler.inflightRequestsMu.Unlock()
	_, ok := scheduler.schedulerInflightRequests[NewSchedulerRequestKey("frontend-1", queryID)]
	return ok
}

// setupReservationSchedulerWithFastCleanup returns a started scheduler whose inactive-user cleanup
// runs every 10ms and purges a tenant after 100ms of inactivity. Its reservations outlive that
// limit. It returns after the cleanup purged tenant "test", which holds the open reservation
// "reservation-1" with 2 slots. A test holds the returned mutex to stop the cleanup while it
// checks a counter that it created. The returned counter counts the cleanups of tenant "test".
func setupReservationSchedulerWithFastCleanup(t *testing.T) (*Scheduler, *sync.Mutex, *atomic.Int64) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = testMaxOutstandingPerTenant
	cfg.QueueReservationEnabled = true
	cfg.QueueReservationTimeout = time.Hour
	scheduler, err := NewScheduler(cfg, &limits{queriers: 2}, log.NewNopLogger(), prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	// The default cleanup waits for minutes, so the test uses its own. The default one stays idle
	// in the scheduler's subservices.
	pause := &sync.Mutex{}
	cleanups := &atomic.Int64{}
	activeUsers := util.NewActiveUsersCleanupService(10*time.Millisecond, 100*time.Millisecond, func(user string) {
		pause.Lock()
		defer pause.Unlock()
		if user == "test" {
			cleanups.Add(1)
		}
		scheduler.cleanupMetricsForInactiveUser(user)
	})
	scheduler.activeUsers = activeUsers
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), activeUsers))
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), scheduler))
	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), activeUsers))
	})

	require.Equal(t, schedulerpb.OK, scheduler.createReservation("frontend-1", reserveMessage("reservation-1", "test", 2)).Status)
	// The rejected RESERVE creates a rejectedReservations series that the cleanup deletes, so the
	// test can see the cleanup.
	require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, scheduler.createReservation("frontend-1", reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant)).Status)
	waitForCleanup(t, scheduler, cleanups, 0)
	return scheduler, pause, cleanups
}

// waitForCleanup waits until the cleanup purged tenant "test" more than after times.
func waitForCleanup(t *testing.T, scheduler *Scheduler, cleanups *atomic.Int64, after int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		return cleanups.Load() > after && testutil.CollectAndCount(scheduler.rejectedReservations) == 0
	}, 5*time.Second, 10*time.Millisecond, "the cleanup must purge the inactive tenant")
}

// requireCountersWhilePaused runs check while the cleanup cannot run. A failed check still
// releases pause, so the cleanup service can stop.
func requireCountersWhilePaused(pause *sync.Mutex, check func()) {
	pause.Lock()
	defer pause.Unlock()
	check()
}

func requireReservationCountersDeletedEventually(t *testing.T, scheduler *Scheduler) {
	t.Helper()
	require.Eventually(t, func() bool {
		return testutil.CollectAndCount(scheduler.reservationsClosed) == 0
	}, 5*time.Second, 10*time.Millisecond, "a later cleanup must delete the counters that the reservation created")
}

func TestSchedulerQueueReservation_CounterChangeAfterTenantCleanupKeepsTenantActive(t *testing.T) {
	testCases := map[string]struct {
		// prepare runs before the pause. It can make the tenant active, so the test waits for one
		// more cleanup after it.
		prepare func(t *testing.T, scheduler *Scheduler)
		change  func(t *testing.T, scheduler *Scheduler)
		// expectedCounters is the number of reservation counter series that change creates.
		expectedCounters int
	}{
		"timeout": {
			change: func(_ *testing.T, scheduler *Scheduler) {
				scheduler.reapReservationsAt(time.Now().Add(2 * time.Hour))
			},
			expectedCounters: 1,
		},
		"complete": {
			change: func(t *testing.T, scheduler *Scheduler) {
				require.Equal(t, schedulerpb.OK, scheduler.closeReservation("frontend-1", "reservation-1", queue.ReservationCompleted).Status)
			},
			expectedCounters: 1,
		},
		"abort": {
			change: func(t *testing.T, scheduler *Scheduler) {
				require.Equal(t, schedulerpb.OK, scheduler.closeReservation("frontend-1", "reservation-1", queue.ReservationAborted).Status)
			},
			expectedCounters: 1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			scheduler, pause, cleanups := setupReservationSchedulerWithFastCleanup(t)
			if testCase.prepare != nil {
				before := cleanups.Load()
				testCase.prepare(t, scheduler)
				waitForCleanup(t, scheduler, cleanups, before)
			}

			requireCountersWhilePaused(pause, func() {
				testCase.change(t, scheduler)
				require.Equal(t, testCase.expectedCounters, testutil.CollectAndCount(scheduler.reservationsClosed))
			})
			requireReservationCountersDeletedEventually(t, scheduler)
		})
	}
}

func TestSchedulerQueueReservation_ActivityAfterTenantCleanupMarksTenantActive(t *testing.T) {
	testCases := map[string]struct {
		activity func(t *testing.T, scheduler *Scheduler)
	}{
		"sub-request insert": {
			activity: func(t *testing.T, scheduler *Scheduler) {
				require.NoError(t, scheduler.enqueueRequest(t.Context(), "frontend-1", reservedEnqueue("reservation-1", "test", 10)))
			},
		},
		"admitted reserve": {
			activity: func(t *testing.T, scheduler *Scheduler) {
				require.Equal(t, schedulerpb.OK, scheduler.createReservation("frontend-1", reserveMessage("reservation-3", "test", 1)).Status)
			},
		},
		"rejected reserve": {
			activity: func(t *testing.T, scheduler *Scheduler) {
				require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, scheduler.createReservation("frontend-1", reserveMessage("reservation-3", "test", testMaxOutstandingPerTenant)).Status)
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			scheduler, _, cleanups := setupReservationSchedulerWithFastCleanup(t)
			before := cleanups.Load()

			testCase.activity(t, scheduler)
			require.Eventually(t, func() bool {
				return cleanups.Load() > before
			}, 5*time.Second, 10*time.Millisecond, "the tenant must be active again, so a later cleanup runs for it")
		})
	}
}

// rejectingFrontend replies to every query result stream with an error.
type rejectingFrontend struct {
	frontendv2pb.UnimplementedFrontendForQuerierServer
	replyErr error
}

func newRejectingFrontend(t *testing.T, replyErr error) string {
	server := grpc.NewServer()
	frontendv2pb.RegisterFrontendForQuerierServer(server, &rejectingFrontend{replyErr: replyErr})

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(server.Stop)

	return l.Addr().String()
}

func (f *rejectingFrontend) QueryResultStream(stream frontendv2pb.FrontendForQuerier_QueryResultStreamServer) error {
	// Read the first message before the reply, as the query-frontend does.
	if _, err := stream.Recv(); err != nil {
		return err
	}

	return f.replyErr
}
