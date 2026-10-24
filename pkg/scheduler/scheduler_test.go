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
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

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
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

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
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

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
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

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

func TestSchedulerQueueReservation_FullLifecycle(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)

	fl, initMsg := initFrontendLoopWithResponse(t, frontendClient, "frontend-1")
	require.True(t, initMsg.QueueReservationSupported)

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)

	// A reserved ENQUEUE carries its own root query ID, as a plain one does.
	completing := reservedEnqueue("reservation-1", "test", 2)
	completing.RootQueryID = "root-2"
	spanExporter.Reset()
	require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, fl, "reservation-1", "test", 1))
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, completing).Status)
	require.Equal(t, "root-2", pendingRequestForTest(t, scheduler, 2).RootQueryID)
	require.Equal(t, []string{"root-2"}, enqueueSpanRootQueryIDsForTest(), "the enqueue span must carry the ENQUEUE's root query ID")

	require.Equal(t, 0.0, testutil.ToFloat64(scheduler.incompleteReservations.WithLabelValues("test")))
	require.Equal(t, 0.0, testutil.ToFloat64(scheduler.unusedReservedSlots.WithLabelValues("test")))
	for _, queryID := range []uint64{1, 2} {
		require.Equal(t, "test", pendingRequestForTest(t, scheduler, queryID).UserID)
	}
	scheduler.reservationsMu.Lock()
	frontendReservations := len(scheduler.reservationsByFrontend["frontend-1"])
	scheduler.reservationsMu.Unlock()
	require.Zero(t, frontendReservations, "the frontend index must drop a completed reservation")

	// The second sub-request completed the reservation, so the reservation is closed.
	resp := sendFrontendMessage(t, fl, reservedEnqueue("reservation-1", "test", 3))
	require.Equal(t, schedulerpb.ERROR, resp.Status)
	require.Equal(t, errReservationClosed.Error(), resp.Error)

	scheduler.inflightRequestsMu.Lock()
	insertedForReservation := len(scheduler.schedulerInflightRequests)
	scheduler.inflightRequestsMu.Unlock()
	require.Equal(t, 2, insertedForReservation, "the first two sub-requests must have been inserted before the third was rejected")
}

func TestSchedulerQueueReservation_FrontendDisconnect(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	leaving := initFrontendLoop(t, frontendClient, "frontend-1")
	staying := initFrontendLoop(t, frontendClient, "frontend-2")
	otherTenantSlots := scheduler.unusedReservedSlots.WithLabelValues("other")

	// frontend-1 holds two reservations of tenant "test" and one of tenant "other". So the release
	// sums the slots of one tenant and releases once for each tenant.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, leaving, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, leaving, reserveMessage("reservation-2", "test", 1)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, leaving, reserveMessage("reservation-3", "other", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, leaving, "reservation-1", "test", 10))
	inserted := pendingRequestForTest(t, scheduler, 10)
	require.NoError(t, inserted.Ctx.Err())
	// Reservation IDs belong to the frontend that allocated them, so frontend-2 can use the same ID.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, staying, reserveMessage("reservation-1", "test", 1)).Status)
	require.Equal(t, 3.0, unusedReservedSlotsForTest(scheduler))
	require.Equal(t, 2.0, testutil.ToFloat64(otherTenantSlots))

	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](leaving))

	// The disconnect counts a reservation after it released the unused slots of every reservation
	// of the frontend.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(scheduler.incompleteReservations.WithLabelValues("test")) == 2 &&
			testutil.ToFloat64(scheduler.incompleteReservations.WithLabelValues("other")) == 1
	}, 5*time.Second, 10*time.Millisecond, "the end of the frontend's last stream must close every reservation of the frontend")
	require.Equal(t, 1.0, unusedReservedSlotsForTest(scheduler), "the disconnect must release only the unused slots of frontend-1")
	require.Zero(t, testutil.ToFloat64(otherTenantSlots))
	require.Equal(t, 1.0, testutil.ToFloat64(scheduler.connectedFrontendClients))
	require.ErrorIs(t, context.Cause(inserted.Ctx), errFrontendDisconnected, "the disconnect must cancel the sub-requests of the frontend")

	scheduler.reservationsMu.Lock()
	_, indexed := scheduler.reservationsByFrontend["frontend-1"]
	scheduler.reservationsMu.Unlock()
	require.False(t, indexed, "the disconnect must drop the frontend from the index")
	openReservationForTest(t, scheduler, reservationKey{frontendAddr: "frontend-2", reservationID: "reservation-1"})

	// The release frees the cap. What stays used is the queued sub-request and the reservation of
	// frontend-2.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, staying, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant-2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, staying, reserveMessage("reservation-3", "other", testMaxOutstandingPerTenant)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, staying, &schedulerpb.FrontendToScheduler{Type: schedulerpb.CANCEL, ReservationID: "reservation-1"}).Status)
}

func TestSchedulerQueueReservation_CloseReleasesUnusedSlotsAndKeepsInsertedSubRequests(t *testing.T) {
	testCases := map[string]struct {
		reservationTimeout time.Duration
		totalSubRequests   int
		insertedQueryIDs   []uint64
		expectedTimeouts   float64
	}{
		"abort via CANCEL": {
			totalSubRequests: testMaxOutstandingPerTenant,
		},
		"abort via CANCEL keeps inserted sub-requests": {
			totalSubRequests: 2,
			insertedQueryIDs: []uint64{10},
		},
		"reservation timeout": {
			reservationTimeout: 50 * time.Millisecond,
			totalSubRequests:   testMaxOutstandingPerTenant,
			expectedTimeouts:   1,
		},
		"reservation timeout keeps inserted sub-requests": {
			reservationTimeout: 50 * time.Millisecond,
			totalSubRequests:   2,
			insertedQueryIDs:   []uint64{10},
			expectedTimeouts:   1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			var opts []func(*Config)
			if testCase.reservationTimeout > 0 {
				opts = append(opts, func(cfg *Config) { cfg.QueueReservationTimeout = testCase.reservationTimeout })
			}
			scheduler, frontendClient := setupReservationScheduler(t, opts...)
			fl := initFrontendLoop(t, frontendClient, "frontend-1")

			require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", testCase.totalSubRequests)).Status)
			require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, fl, "reservation-1", "test", testCase.insertedQueryIDs...))
			var inserted []*SchedulerRequest
			for _, queryID := range testCase.insertedQueryIDs {
				inserted = append(inserted, pendingRequestForTest(t, scheduler, queryID))
			}

			if testCase.reservationTimeout > 0 {
				// The reaper counts the timeout and then the incomplete reservation, both after it
				// released the slots.
				require.Eventually(t, func() bool {
					return testutil.ToFloat64(scheduler.reservationTimeouts.WithLabelValues("test")) == 1 &&
						testutil.ToFloat64(scheduler.incompleteReservations.WithLabelValues("test")) == 1
				}, 5*time.Second, 10*time.Millisecond, "the reservation timeout must be counted")
				require.Zero(t, unusedReservedSlotsForTest(scheduler), "a timed-out reservation must release its unused slots")
			} else {
				abort := &schedulerpb.FrontendToScheduler{Type: schedulerpb.CANCEL, QueryID: 0, ReservationID: "reservation-1"}
				require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, abort).Status)
				require.Zero(t, unusedReservedSlotsForTest(scheduler), "an abort must release every reserved slot")
			}
			require.Equal(t, testCase.expectedTimeouts, testutil.ToFloat64(scheduler.reservationTimeouts.WithLabelValues("test")))
			require.Equal(t, 1.0, testutil.ToFloat64(scheduler.incompleteReservations.WithLabelValues("test")))
			func() {
				scheduler.reservationsMu.Lock()
				defer scheduler.reservationsMu.Unlock()
				require.Empty(t, scheduler.reservationsByFrontend["frontend-1"], "a close must remove the reservation from the frontend index")
			}()

			// The frontend cancels inserted sub-requests with a CANCEL per query when it needs to.
			for i, req := range inserted {
				require.NoError(t, req.Ctx.Err(), "a close releases only the unused slots")
				require.True(t, isPendingForTest(scheduler, testCase.insertedQueryIDs[i]))
			}

			// The close released every unused slot. So the cap admits all slots that no queued
			// sub-request uses.
			freeSlots := testMaxOutstandingPerTenant - len(testCase.insertedQueryIDs)
			require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", freeSlots)).Status)
		})
	}
}

func TestSchedulerQueueReservation_RejectsInvalidReserve(t *testing.T) {
	testCases := map[string]struct {
		queueReservationDisabled bool
		reservationID            string
		userID                   string
		totalSubRequests         uint64
		// expectedError is empty when the test does not check the error text.
		expectedError string
	}{
		"queue reservation disabled": {
			queueReservationDisabled: true,
			reservationID:            "reservation-1",
			userID:                   "test",
			totalSubRequests:         1,
			expectedError:            errQueueReservationDisabled.Error(),
		},
		"zero total sub-requests": {
			reservationID:    "reservation-1",
			userID:           "test",
			totalSubRequests: 0,
			expectedError:    "invalid total sub-requests count",
		},
		"total sub-requests above math.MaxInt": {
			reservationID:    "reservation-1",
			userID:           "test",
			totalSubRequests: math.MaxUint64,
			expectedError:    "invalid total sub-requests count",
		},
		// A CANCEL with an empty reservationID cannot abort a reservation. So RESERVE must not
		// open one.
		"empty reservation ID": {
			reservationID:    "",
			userID:           "test",
			totalSubRequests: 1,
			expectedError:    "invalid reservation ID",
		},
		// A tenant ID that is not valid UTF-8 would panic as a metric label value.
		"tenant ID that is not valid UTF-8": {
			reservationID:    "reservation-1",
			userID:           "bad\xff",
			totalSubRequests: 1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			scheduler, frontendClient := setupReservationScheduler(t, func(cfg *Config) {
				cfg.QueueReservationEnabled = !testCase.queueReservationDisabled
			})
			fl, initMsg := initFrontendLoopWithResponse(t, frontendClient, "frontend-1")
			require.Equal(t, !testCase.queueReservationDisabled, initMsg.QueueReservationSupported)

			resp := sendFrontendMessage(t, fl, &schedulerpb.FrontendToScheduler{
				Type:             schedulerpb.RESERVE,
				ReservationID:    testCase.reservationID,
				UserID:           testCase.userID,
				TotalSubRequests: testCase.totalSubRequests,
			})
			require.Equal(t, schedulerpb.ERROR, resp.Status)
			if testCase.expectedError != "" {
				require.Equal(t, testCase.expectedError, resp.Error)
			} else {
				require.NotEmpty(t, resp.Error)
			}

			require.Zero(t, unusedReservedSlotsForTest(scheduler))
			scheduler.reservationsMu.Lock()
			defer scheduler.reservationsMu.Unlock()
			require.Empty(t, scheduler.openReservations, "a rejected RESERVE must not add a reservation")
		})
	}
}

func TestSchedulerQueueReservation_CleanupMetricsForInactiveUser(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	scheduler, frontendClient, _ := setupScheduler(t, reg, func(cfg *Config) {
		cfg.QueueReservationEnabled = true
	})
	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})

	fl := initFrontendLoop(t, frontendClient, "frontend-1")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 1)).Status)
	require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant+1)).Status)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_reservation_unused_slots Number of reserved queue slots that no queued sub-request uses yet.
		# TYPE cortex_query_scheduler_reservation_unused_slots gauge
		cortex_query_scheduler_reservation_unused_slots{user="test"} 1
	`), "cortex_query_scheduler_reservation_unused_slots"))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_reservation_discarded_total Total number of queue reservations discarded because the tenant had too many outstanding requests.
		# TYPE cortex_query_scheduler_reservation_discarded_total counter
		cortex_query_scheduler_reservation_discarded_total{user="test"} 1
	`), "cortex_query_scheduler_reservation_discarded_total"))
	// Every sub-request of the rejected reservation counts as a discarded request.
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_query_scheduler_discarded_requests_total Total number of query requests discarded. A rejected queue reservation counts each of its sub-requests.
		# TYPE cortex_query_scheduler_discarded_requests_total counter
		cortex_query_scheduler_discarded_requests_total{user="test"} %d
	`, testMaxOutstandingPerTenant+1)), "cortex_query_scheduler_discarded_requests_total"))

	// The cleanup keeps the unused slots gauge while a reservation is open, so the abort comes first.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, &schedulerpb.FrontendToScheduler{Type: schedulerpb.CANCEL, ReservationID: "reservation-1"}).Status)

	scheduler.cleanupMetricsForInactiveUser("test")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_reservation_unused_slots"))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_reservation_discarded_total"))
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), "cortex_query_scheduler_discarded_requests_total"))
}

func TestSchedulerForwardsErrorToFrontend_HTTPPayload(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)
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
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerForwardsErrorToFrontend_ProtobufPayload(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)
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

	verifyQueryComponentUtilizationLeft(t, scheduler)
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

func reserveMessage(reservationID string, userID string, totalSubRequests int) *schedulerpb.FrontendToScheduler {
	return &schedulerpb.FrontendToScheduler{
		Type:             schedulerpb.RESERVE,
		ReservationID:    reservationID,
		UserID:           userID,
		TotalSubRequests: uint64(totalSubRequests),
	}
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

// sendReservedEnqueues sends one reserved ENQUEUE for each of queryIDs on fl. It stops at the first
// reply that is not OK and returns that status. Otherwise it returns OK.
func sendReservedEnqueues(t *testing.T, fl schedulerpb.SchedulerForFrontend_FrontendLoopClient, reservationID string, userID string, queryIDs ...uint64) schedulerpb.SchedulerToFrontendStatus {
	t.Helper()
	for _, queryID := range queryIDs {
		if resp := sendFrontendMessage(t, fl, reservedEnqueue(reservationID, userID, queryID)); resp.Status != schedulerpb.OK {
			return resp.Status
		}
	}
	return schedulerpb.OK
}

// startReservedEnqueueForTest claims one slot of the open reservation at key.
func startReservedEnqueueForTest(t *testing.T, scheduler *Scheduler, key reservationKey) {
	t.Helper()
	_, err := scheduler.startReservedEnqueue(key, "test", time.Now())
	require.NoError(t, err)
}

func setupReservationScheduler(t *testing.T, opts ...func(*Config)) (*Scheduler, schedulerpb.SchedulerForFrontendClient) {
	return setupReservationSchedulerWithLogger(t, log.NewNopLogger(), opts...)
}

func setupReservationSchedulerWithLogger(t *testing.T, logger log.Logger, opts ...func(*Config)) (*Scheduler, schedulerpb.SchedulerForFrontendClient) {
	scheduler, frontendClient, _ := setupSchedulerWithLogger(t, nil, logger, append([]func(*Config){func(cfg *Config) {
		cfg.QueueReservationEnabled = true
	}}, opts...)...)
	t.Cleanup(func() {
		drainScheduler(t, scheduler)
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))
	})
	return scheduler, frontendClient
}

func TestSchedulerQueueReservation_DuplicateReserveIsRejectedAndReleasesItsSlots(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, schedulerpb.ERROR, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, 2.0, unusedReservedSlotsForTest(scheduler),
		"a rejected duplicate RESERVE must release the slots that it reserved")

	require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant)).Status)
	require.Equal(t, 2.0, unusedReservedSlotsForTest(scheduler))
	scheduler.reservationsMu.Lock()
	indexed := len(scheduler.reservationsByFrontend["frontend-1"])
	scheduler.reservationsMu.Unlock()
	require.Equal(t, 1, indexed, "a rejected RESERVE must not add an entry to the frontend index")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, &schedulerpb.FrontendToScheduler{Type: schedulerpb.CANCEL, ReservationID: "reservation-1"}).Status)
	require.Equal(t, 0.0, unusedReservedSlotsForTest(scheduler))
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant)).Status)
}

func TestSchedulerQueueReservation_RejectedReserveLogsOnce(t *testing.T) {
	logs := &concurrency.SyncBuffer{}
	_, frontendClient := setupReservationSchedulerWithLogger(t, log.NewLogfmtLogger(logs))
	fl := initFrontendLoop(t, frontendClient, "frontend-1")

	// One queued sub-request and two unused slots, so that the log line has distinct counts.
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 3)).Status)
	require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, fl, "reservation-1", "test", 10))

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
		fmt.Sprintf(`level=warn msg="%s" tenant=test addr=frontend-1 reservation_id=reservation-2 requested=4 queued=1 reserved=2 max_tenant_queue_size=%d`, message, testMaxOutstandingPerTenant),
		rejection)
}

func TestSchedulerQueueReservation_RejectsReservedEnqueue(t *testing.T) {
	testCases := map[string]struct {
		queueReservationDisabled bool
		// reserve makes frontend-1 open reservation-1 for 1 sub-request before the reserved
		// ENQUEUE.
		reserve               bool
		senderAddr            string
		reservationID         string
		userID                string
		expectedError         error
		expectedReservedSlots float64
	}{
		"unknown reservation": {
			senderAddr:    "frontend-1",
			reservationID: "reservation-7",
			userID:        "test",
			expectedError: errReservationClosed,
		},
		// Reservation IDs belong to the frontend that allocated them.
		"reservation of another frontend": {
			reserve:               true,
			senderAddr:            "frontend-2",
			reservationID:         "reservation-1",
			userID:                "test",
			expectedError:         errReservationClosed,
			expectedReservedSlots: 1,
		},
		"another tenant": {
			reserve:               true,
			senderAddr:            "frontend-1",
			reservationID:         "reservation-1",
			userID:                "other-tenant",
			expectedError:         errReservationOtherTenant,
			expectedReservedSlots: 1,
		},
		// A reserved ENQUEUE must carry the reservation's tenant.
		"empty tenant": {
			reserve:               true,
			senderAddr:            "frontend-1",
			reservationID:         "reservation-1",
			userID:                "",
			expectedError:         errReservationOtherTenant,
			expectedReservedSlots: 1,
		},
		// With queue reservation disabled, no reservation can be open.
		"queue reservation disabled": {
			queueReservationDisabled: true,
			senderAddr:               "frontend-1",
			reservationID:            "reservation-1",
			userID:                   "test",
			expectedError:            errReservationClosed,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			scheduler, frontendClient := setupReservationScheduler(t, func(cfg *Config) {
				cfg.QueueReservationEnabled = !testCase.queueReservationDisabled
			})
			opener := initFrontendLoop(t, frontendClient, "frontend-1")
			sender := opener
			if testCase.senderAddr != "frontend-1" {
				sender = initFrontendLoop(t, frontendClient, testCase.senderAddr)
			}
			if testCase.reserve {
				require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, opener, reserveMessage("reservation-1", "test", 1)).Status)
			}

			resp := sendFrontendMessage(t, sender, reservedEnqueue(testCase.reservationID, testCase.userID, 10))
			require.Equal(t, schedulerpb.ERROR, resp.Status)
			require.Equal(t, testCase.expectedError.Error(), resp.Error)

			scheduler.inflightRequestsMu.Lock()
			_, pending := scheduler.schedulerInflightRequests[NewSchedulerRequestKey(testCase.senderAddr, 10)]
			scheduler.inflightRequestsMu.Unlock()
			require.False(t, pending, "a rejected reserved ENQUEUE must not reach the queue")
			require.Equal(t, testCase.expectedReservedSlots, unusedReservedSlotsForTest(scheduler))

			if testCase.reserve {
				// The rejection keeps the reservation, so its own frontend can still use it.
				require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, opener, "reservation-1", "test", 11))
			}
		})
	}
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

func TestSchedulerQueueReservation_AbortBeforeReserveIsIgnoredAndTheTimeoutReleases(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}

	// The abort reaches the scheduler before the RESERVE adds the reservation.
	scheduler.abortReservation(key.frontendAddr, key.reservationID)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, 2.0, unusedReservedSlotsForTest(scheduler), "the early abort must not release the slots")

	expireReservation(scheduler, key)
	require.NoError(t, scheduler.reapExpiredReservations(t.Context()))
	// The scheduler's own reaper can remove the reservation first. Either reaper counts the timeout
	// after it released the slots.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(scheduler.reservationTimeouts.WithLabelValues("test")) == 1
	}, 5*time.Second, 10*time.Millisecond, "the reservation timeout must be counted")
	require.Zero(t, unusedReservedSlotsForTest(scheduler), "the reservation timeout must release the slots")
}

func TestSchedulerQueueReservation_ReservedEnqueueRefreshesTheDeadline(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}

	// The reservation timeout is an idle timeout, so each sub-request moves the deadline forward.
	// now is later than the deadline that the RESERVE set, so the move is visible. The test does
	// not expire the reservation, because the scheduler's own reaper would race it.
	now := time.Now().Add(time.Hour)
	res, err := scheduler.startReservedEnqueue(key, "test", now)
	require.NoError(t, err)

	scheduler.reservationsMu.Lock()
	defer scheduler.reservationsMu.Unlock()
	require.Equal(t, now.Add(scheduler.cfg.QueueReservationTimeout), res.deadline)
}

func TestSchedulerQueueReservation_ReservedEnqueueBeyondTotalSubRequestsIsRejected(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 1)).Status)

	// Each stream can claim a slot before its sub-request reaches the queue. So the claim check
	// counts in-flight claims, and the second claim must fail.
	key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}
	startReservedEnqueueForTest(t, scheduler, key)
	_, err := scheduler.startReservedEnqueue(key, "test", time.Now())
	require.ErrorIs(t, err, errReservationOverDelivered)
}

func TestSchedulerQueueReservation_ReaperKeepsExpiredReservationWithInflightSubRequest(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}
	res := openReservationForTest(t, scheduler, key)
	startReservedEnqueueForTest(t, scheduler, key)

	// The scheduler's own reaper also skips this reservation, so the test does not race it.
	expireReservation(scheduler, key)
	require.NoError(t, scheduler.reapExpiredReservations(t.Context()))

	require.Same(t, res, openReservationForTest(t, scheduler, key), "the reaper must keep a reservation that has a sub-request in flight")
	require.Equal(t, 2.0, unusedReservedSlotsForTest(scheduler), "the reaper must keep the reservation's slots")
	require.Zero(t, testutil.ToFloat64(scheduler.reservationTimeouts.WithLabelValues("test")))
}

func TestSchedulerQueueReservation_SuccessfulReservedEnqueueMovesTheDeadlineForward(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}
	res := openReservationForTest(t, scheduler, key)
	startReservedEnqueueForTest(t, scheduler, key)

	// The reaper skips a reservation with an in-flight sub-request, so this deadline stays in the
	// past until the sub-request ends.
	scheduler.reservationsMu.Lock()
	res.deadline = time.Now().Add(-time.Second)
	scheduler.reservationsMu.Unlock()

	req := newReservedRequestForTest(100)
	require.NoError(t, scheduler.queue.EnqueueIntoSlot(req, func() { scheduler.addRequestToPending(req) }))
	before := time.Now()
	scheduler.finishReservedEnqueue(res, nil)

	scheduler.reservationsMu.Lock()
	defer scheduler.reservationsMu.Unlock()
	require.False(t, res.deadline.Before(before.Add(scheduler.cfg.QueueReservationTimeout)),
		"a successful reserved ENQUEUE must move the deadline forward from the end of its insert")
}

func TestSchedulerQueueReservation_ExtendDeadlineNeverMovesTheDeadlineBack(t *testing.T) {
	scheduler := &Scheduler{cfg: Config{QueueReservationTimeout: time.Minute}}
	base := time.Now()
	initial := base.Add(scheduler.cfg.QueueReservationTimeout)

	testCases := map[string]struct {
		now              time.Time
		expectedDeadline time.Time
	}{
		"a later now moves the deadline forward": {
			now:              base.Add(time.Second),
			expectedDeadline: base.Add(time.Second).Add(scheduler.cfg.QueueReservationTimeout),
		},
		"an earlier now keeps the deadline": {
			now:              base.Add(-time.Second),
			expectedDeadline: initial,
		},
		"the same now keeps the deadline": {
			now:              base,
			expectedDeadline: initial,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			res := &openReservation{deadline: initial}
			scheduler.reservationsMu.Lock()
			defer scheduler.reservationsMu.Unlock()
			scheduler.extendDeadlineLocked(res, testCase.now)
			require.True(t, testCase.expectedDeadline.Equal(res.deadline), "got %s, want %s", res.deadline, testCase.expectedDeadline)
		})
	}
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

func openReservationForTest(t *testing.T, scheduler *Scheduler, key reservationKey) *openReservation {
	t.Helper()
	scheduler.reservationsMu.Lock()
	defer scheduler.reservationsMu.Unlock()
	res := scheduler.openReservations[key]
	require.NotNil(t, res)
	return res
}

func unusedReservedSlotsForTest(scheduler *Scheduler) float64 {
	return testutil.ToFloat64(scheduler.unusedReservedSlots.WithLabelValues("test"))
}

func newReservedRequestForTest(queryID uint64) *SchedulerRequest {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &SchedulerRequest{FrontendAddr: "frontend-1", UserID: "test", QueryID: queryID, Ctx: ctx, CancelFunc: cancel}
}

func TestSchedulerQueueReservation_CloseDuringInflightSubRequestKeepsSiblingReservation(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-2", "test", 1)).Status)

	keyA := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}
	resA := openReservationForTest(t, scheduler, keyA)

	// Reservation A's sub-request claims a slot. Then an abort closes A before the sub-request
	// reaches the queue.
	startReservedEnqueueForTest(t, scheduler, keyA)
	scheduler.abortReservation(keyA.frontendAddr, keyA.reservationID)
	require.Equal(t, 2.0, unusedReservedSlotsForTest(scheduler), "the close must keep the in-flight sub-request's slot")

	// The in-flight sub-request now reaches the queue and uses its slot.
	req := newReservedRequestForTest(100)
	require.NoError(t, scheduler.queue.EnqueueIntoSlot(req, func() { scheduler.addRequestToPending(req) }))
	scheduler.finishReservedEnqueue(resA, nil)
	require.NoError(t, req.Ctx.Err(), "a sub-request inserted after its reservation closed stays in the queue, as a plain request does")
	_, err := scheduler.startReservedEnqueue(keyA, "test", time.Now())
	require.ErrorIs(t, err, errReservationClosed, "the next sub-request of the closed reservation must be rejected")

	// Reservation B is intact.
	require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, fl, "reservation-2", "test", 200))
	require.Zero(t, unusedReservedSlotsForTest(scheduler))
}

// Each stream of the frontend can have one sub-request of the reservation in flight. This test
// calls startReservedEnqueue and finishReservedEnqueue directly, so it does not need several
// streams.
func TestSchedulerQueueReservation_CloseDuringInflightSubRequests(t *testing.T) {
	testCases := map[string]struct {
		totalSubRequests int
		// finishErrs holds one entry for each in-flight sub-request. A nil entry inserts the
		// sub-request into the queue before it finishes.
		finishErrs                      []error
		expectedReservedSlotsAfterClose float64
		expectedIncomplete              float64
	}{
		// The close keeps the failed sub-request's slot, so only the sub-request's handler can
		// release it.
		"failed in-flight sub-request releases its slot": {
			totalSubRequests:                2,
			finishErrs:                      []error{errors.New("boom")},
			expectedReservedSlotsAfterClose: 1,
			expectedIncomplete:              1,
		},
		// Every sub-request of the reservation is in the queue. So the reservation is not
		// incomplete.
		"last in-flight sub-request reaches the queue": {
			totalSubRequests:                1,
			finishErrs:                      []error{nil},
			expectedReservedSlotsAfterClose: 1,
			expectedIncomplete:              0,
		},
		"two failed in-flight sub-requests count one incomplete reservation": {
			totalSubRequests:                3,
			finishErrs:                      []error{errors.New("boom"), errors.New("boom")},
			expectedReservedSlotsAfterClose: 2,
			expectedIncomplete:              1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			scheduler, frontendClient := setupReservationScheduler(t)
			fl := initFrontendLoop(t, frontendClient, "frontend-1")
			require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", testCase.totalSubRequests)).Status)

			key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}
			res := openReservationForTest(t, scheduler, key)
			for range testCase.finishErrs {
				startReservedEnqueueForTest(t, scheduler, key)
			}
			scheduler.abortReservation(key.frontendAddr, key.reservationID)
			require.Equal(t, testCase.expectedReservedSlotsAfterClose, unusedReservedSlotsForTest(scheduler),
				"the close must keep the slots of the in-flight sub-requests")

			for i, finishErr := range testCase.finishErrs {
				if finishErr == nil {
					req := newReservedRequestForTest(uint64(100 + i))
					require.NoError(t, scheduler.queue.EnqueueIntoSlot(req, func() { scheduler.addRequestToPending(req) }))
				}
				scheduler.finishReservedEnqueue(res, finishErr)
			}
			require.Zero(t, unusedReservedSlotsForTest(scheduler))
			require.Equal(t, testCase.expectedIncomplete, testutil.ToFloat64(scheduler.incompleteReservations.WithLabelValues("test")),
				"the last in-flight sub-request counts the reservation as incomplete, once")
		})
	}
}

func TestSchedulerQueueReservation_FailedSubRequestInOpenReservationClosesItAndReleasesTheRest(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 3)).Status)
	require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, fl, "reservation-1", "test", 10))
	inserted := pendingRequestForTest(t, scheduler, 10)

	key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}
	res := openReservationForTest(t, scheduler, key)
	startReservedEnqueueForTest(t, scheduler, key)
	scheduler.finishReservedEnqueue(res, errors.New("boom"))

	require.Zero(t, unusedReservedSlotsForTest(scheduler), "a failed sub-request must release the reservation's remaining slots")
	require.NoError(t, inserted.Ctx.Err(), "a failed reservation keeps the sub-requests it already inserted")
	require.True(t, isPendingForTest(scheduler, 10))
	require.Equal(t, 1.0, testutil.ToFloat64(scheduler.incompleteReservations.WithLabelValues("test")))
	scheduler.reservationsMu.Lock()
	require.NotContains(t, scheduler.openReservations, key)
	scheduler.reservationsMu.Unlock()
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

func TestSchedulerQueueReservation_StreamCloseKeepsTheReservationWhileTheFrontendHasAnotherStream(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	opener := initFrontendLoop(t, frontendClient, "frontend-1")
	other := initFrontendLoop(t, frontendClient, "frontend-1")

	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, opener, reserveMessage("reservation-1", "test", 2)).Status)
	require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, opener, "reservation-1", "test", 10))
	inserted := pendingRequestForTest(t, scheduler, 10)

	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](opener))
	test.Poll(t, 5*time.Second, float64(1), func() interface{} {
		return testutil.ToFloat64(scheduler.connectedFrontendClients)
	})
	require.Equal(t, 1.0, unusedReservedSlotsForTest(scheduler), "the reservation belongs to the frontend, so one closed stream releases nothing")
	require.NoError(t, inserted.Ctx.Err())

	require.Equal(t, schedulerpb.OK, sendReservedEnqueues(t, other, "reservation-1", "test", 11))
	require.Zero(t, unusedReservedSlotsForTest(scheduler))
	require.True(t, isPendingForTest(scheduler, 11))
}

func TestSchedulerQueueReservation_CleanupKeepsReservedSlotsWhileReservationIsOpen(t *testing.T) {
	scheduler, frontendClient := setupReservationScheduler(t)
	fl := initFrontendLoop(t, frontendClient, "frontend-1")
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, reserveMessage("reservation-1", "test", 2)).Status)

	scheduler.cleanupMetricsForInactiveUser("test")

	// The reservation still holds 2 slots. A deleted gauge would restart at 0 and go negative on
	// release.
	require.Equal(t, 2.0, unusedReservedSlotsForTest(scheduler))
	require.Equal(t, schedulerpb.OK, sendFrontendMessage(t, fl, &schedulerpb.FrontendToScheduler{Type: schedulerpb.CANCEL, ReservationID: "reservation-1"}).Status)
	require.Zero(t, unusedReservedSlotsForTest(scheduler))
}

// setupReservationSchedulerWithFastCleanup returns a started scheduler whose inactive-user cleanup
// runs every 10ms and purges a tenant after 100ms of inactivity. Its reservations outlive that
// limit. It returns after the cleanup purged tenant "test", which holds the open reservation
// "reservation-1" for 2 sub-requests. A test holds the returned mutex to stop the cleanup while it
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
	// The rejected RESERVE creates a discardedReservations counter that the cleanup deletes, so the
	// test can see the cleanup.
	require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, scheduler.createReservation("frontend-1", reserveMessage("reservation-2", "test", testMaxOutstandingPerTenant)).Status)
	require.Eventually(t, func() bool {
		return testutil.CollectAndCount(scheduler.discardedReservations) == 0
	}, 5*time.Second, 10*time.Millisecond, "the cleanup must purge the inactive tenant")
	return scheduler, pause, cleanups
}

// expireReservation moves the deadline of the reservation at key into the past. The scheduler's own
// reaper can then close it before the test calls reapExpiredReservations, unless a sub-request is
// in flight. So a test must not assume that its own reap closes the reservation. It waits for the
// timeout counter instead.
func expireReservation(scheduler *Scheduler, key reservationKey) {
	scheduler.reservationsMu.Lock()
	defer scheduler.reservationsMu.Unlock()
	scheduler.openReservations[key].deadline = time.Now().Add(-time.Second)
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
		return testutil.CollectAndCount(scheduler.reservationTimeouts) == 0 &&
			testutil.CollectAndCount(scheduler.incompleteReservations) == 0
	}, 5*time.Second, 10*time.Millisecond, "a later cleanup must delete the counters that the reservation created")
}

func TestSchedulerQueueReservation_TimeoutAfterTenantCleanupKeepsTenantActive(t *testing.T) {
	scheduler, pause, _ := setupReservationSchedulerWithFastCleanup(t)

	requireCountersWhilePaused(pause, func() {
		expireReservation(scheduler, reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"})
		require.NoError(t, scheduler.reapExpiredReservations(t.Context()))
		// The scheduler's own reaper can win the race, so the test waits for either.
		require.Eventually(t, func() bool {
			return testutil.CollectAndCount(scheduler.reservationTimeouts) == 1
		}, 5*time.Second, 10*time.Millisecond)
	})
	requireReservationCountersDeletedEventually(t, scheduler)
}

func TestSchedulerQueueReservation_AbortAfterTenantCleanupKeepsTenantActive(t *testing.T) {
	scheduler, pause, _ := setupReservationSchedulerWithFastCleanup(t)

	requireCountersWhilePaused(pause, func() {
		scheduler.abortReservation("frontend-1", "reservation-1")
		require.Equal(t, 1, testutil.CollectAndCount(scheduler.incompleteReservations))
	})
	requireReservationCountersDeletedEventually(t, scheduler)
}

func TestSchedulerQueueReservation_FailedSubRequestAfterTenantCleanupKeepsTenantActive(t *testing.T) {
	scheduler, pause, _ := setupReservationSchedulerWithFastCleanup(t)
	key := reservationKey{frontendAddr: "frontend-1", reservationID: "reservation-1"}
	res := openReservationForTest(t, scheduler, key)
	startReservedEnqueueForTest(t, scheduler, key)

	requireCountersWhilePaused(pause, func() {
		scheduler.finishReservedEnqueue(res, errors.New("boom"))
		require.Equal(t, 1, testutil.CollectAndCount(scheduler.incompleteReservations))
	})
	requireReservationCountersDeletedEventually(t, scheduler)
}

func TestSchedulerQueueReservation_ActivityAfterTenantCleanupMarksTenantActive(t *testing.T) {
	testCases := map[string]struct {
		activity func(t *testing.T, scheduler *Scheduler)
	}{
		// Both sub-requests arrive, so no incomplete count marks the tenant active. Only the
		// successFn of each insert does.
		"sub-request insert": {
			activity: func(t *testing.T, scheduler *Scheduler) {
				for _, queryID := range []uint64{10, 11} {
					require.NoError(t, scheduler.enqueueRequest(context.Background(), "frontend-1", reservedEnqueue("reservation-1", "test", queryID)))
				}
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
