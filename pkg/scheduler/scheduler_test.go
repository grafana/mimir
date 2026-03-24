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
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/cancellation"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb" //lint:ignore faillint we can't avoid using this given that's where the Protobuf definition lives
	"github.com/grafana/mimir/pkg/scheduler/queue"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/promtest"
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

func setupScheduler(t *testing.T, reg prometheus.Registerer) (*Scheduler, schedulerpb.SchedulerForFrontendClient, schedulerpb.SchedulerForQuerierClient) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = testMaxOutstandingPerTenant

	s, err := NewScheduler(cfg, &limits{queriers: 2}, log.NewNopLogger(), reg)
	require.NoError(t, err)

	server := grpc.NewServer()
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

	querierWorkerConn := queue.NewUnregisteredQuerierWorkerConn(context.Background(), querierID)
	require.NoError(t, s.requestQueue.AwaitRegisterQuerierWorkerConn(querierWorkerConn))
	defer s.requestQueue.SubmitUnregisterQuerierWorkerConn(querierWorkerConn)

	consumer := func(request queue.QueryRequest) error {
		return nil
	}

	for {
		if s.requestQueue.IsEmpty() {
			return
		}

		idx, err := queueConsume(s.requestQueue, querierWorkerConn, lastTenantIndex, consumer)
		require.NoError(t, err)
		lastTenantIndex = idx
	}
}

type consumeRequest func(request queue.QueryRequest) error

func queueConsume(
	q *queue.RequestQueue, querierWorkerConn *queue.QuerierWorkerConn, lastTenantIdx queue.TenantIndex, consumeFunc consumeRequest,
) (queue.TenantIndex, error) {
	dequeueReq := queue.NewQuerierWorkerDequeueRequest(querierWorkerConn, lastTenantIdx)
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
	require.Equal(t, true, scheduler.requestQueue.IsEmpty())

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

	req := schedulerpb.FrontendToScheduler{
		Type:    schedulerpb.ENQUEUE,
		QueryID: 0,
		UserID:  "test",
		Payload: &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"}},
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

	require.NoError(t, promtest.HasNativeHistogram(reg, "cortex_query_scheduler_queue_duration_seconds"))
	require.NoError(t, promtest.HasSampleCount(reg, "cortex_query_scheduler_queue_duration_seconds", 1))
}

func initFrontendLoop(t *testing.T, client schedulerpb.SchedulerForFrontendClient, frontendAddr string) schedulerpb.SchedulerForFrontend_FrontendLoopClient {
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

	return loop
}

func initQuerierLoop(t *testing.T, querierClient schedulerpb.SchedulerForQuerierClient, querier string) schedulerpb.SchedulerForQuerier_QuerierLoopClient {
	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: querier}))

	return querierLoop
}
func frontendToScheduler(t *testing.T, frontendLoop schedulerpb.SchedulerForFrontend_FrontendLoopClient, req *schedulerpb.FrontendToScheduler) {
	require.NoError(t, frontendLoop.Send(req))
	msg, err := frontendLoop.Recv()
	require.NoError(t, err)
	require.Equal(t, schedulerpb.OK, msg.Status)
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
	require.Zero(t, scheduler.requestQueue.QueryComponentUtilization.GetForComponent(queue.Ingester))
	require.Zero(t, scheduler.requestQueue.QueryComponentUtilization.GetForComponent(queue.StoreGateway))
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
