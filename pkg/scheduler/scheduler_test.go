// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/scheduler_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package scheduler

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/scheduler/queue"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	util_test "github.com/grafana/mimir/pkg/util/test"
)

const testMaxOutstandingPerTenant = 5

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

func setupScheduler(t *testing.T, reg prometheus.Registerer) (*Scheduler, schedulerpb.SchedulerForFrontendClient, schedulerpb.SchedulerForQuerierClient) {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = testMaxOutstandingPerTenant

	s, err := NewScheduler(cfg, &limits{queriers: 2}, log.NewNopLogger(), reg, "")
	require.NoError(t, err)

	server := grpc.NewServer()
	schedulerpb.RegisterSchedulerForFrontendServer(server, s)
	schedulerpb.RegisterSchedulerForQuerierServer(server, s)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), s))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), s)
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

func TestSchedulerBasicEnqueue(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:         schedulerpb.ENQUEUE,
		QueryID:      1,
		UserID:       "test",
		HttpRequest:  &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		StatsEnabled: true,
	})

	{
		querierLoop, err := querierClient.QuerierLoop(context.Background())
		require.NoError(t, err)
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

		msg2, err := querierLoop.Recv()
		require.NoError(t, err)
		require.Equal(t, uint64(1), msg2.QueryID)
		require.Equal(t, "frontend-12345", msg2.FrontendAddress)
		require.Equal(t, "GET", msg2.HttpRequest.Method)
		require.Equal(t, "/hello", msg2.HttpRequest.Url)
		require.True(t, msg2.StatsEnabled)
		require.Greater(t, msg2.QueueTimeNanos, int64(0))
		require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{}))
	}

	verifyNoPendingRequestsLeft(t, scheduler)
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerEnqueueWithCancel(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
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
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello1"},
	})

	frontendToScheduler(t, frontendLoop2, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello2"},
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
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Wait until the frontend has connected to the scheduler.
	test.Poll(t, time.Second, float64(1), func() interface{} {
		return promtest.ToFloat64(scheduler.connectedFrontendClients)
	})

	// Disconnect frontend.
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToFrontend](frontendLoop))

	// Wait until the frontend has disconnected.
	test.Poll(t, time.Second, float64(0), func() interface{} {
		return promtest.ToFloat64(scheduler.connectedFrontendClients)
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
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
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

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
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

	closer, err := config.Configuration{}.InitGlobalTracer("test")
	require.NoError(t, err)
	defer closer.Close()

	req := &schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.ENQUEUE,
		QueryID:         1,
		UserID:          "test",
		HttpRequest:     &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
		FrontendAddress: "frontend-12345",
	}

	sp, _ := opentracing.StartSpanFromContext(context.Background(), "client")
	opentracing.GlobalTracer().Inject(sp.Context(), opentracing.HTTPHeaders, (*httpgrpcutil.HttpgrpcHeadersCarrier)(req.HttpRequest))

	frontendToScheduler(t, frontendLoop, req)

	scheduler.inflightRequestsMu.Lock()
	defer scheduler.inflightRequestsMu.Unlock()
	require.Equal(t, 1, len(scheduler.schedulerInflightRequests))

	for _, r := range scheduler.schedulerInflightRequests {
		require.NotNil(t, r.ParentSpanContext)
	}
}

func TestSchedulerShutdown_FrontendLoop(t *testing.T) {
	scheduler, frontendClient, _ := setupScheduler(t, nil)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")

	// Stop the scheduler. This will disable receiving new requests from frontends.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))

	// We can still send request to scheduler, but we get shutdown error back.
	require.NoError(t, frontendLoop.Send(&schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
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
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Scheduler now has 1 query. Let's connect querier and fetch it.

	querierLoop, err := querierClient.QuerierLoop(context.Background())
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	// Dequeue first query.
	_, err = querierLoop.Recv()
	require.NoError(t, err)

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), scheduler))

	// Unblock scheduler loop, to find next request.
	err = querierLoop.Send(&schedulerpb.QuerierToScheduler{})
	require.NoError(t, err)

	// This should now return with error, since scheduler is going down.
	_, err = querierLoop.Recv()
	require.Error(t, err)
}

func TestSchedulerMaxOutstandingRequests(t *testing.T) {
	_, frontendClient, _ := setupScheduler(t, nil)

	for i := 0; i < testMaxOutstandingPerTenant; i++ {
		// coming from different frontends
		fl := initFrontendLoop(t, frontendClient, fmt.Sprintf("frontend-%d", i))
		require.NoError(t, fl.Send(&schedulerpb.FrontendToScheduler{
			Type:        schedulerpb.ENQUEUE,
			QueryID:     uint64(i),
			UserID:      "test", // for same user.
			HttpRequest: &httpgrpc.HTTPRequest{},
		}))

		msg, err := fl.Recv()
		require.NoError(t, err)
		require.Equal(t, schedulerpb.OK, msg.Status)
	}

	// One more query from the same user will trigger an error.
	fl := initFrontendLoop(t, frontendClient, "extra-frontend")

	req := schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     0,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	}

	// Inject span context to the request so we can check handling of max outstanding requests.
	mockTracer := mocktracer.New()
	opentracing.SetGlobalTracer(mockTracer)
	sp, _ := opentracing.StartSpanFromContextWithTracer(context.Background(), mockTracer, "client")
	require.NoError(t, mockTracer.Inject(sp.Context(), opentracing.HTTPHeaders, (*httpgrpcutil.HttpgrpcHeadersCarrier)(req.HttpRequest)))

	require.NoError(t, fl.Send(&req))

	msg, err := fl.Recv()
	require.NoError(t, err)
	require.Equal(t, schedulerpb.TOO_MANY_REQUESTS_PER_TENANT, msg.Status)

	spans := mockTracer.FinishedSpans()
	require.Greater(t, len(spans), 0, "expected at least one span even if rejected by queue full")
}

func TestSchedulerForwardsErrorToFrontend(t *testing.T) {
	scheduler, frontendClient, querierClient := setupScheduler(t, nil)

	fm := &frontendMock{resp: map[uint64]*httpgrpc.HTTPResponse{}}
	frontendAddress := ""

	// Setup frontend grpc server
	{
		frontendGrpcServer := grpc.NewServer()
		frontendv2pb.RegisterFrontendForQuerierServer(frontendGrpcServer, fm)

		l, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)

		frontendAddress = l.Addr().String()

		go func() {
			_ = frontendGrpcServer.Serve(l)
		}()

		t.Cleanup(func() {
			_ = l.Close()
		})
	}

	// After preparations, start frontend and querier.
	frontendLoop := initFrontendLoop(t, frontendClient, frontendAddress)
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     100,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	// Scheduler now has 1 query. We now connect querier, fetch the request, and then close the connection.
	// This will make scheduler to report error back to frontend.

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
		resp := fm.getRequest(100)
		if resp == nil {
			return false
		}

		require.Equal(t, int32(http.StatusInternalServerError), resp.Code)
		return true
	})
	verifyQueryComponentUtilizationLeft(t, scheduler)
}

func TestSchedulerQueueMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()

	scheduler, frontendClient, _ := setupScheduler(t, reg)

	frontendLoop := initFrontendLoop(t, frontendClient, "frontend-12345")
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "test",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})
	frontendToScheduler(t, frontendLoop, &schedulerpb.FrontendToScheduler{
		Type:        schedulerpb.ENQUEUE,
		QueryID:     1,
		UserID:      "another",
		HttpRequest: &httpgrpc.HTTPRequest{Method: "GET", Url: "/hello"},
	})

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{user="another"} 1
		cortex_query_scheduler_queue_length{user="test"} 1
	`), "cortex_query_scheduler_queue_length"))

	scheduler.cleanupMetricsForInactiveUser("test")

	require.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_scheduler_queue_length Number of queries in the queue.
		# TYPE cortex_query_scheduler_queue_length gauge
		cortex_query_scheduler_queue_length{user="another"} 1
	`), "cortex_query_scheduler_queue_length"))
}

func TestSchedulerQuerierMetrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	_, _, querierClient := setupScheduler(t, reg)

	ctx, cancel := context.WithCancel(context.Background())
	querierLoop, err := querierClient.QuerierLoop(ctx)
	require.NoError(t, err)
	require.NoError(t, querierLoop.Send(&schedulerpb.QuerierToScheduler{QuerierID: "querier-1"}))

	require.Eventually(t, func() bool {
		err := promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_query_scheduler_connected_querier_clients Number of querier worker clients currently connected to the query-scheduler.
			# TYPE cortex_query_scheduler_connected_querier_clients gauge
			cortex_query_scheduler_connected_querier_clients 1
		`), "cortex_query_scheduler_connected_querier_clients")

		return err == nil
	}, time.Second, 10*time.Millisecond, "expected cortex_query_scheduler_connected_querier_clients metric to be incremented after querier connected")

	cancel()
	require.NoError(t, util.CloseAndExhaust[*schedulerpb.SchedulerToQuerier](querierLoop))

	require.Eventually(t, func() bool {
		err := promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_query_scheduler_connected_querier_clients Number of querier worker clients currently connected to the query-scheduler.
			# TYPE cortex_query_scheduler_connected_querier_clients gauge
			cortex_query_scheduler_connected_querier_clients 0
		`), "cortex_query_scheduler_connected_querier_clients")

		return err == nil
	}, time.Second, 10*time.Millisecond, "expected cortex_query_scheduler_connected_querier_clients metric to be decremented after querier disconnected")
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
	mu   sync.Mutex
	resp map[uint64]*httpgrpc.HTTPResponse
}

func (f *frontendMock) QueryResult(_ context.Context, request *frontendv2pb.QueryResultRequest) (*frontendv2pb.QueryResultResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.resp[request.QueryID] = request.HttpResponse
	return &frontendv2pb.QueryResultResponse{}, nil
}

// satisfy frontendv2pb.FrontendForQuerierServer interface
func (f *frontendMock) QueryResultStream(_ frontendv2pb.FrontendForQuerier_QueryResultStreamServer) error {
	panic("unexpected call to QueryResultStream")
}

func (f *frontendMock) getRequest(queryID uint64) *httpgrpc.HTTPResponse {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.resp[queryID]
}
