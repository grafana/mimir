// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
)

const reservationTestUserID = "test"

// closeStreamReply makes mockScheduler end the stream instead of replying.
var closeStreamReply = &schedulerpb.SchedulerToFrontend{Error: "close stream"}

type reservationTestScheduler struct {
	*mockScheduler
	addr string
}

// newReservationTestScheduler returns a scheduler that replies to RESERVE with reserveReply and
// to every ENQUEUE with OK. A nil reserveReply replies OK.
func newReservationTestScheduler(t *testing.T, supported bool, reserveReply func(msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend) *reservationTestScheduler {
	ms := newMockScheduler(t, nil, func(_ *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type == schedulerpb.RESERVE && reserveReply != nil {
			return reserveReply(msg)
		}
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})
	ms.queueReservationSupported = supported
	return &reservationTestScheduler{mockScheduler: ms}
}

func (s *reservationTestScheduler) messages(typ schedulerpb.FrontendToSchedulerType) []*schedulerpb.FrontendToScheduler {
	s.mu.Lock()
	defer s.mu.Unlock()

	var msgs []*schedulerpb.FrontendToScheduler
	for _, m := range s.msgs {
		if m.Type == typ {
			msgs = append(msgs, m)
		}
	}
	return msgs
}

func (s *reservationTestScheduler) completes() []string {
	var ids []string
	for _, m := range s.messages(schedulerpb.COMPLETE_RESERVATION) {
		ids = append(ids, m.ReservationID)
	}
	return ids
}

func (s *reservationTestScheduler) aborts() []string {
	var ids []string
	for _, m := range s.messages(schedulerpb.CANCEL) {
		if m.QueryID == 0 {
			ids = append(ids, m.ReservationID)
		}
	}
	return ids
}

// setupReservationFrontend starts a frontend connected to every scheduler, with queue reservation
// enabled.
func setupReservationFrontend(t *testing.T, reg prometheus.Registerer, mutateCfg func(*Config), schedulers ...*reservationTestScheduler) *Frontend {
	frontendListener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	frontendServer := grpc.NewServer(
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
	)
	frontendHost, frontendPort, err := net.SplitHostPort(frontendListener.Addr().String())
	require.NoError(t, err)
	grpcPort, err := strconv.Atoi(frontendPort)
	require.NoError(t, err)

	listeners := make([]net.Listener, 0, len(schedulers))
	for _, s := range schedulers {
		l, err := net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		s.addr = l.Addr().String()
		listeners = append(listeners, l)
	}

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.SchedulerAddress = schedulers[0].addr
	cfg.WorkerConcurrency = testFrontendWorkerConcurrency
	cfg.Addr = frontendHost
	cfg.Port = grpcPort
	cfg.QueryStoreAfter = 12 * time.Hour
	cfg.QueueReservationEnabled = true
	if mutateCfg != nil {
		mutateCfg(&cfg)
	}

	f, err := NewFrontend(cfg, limits{queryIngestersWithin: 13 * time.Hour}, log.NewLogfmtLogger(os.Stdout), reg, newTestCodec())
	require.NoError(t, err)

	frontendv2pb.RegisterFrontendForQuerierServer(frontendServer, f)
	go func() {
		_ = frontendServer.Serve(frontendListener)
	}()
	t.Cleanup(func() {
		_ = frontendListener.Close()
		frontendServer.GracefulStop()
	})

	for i, s := range schedulers {
		s.f = f
		server := grpc.NewServer()
		schedulerpb.RegisterSchedulerForFrontendServer(server, s)
		l := listeners[i]
		go func() {
			_ = server.Serve(l)
		}()
		t.Cleanup(func() {
			_ = l.Close()
			server.GracefulStop()
		})
	}

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), f))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), f)
	})
	for _, s := range schedulers[1:] {
		f.schedulerWorkers.addScheduler(s.addr)
	}

	supported := 0
	for _, s := range schedulers {
		if s.queueReservationSupported {
			supported++
		}
		test.Poll(t, time.Second, cfg.WorkerConcurrency, func() interface{} {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.frontendAddr[net.JoinHostPort(cfg.Addr, strconv.Itoa(cfg.Port))]
		})
	}
	test.Poll(t, time.Second, supported*cfg.WorkerConcurrency, func() interface{} {
		streams := f.schedulerWorkers.reservationStreams
		streams.mu.Lock()
		defer streams.mu.Unlock()
		return streams.count
	})

	return f
}

// newReservationTestContext returns the context of one evaluation and its scope. The parallelism
// limit holds every sub-request above maxParallelism until an earlier one finishes.
func newReservationTestContext(t *testing.T, maxParallelism int) (context.Context, *querymiddleware.RemoteExecutionReservationScope) {
	ctx := user.InjectOrgID(t.Context(), reservationTestUserID)
	ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(maxParallelism))
	scope := querymiddleware.NewRemoteExecutionReservationScope()
	return querymiddleware.ContextWithRemoteExecutionReservationScope(ctx, scope), scope
}

// doGroupRequests registers n groups in the scope of ctx, then sends one sub-request per group,
// as an MQE evaluation does.
func doGroupRequests(t *testing.T, f *Frontend, ctx context.Context, n int) []ResponseStream {
	scope := querymiddleware.RemoteExecutionReservationScopeFromContext(ctx)
	groups := make([]*querymiddleware.RemoteExecutionReservationMember, n)
	for i := range groups {
		var err error
		groups[i], err = scope.Register()
		require.NoError(t, err)
	}

	streams := make([]ResponseStream, 0, n)
	for _, g := range groups {
		resp, err := f.DoProtobufRequest(querymiddleware.ContextWithRemoteExecutionReservationMember(ctx, g), &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
		require.NoError(t, err)
		t.Cleanup(resp.Close)
		streams = append(streams, resp)
	}
	return streams
}

func requireStreamsSucceed(t *testing.T, f *Frontend, s *reservationTestScheduler, streams []ResponseStream) {
	ctx := user.InjectOrgID(t.Context(), reservationTestUserID)
	for _, m := range s.messages(schedulerpb.ENQUEUE) {
		go sendStreamingResponse(t, f, reservationTestUserID, m.QueryID, newStringMessage("result"))
	}
	for _, resp := range streams {
		msg, err := resp.Next(ctx)
		require.NoError(t, err)
		msg.FreeBuffer()
	}
}

func requireStreamError(t *testing.T, expected error, streams []ResponseStream) {
	ctx := user.InjectOrgID(t.Context(), reservationTestUserID)
	for _, resp := range streams {
		msg, err := resp.Next(ctx)
		require.Equal(t, expected, err)
		require.Nil(t, msg)
	}
}

// totalMessages returns the number of messages of type typ that all schedulers received.
func totalMessages(typ schedulerpb.FrontendToSchedulerType, schedulers ...*reservationTestScheduler) int {
	total := 0
	for _, s := range schedulers {
		total += len(s.messages(typ))
	}
	return total
}

func TestQueueReservation_AllSubRequestsGoToOneScheduler(t *testing.T) {
	const subRequests = 3

	reg := prometheus.NewPedanticRegistry()
	first := newReservationTestScheduler(t, true, nil)
	second := newReservationTestScheduler(t, true, nil)
	f := setupReservationFrontend(t, reg, nil, first, second)

	ctx, scope := newReservationTestContext(t, subRequests)
	streams := doGroupRequests(t, f, ctx, subRequests)

	test.Poll(t, time.Second, subRequests, func() interface{} {
		return totalMessages(schedulerpb.ENQUEUE, first, second)
	})

	require.Equal(t, 1, totalMessages(schedulerpb.RESERVE, first, second))
	chosen, unused := first, second
	if len(second.messages(schedulerpb.RESERVE)) == 1 {
		chosen, unused = second, first
	}

	reserves := chosen.messages(schedulerpb.RESERVE)
	require.Len(t, reserves, 1)
	require.Equal(t, uint64(subRequests), reserves[0].Slots)
	require.Equal(t, reservationTestUserID, reserves[0].UserID)
	require.NotEmpty(t, reserves[0].ReservationID)
	for _, m := range chosen.messages(schedulerpb.ENQUEUE) {
		require.Equal(t, reserves[0].ReservationID, m.ReservationID)
		require.Equal(t, reservationTestUserID, m.UserID)
	}

	requireStreamsSucceed(t, f, chosen, streams)
	scope.Close(false)

	test.Poll(t, time.Second, []string{reserves[0].ReservationID}, func() interface{} {
		return chosen.completes()
	})
	require.Empty(t, chosen.aborts())
	require.Empty(t, unused.messages(schedulerpb.RESERVE))
	require.Empty(t, unused.messages(schedulerpb.ENQUEUE))
	require.Empty(t, unused.messages(schedulerpb.COMPLETE_RESERVATION))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_frontend_queue_reservations_total Total number of queue reservations that the query-frontend tried to open on a query-scheduler, by result.
		# TYPE cortex_query_frontend_queue_reservations_total counter
		cortex_query_frontend_queue_reservations_total{result="ok"} 1
	`), "cortex_query_frontend_queue_reservations_total"))
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"enqueued": subRequests})
}

func TestQueueReservation_SlotsAreTheSmallerOfParallelismAndSubRequests(t *testing.T) {
	tests := map[string]struct {
		maxParallelism int
		subRequests    int
		expectedSlots  int
	}{
		"parallelism below the sub-request count":    {maxParallelism: 2, subRequests: 3, expectedSlots: 2},
		"parallelism equal to the sub-request count": {maxParallelism: 3, subRequests: 3, expectedSlots: 3},
		"parallelism above the sub-request count":    {maxParallelism: 5, subRequests: 3, expectedSlots: 3},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := newReservationTestScheduler(t, true, nil)
			f := setupReservationFrontend(t, nil, nil, s)

			ctx, scope := newReservationTestContext(t, tc.maxParallelism)
			defer scope.Close(false)
			streams := doGroupRequests(t, f, ctx, tc.subRequests)

			// The parallelism limit holds the other sub-requests until a result comes, so the
			// queue never has more sub-requests of the reservation than it has slots.
			test.Poll(t, time.Second, tc.expectedSlots, func() interface{} {
				return len(s.messages(schedulerpb.ENQUEUE))
			})
			reserves := s.messages(schedulerpb.RESERVE)
			require.Len(t, reserves, 1)
			require.Equal(t, uint64(tc.expectedSlots), reserves[0].Slots)

			results := make(chan error, len(streams))
			for _, resp := range streams {
				go func() {
					msg, err := resp.Next(ctx)
					if err == nil {
						msg.FreeBuffer()
					}
					results <- err
				}()
			}

			for finished := range tc.subRequests {
				test.Poll(t, time.Second, true, func() interface{} {
					return len(s.messages(schedulerpb.ENQUEUE)) > finished
				})
				require.LessOrEqual(t, len(s.messages(schedulerpb.ENQUEUE))-finished, tc.expectedSlots)
				m := s.messages(schedulerpb.ENQUEUE)[finished]
				require.Equal(t, reserves[0].ReservationID, m.ReservationID)
				go sendStreamingResponse(t, f, reservationTestUserID, m.QueryID, newStringMessage("result"))
				require.NoError(t, <-results)
			}
			require.Len(t, s.messages(schedulerpb.ENQUEUE), tc.subRequests)
		})
	}
}

func TestQueueReservation_PlainPath(t *testing.T) {
	tests := map[string]struct {
		enabled   bool
		supported bool
	}{
		"queue reservation disabled on the frontend": {enabled: false, supported: true},
		"scheduler without queue reservation":        {enabled: true, supported: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			s := newReservationTestScheduler(t, tc.supported, nil)
			f := setupReservationFrontend(t, nil, func(cfg *Config) { cfg.QueueReservationEnabled = tc.enabled }, s)

			ctx, scope := newReservationTestContext(t, 1)
			streams := doGroupRequests(t, f, ctx, 1)
			test.Poll(t, time.Second, 1, func() interface{} {
				return len(s.messages(schedulerpb.ENQUEUE))
			})
			requireStreamsSucceed(t, f, s, streams)
			scope.Close(false)

			require.Empty(t, s.messages(schedulerpb.RESERVE))
			require.Empty(t, s.messages(schedulerpb.ENQUEUE)[0].ReservationID)
		})
	}
}

func TestQueueReservation_MixedSchedulersDuringRollout(t *testing.T) {
	const (
		reservedSubRequests = 3
		plainRequests       = 10
	)

	upgraded := newReservationTestScheduler(t, true, nil)
	notUpgraded := newReservationTestScheduler(t, false, nil)
	f := setupReservationFrontend(t, nil, nil, upgraded, notUpgraded)

	ctx, scope := newReservationTestContext(t, reservedSubRequests)
	defer scope.Close(false)
	doGroupRequests(t, f, ctx, reservedSubRequests)

	plainCtx := user.InjectOrgID(t.Context(), reservationTestUserID)
	plainCtx = querymiddleware.ContextWithParallelismLimiter(plainCtx, querymiddleware.NewParallelismLimiter(plainRequests))
	for range plainRequests {
		resp, err := f.DoProtobufRequest(plainCtx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
		require.NoError(t, err)
		t.Cleanup(resp.Close)
	}

	test.Poll(t, time.Second, reservedSubRequests+plainRequests, func() interface{} {
		return totalMessages(schedulerpb.ENQUEUE, upgraded, notUpgraded)
	})

	require.Empty(t, notUpgraded.messages(schedulerpb.RESERVE))
	for _, m := range notUpgraded.messages(schedulerpb.ENQUEUE) {
		require.Empty(t, m.ReservationID, "a query-scheduler without queue reservation must get only plain ENQUEUEs")
	}

	reserves := upgraded.messages(schedulerpb.RESERVE)
	require.Len(t, reserves, 1)
	require.Equal(t, uint64(reservedSubRequests), reserves[0].Slots)
	reserved := 0
	for _, m := range upgraded.messages(schedulerpb.ENQUEUE) {
		if m.ReservationID != "" {
			require.Equal(t, reserves[0].ReservationID, m.ReservationID)
			reserved++
		}
	}
	require.Equal(t, reservedSubRequests, reserved)
}

func TestQueueReservation_SchedulerUpgradedWhileFrontendRuns(t *testing.T) {
	var upgrade atomic.Bool
	s := newReservationTestScheduler(t, false, nil)
	s.replyFunc = func(_ *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type == schedulerpb.ENQUEUE && upgrade.CompareAndSwap(true, false) {
			// The query-scheduler restarts with queue reservation, so the stream reconnects and
			// gets the new INIT reply.
			s.mu.Lock()
			s.queueReservationSupported = true
			s.mu.Unlock()
			return closeStreamReply
		}
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}
	f := setupReservationFrontend(t, nil, func(cfg *Config) { cfg.WorkerConcurrency = 1 }, s)

	before, beforeScope := newReservationTestContext(t, 1)
	defer beforeScope.Close(false)
	doGroupRequests(t, f, before, 1)
	test.Poll(t, time.Second, 1, func() interface{} {
		return len(s.messages(schedulerpb.ENQUEUE))
	})
	require.Empty(t, s.messages(schedulerpb.RESERVE))
	require.Empty(t, s.messages(schedulerpb.ENQUEUE)[0].ReservationID)

	// The plain request that triggers the restart fails on the old stream, and the frontend
	// sends it again on the new stream.
	upgrade.Store(true)
	triggerCtx := querymiddleware.ContextWithParallelismLimiter(user.InjectOrgID(t.Context(), reservationTestUserID), querymiddleware.NewParallelismLimiter(1))
	trigger, err := f.DoProtobufRequest(triggerCtx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
	require.NoError(t, err)
	t.Cleanup(trigger.Close)
	test.Poll(t, time.Second, 1, func() interface{} {
		streams := f.schedulerWorkers.reservationStreams
		streams.mu.Lock()
		defer streams.mu.Unlock()
		return streams.count
	})

	after, afterScope := newReservationTestContext(t, 2)
	defer afterScope.Close(false)
	doGroupRequests(t, f, after, 2)
	test.Poll(t, time.Second, 2, func() interface{} {
		reserved := 0
		for _, m := range s.messages(schedulerpb.ENQUEUE) {
			if m.ReservationID != "" {
				reserved++
			}
		}
		return reserved
	})

	reserves := s.messages(schedulerpb.RESERVE)
	require.Len(t, reserves, 1)
	require.Equal(t, uint64(2), reserves[0].Slots)
}

func TestQueueReservation_BrokenStreamAbortsLostReservation(t *testing.T) {
	s := newReservationTestScheduler(t, true, func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		return closeStreamReply
	})
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, s)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)
	requireStreamError(t, apierror.New(apierror.TypeInternal, "failed to enqueue request"), doGroupRequests(t, f, ctx, 1))
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"reservation_lost": 1})

	// The frontend tries the RESERVE once per attempt, each time with a new reservation ID. The
	// scheduler can have granted each one before its stream broke, so the frontend aborts each one.
	lost := s.messages(schedulerpb.RESERVE)
	require.Len(t, lost, testFrontendWorkerConcurrency+1)
	var lostIDs []string
	for _, m := range lost {
		lostIDs = append(lostIDs, m.ReservationID)
	}
	require.Len(t, slices.Compact(slices.Sorted(slices.Values(lostIDs))), len(lostIDs), "each attempt must use a new reservation ID")
	test.Poll(t, 5*time.Second, slices.Sorted(slices.Values(lostIDs)), func() interface{} {
		return slices.Sorted(slices.Values(s.aborts()))
	})
	require.Empty(t, s.messages(schedulerpb.ENQUEUE))
}

func TestQueueReservation_RetriesReserveAfterTransportError(t *testing.T) {
	var reserves atomic.Int64
	s := newReservationTestScheduler(t, true, func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if reserves.Inc() == 1 {
			return closeStreamReply
		}
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	})
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, s)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)
	doGroupRequests(t, f, ctx, 1)
	test.Poll(t, 5*time.Second, 1, func() interface{} {
		return len(s.messages(schedulerpb.ENQUEUE))
	})

	attempts := s.messages(schedulerpb.RESERVE)
	require.Len(t, attempts, 2)
	require.NotEqual(t, attempts[0].ReservationID, attempts[1].ReservationID, "the retry must use a new reservation ID")
	require.Equal(t, attempts[1].ReservationID, s.messages(schedulerpb.ENQUEUE)[0].ReservationID, "the sub-request must use the granted reservation")
	test.Poll(t, time.Second, []string{attempts[0].ReservationID}, func() interface{} {
		return s.aborts()
	})
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"enqueued": 1})
}

func TestQueueReservation_FinalStatus(t *testing.T) {
	tests := map[string]struct {
		status          schedulerpb.SchedulerToFrontendStatus
		expectedErr     error
		expectedOutcome string
	}{
		"too many requests": {
			status:          schedulerpb.TOO_MANY_REQUESTS_PER_TENANT,
			expectedErr:     apierror.New(apierror.TypeTooManyRequests, "too many outstanding requests"),
			expectedOutcome: "too_many_requests",
		},
		"shutting down": {
			status:          schedulerpb.SHUTTING_DOWN,
			expectedErr:     apierror.New(apierror.TypeInternal, "failed to enqueue request"),
			expectedOutcome: "reservation_lost",
		},
		"error": {
			status:          schedulerpb.ERROR,
			expectedErr:     apierror.New(apierror.TypeInternal, "scheduler error"),
			expectedOutcome: "scheduler_error",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reject := func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				return &schedulerpb.SchedulerToFrontend{Status: tc.status, Error: "scheduler error"}
			}
			first := newReservationTestScheduler(t, true, reject)
			second := newReservationTestScheduler(t, true, reject)
			reg := prometheus.NewPedanticRegistry()
			f := setupReservationFrontend(t, reg, nil, first, second)

			ctx, scope := newReservationTestContext(t, 1)
			defer scope.Close(false)
			requireStreamError(t, tc.expectedErr, doGroupRequests(t, f, ctx, 2))
			// The failed reservation fails each sub-request of the evaluation.
			verifyEnqueueRequestsMetric(t, reg, map[string]int{tc.expectedOutcome: 2})

			require.Equal(t, 1, totalMessages(schedulerpb.RESERVE, first, second), "a rejected reservation must not go to another scheduler")
			require.Zero(t, totalMessages(schedulerpb.ENQUEUE, first, second))
		})
	}
}

func TestQueueReservation_FailedSubRequestAbortsReservation(t *testing.T) {
	var enqueues atomic.Int64
	s := newReservationTestScheduler(t, true, nil)
	s.replyFunc = func(f *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		}
		if enqueues.Inc() > 1 {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "enqueue failed"}
		}
		go func() {
			assert.NoError(t, sendStreamingResponseWithErrorCapture(f, msg.UserID, msg.QueryID, nil, newStringMessage("result")))
		}()
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, s)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)

	var errs []error
	for _, resp := range doGroupRequests(t, f, ctx, 2) {
		msg, err := resp.Next(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		msg.FreeBuffer()
	}
	require.Equal(t, []error{apierror.New(apierror.TypeInternal, "enqueue failed")}, errs)
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"enqueued": 1, "scheduler_error": 1})

	reserves := s.messages(schedulerpb.RESERVE)
	require.Len(t, reserves, 1)
	test.Poll(t, time.Second, []string{reserves[0].ReservationID}, func() interface{} {
		return s.aborts()
	})
}

func TestQueueReservation_ReservationNotFoundCountsAsTimedOut(t *testing.T) {
	s := newReservationTestScheduler(t, true, nil)
	s.replyFunc = func(_ *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type == schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.RESERVATION_NOT_FOUND, Error: "no reservation is open with this ID"}
		}
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, s)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)
	requireStreamError(t, apierror.New(apierror.TypeInternal, "no reservation is open with this ID"), doGroupRequests(t, f, ctx, 1))
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"reservation_timed_out": 1})
}

func TestQueueReservation_ReservationNotFoundAfterAbortCountsAsSchedulerError(t *testing.T) {
	var enqueues atomic.Int64
	s := newReservationTestScheduler(t, true, nil)
	s.replyFunc = func(_ *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		}
		if enqueues.Inc() == 1 {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "enqueue failed"}
		}
		// The failed sub-request aborts the reservation, so the scheduler no longer has it.
		test.Poll(t, time.Second, 1, func() interface{} {
			return len(s.aborts())
		})
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.RESERVATION_NOT_FOUND, Error: "no reservation is open with this ID"}
	}
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, s)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)
	for _, resp := range doGroupRequests(t, f, ctx, 2) {
		_, err := resp.Next(ctx)
		require.Error(t, err)
	}
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"scheduler_error": 2})
}

func TestQueueReservation_BusySchedulerDoesNotTakeReservation(t *testing.T) {
	release := make(chan struct{})
	holdReply := func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		<-release
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
	}
	first := newReservationTestScheduler(t, true, holdReply)
	second := newReservationTestScheduler(t, true, holdReply)
	f := setupReservationFrontend(t, nil, func(cfg *Config) { cfg.WorkerConcurrency = 1 }, first, second)
	defer close(release)

	// The only stream of the first scheduler that takes a reservation waits for its reply, so the
	// other scheduler takes the next reservation.
	for i := 1; i <= 2; i++ {
		ctx, scope := newReservationTestContext(t, 1)
		defer scope.Close(false)
		doGroupRequests(t, f, ctx, 1)
		test.Poll(t, time.Second, i, func() interface{} {
			return totalMessages(schedulerpb.RESERVE, first, second)
		})
	}

	require.Len(t, first.messages(schedulerpb.RESERVE), 1)
	require.Len(t, second.messages(schedulerpb.RESERVE), 1)
}

func TestQueueReservation_ReservationWaitsForAStream(t *testing.T) {
	newReservation := func() *queueReservation {
		f := &Frontend{
			log:       log.NewNopLogger(),
			reserveCh: make(chan *reserveRequest),
			queueReservations: promauto.With(prometheus.NewPedanticRegistry()).NewCounterVec(prometheus.CounterOpts{
				Name: "cortex_query_frontend_queue_reservations_total",
			}, []string{"result"}),
		}
		return &queueReservation{f: f, userID: reservationTestUserID, slots: 1, done: make(chan struct{})}
	}

	t.Run("a stream starts and takes the reservation", func(t *testing.T) {
		r := newReservation()
		go r.run(t.Context())

		// No stream reads reserveCh until now, so the reservation waits for one.
		req := <-r.f.reserveCh
		w := &frontendSchedulerWorker{}
		req.reply <- reserveResult{worker: w, status: schedulerpb.OK}
		<-r.done

		require.NoError(t, r.err)
		require.Same(t, w, r.worker)
	})

	t.Run("the query context ends first", func(t *testing.T) {
		r := newReservation()
		ctx, cancel := context.WithCancelCause(t.Context())
		cause := errors.New("query cancelled")
		cancel(cause)

		r.run(ctx)

		require.ErrorIs(t, r.err, cause)
		require.Nil(t, r.worker)
	})
}

func TestQueueReservation_LostReservationStreamsFailSubRequests(t *testing.T) {
	s := newReservationTestScheduler(t, true, nil)
	s.replyFunc = func(_ *Frontend, msg *schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		if msg.Type != schedulerpb.ENQUEUE {
			return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
		}
		// The only stream ends and reconnects as if the scheduler restarted without queue
		// reservation, so no stream takes the other sub-request.
		s.mu.Lock()
		s.queueReservationSupported = false
		s.mu.Unlock()
		return closeStreamReply
	}
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, func(cfg *Config) { cfg.WorkerConcurrency = 1 }, s)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)
	requireStreamError(t, apierror.New(apierror.TypeInternal, "failed to enqueue request"), doGroupRequests(t, f, ctx, 2))
	require.Len(t, s.messages(schedulerpb.ENQUEUE), 1)
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"reservation_lost": 2})
}

func TestQueueReservation_CloseIsDroppedWhenCancelChannelIsFull(t *testing.T) {
	f := &Frontend{log: log.NewNopLogger()}
	w := &frontendSchedulerWorker{cancelCh: make(chan cancelRequest, 1)}

	f.closeReservation(w, "reservation", true)
	require.Equal(t, cancelRequest{reservationID: "reservation", complete: true}, <-w.cancelCh)

	w.cancelCh <- cancelRequest{queryID: 1}
	f.closeReservation(w, "reservation", false)
	require.Equal(t, cancelRequest{queryID: 1}, <-w.cancelCh)
	require.Empty(t, w.cancelCh)
}

func TestQueueReservation_ClosesGrantAfterTheEvaluationEnded(t *testing.T) {
	tests := map[string]struct {
		endEvaluation func(cancel context.CancelFunc, scope *querymiddleware.RemoteExecutionReservationScope, streams []ResponseStream)
		breakStream   bool
		expectAbort   bool
	}{
		"query context cancelled while the reservation waits for its reply": {
			endEvaluation: func(cancel context.CancelFunc, _ *querymiddleware.RemoteExecutionReservationScope, _ []ResponseStream) {
				cancel()
			},
			expectAbort: true,
		},
		"query context cancelled, then the stream breaks before the reply": {
			endEvaluation: func(cancel context.CancelFunc, _ *querymiddleware.RemoteExecutionReservationScope, _ []ResponseStream) {
				cancel()
			},
			breakStream: true,
			expectAbort: true,
		},
		"scope aborted while the reservation waits for its reply": {
			endEvaluation: func(_ context.CancelFunc, scope *querymiddleware.RemoteExecutionReservationScope, streams []ResponseStream) {
				streams[0].Close()
				scope.Close(true)
			},
			expectAbort: true,
		},
		"scope completed while the reservation waits for its reply": {
			endEvaluation: func(_ context.CancelFunc, scope *querymiddleware.RemoteExecutionReservationScope, streams []ResponseStream) {
				streams[0].Close()
				scope.Close(false)
			},
			expectAbort: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			s := newReservationTestScheduler(t, true, func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				<-release
				if tc.breakStream {
					return closeStreamReply
				}
				return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}
			})
			reg := prometheus.NewPedanticRegistry()
			f := setupReservationFrontend(t, reg, nil, s)

			ctx, scope := newReservationTestContext(t, 1)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			streams := doGroupRequests(t, f, ctx, 1)
			test.Poll(t, time.Second, 1, func() interface{} {
				return len(s.messages(schedulerpb.RESERVE))
			})

			tc.endEvaluation(cancel, scope, streams)
			close(release)

			reservationID := s.messages(schedulerpb.RESERVE)[0].ReservationID
			if tc.expectAbort {
				test.Poll(t, time.Second, []string{reservationID}, func() interface{} {
					return s.aborts()
				})
			} else {
				test.Poll(t, time.Second, []string{reservationID}, func() interface{} {
					return s.completes()
				})
			}

			require.Empty(t, s.messages(schedulerpb.ENQUEUE))
			test.Poll(t, time.Second, nil, func() interface{} {
				return testutil.GatherAndCompare(reg, strings.NewReader(`
					# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
					# TYPE cortex_query_frontend_enqueue_requests_total counter
					cortex_query_frontend_enqueue_requests_total{outcome="cancelled"} 1
					cortex_query_frontend_enqueue_requests_total{outcome="enqueued"} 0
					cortex_query_frontend_enqueue_requests_total{outcome="invalid_request"} 0
					cortex_query_frontend_enqueue_requests_total{outcome="reservation_lost"} 0
					cortex_query_frontend_enqueue_requests_total{outcome="reservation_timed_out"} 0
					cortex_query_frontend_enqueue_requests_total{outcome="retries_exhausted"} 0
					cortex_query_frontend_enqueue_requests_total{outcome="scheduler_error"} 0
					cortex_query_frontend_enqueue_requests_total{outcome="too_many_requests"} 0
				`), "cortex_query_frontend_enqueue_requests_total")
			})
		})
	}
}

func TestQueueReservation_ScopeCloseClosesReservation(t *testing.T) {
	for name, aborted := range map[string]bool{"completed": false, "aborted": true} {
		t.Run(name, func(t *testing.T) {
			s := newReservationTestScheduler(t, true, nil)
			f := setupReservationFrontend(t, nil, nil, s)

			ctx, scope := newReservationTestContext(t, 2)
			sending, err := scope.Register()
			require.NoError(t, err)
			_, err = scope.Register()
			require.NoError(t, err, "the second group registers but never sends its sub-request")

			resp, err := f.DoProtobufRequest(querymiddleware.ContextWithRemoteExecutionReservationMember(ctx, sending), &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
			require.NoError(t, err)
			t.Cleanup(resp.Close)
			test.Poll(t, time.Second, 1, func() interface{} {
				return len(s.messages(schedulerpb.ENQUEUE))
			})
			requireStreamsSucceed(t, f, s, []ResponseStream{resp})
			require.Empty(t, s.aborts())
			require.Empty(t, s.completes(), "the reservation stays open until the evaluation ends")

			scope.Close(aborted)

			reserves := s.messages(schedulerpb.RESERVE)
			require.Len(t, reserves, 1)
			require.Equal(t, uint64(2), reserves[0].Slots)
			closes := s.completes
			if aborted {
				closes = s.aborts
			}
			test.Poll(t, time.Second, []string{reserves[0].ReservationID}, func() interface{} {
				return closes()
			})
		})
	}
}

func TestQueueReservation_SendsOneClose(t *testing.T) {
	w := &frontendSchedulerWorker{cancelCh: make(chan cancelRequest, 3)}
	r := &queueReservation{f: &Frontend{log: log.NewNopLogger()}, worker: w, reservationID: "reservation"}

	r.Close(false)
	r.Close(true)
	r.abort()

	require.Equal(t, cancelRequest{reservationID: "reservation", complete: true}, <-w.cancelCh)
	require.Empty(t, w.cancelCh)
}

func newGrantedTestReservation(reg prometheus.Registerer) (*queueReservation, *frontendSchedulerWorker) {
	f := &Frontend{
		log: log.NewNopLogger(),
		enqueueRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_frontend_enqueue_requests_total",
			Help: "Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.",
		}, []string{"outcome"}),
	}
	w := &frontendSchedulerWorker{
		ctx:                context.Background(),
		cancelCh:           make(chan cancelRequest, 1),
		reservedRequestsCh: make(chan *frontendRequest, 1),
	}
	done := make(chan struct{})
	close(done)
	r := &queueReservation{f: f, userID: reservationTestUserID, slots: 1, done: done, worker: w, reservationID: "reservation", streamsGone: make(chan struct{})}
	return r, w
}

func TestQueueReservation_CancelledSubRequestDoesNotSendEnqueue(t *testing.T) {
	const attempts = 100

	reg := prometheus.NewPedanticRegistry()
	r, w := newGrantedTestReservation(reg)
	cause := errors.New("query cancelled")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)

	// Each select has a ready ctx.Done() and a ready send, so one attempt without the ctx check
	// sends the ENQUEUE half of the time.
	for range attempts {
		freq := &frontendRequest{enqueue: make(chan enqueueResult, 1)}
		freq.enqueue <- enqueueResult{status: waitForResponse}
		_, err := r.enqueue(ctx, freq)
		require.ErrorIs(t, err, cause)
		require.Empty(t, w.reservedRequestsCh, "a cancelled sub-request must not send an ENQUEUE")
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
		# TYPE cortex_query_frontend_enqueue_requests_total counter
		cortex_query_frontend_enqueue_requests_total{outcome="cancelled"} %d
	`, attempts)), "cortex_query_frontend_enqueue_requests_total"))
}

func TestQueueReservation_FailureAfterCancelCountsAsCancelled(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	r, w := newGrantedTestReservation(reg)
	cause := errors.New("query cancelled")
	ctx, cancel := context.WithCancelCause(t.Context())

	go func() {
		freq := <-w.reservedRequestsCh
		// The query ends while the ENQUEUE is in flight, and the query-scheduler then rejects it.
		cancel(cause)
		freq.enqueue <- enqueueResult{status: schedulerReturnedError, schedulerErr: "the reservation has no idle slot"}
	}()

	_, err := r.enqueue(ctx, &frontendRequest{enqueue: make(chan enqueueResult, 1)})
	require.ErrorIs(t, err, cause)
	require.Equal(t, cancelRequest{reservationID: "reservation"}, <-w.cancelCh, "the failed sub-request aborts the reservation")

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
		# TYPE cortex_query_frontend_enqueue_requests_total counter
		cortex_query_frontend_enqueue_requests_total{outcome="cancelled"} 1
	`), "cortex_query_frontend_enqueue_requests_total"))
}
