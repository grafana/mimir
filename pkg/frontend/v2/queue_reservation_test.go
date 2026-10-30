// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/httpgrpc"
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
	"github.com/grafana/mimir/pkg/util/spanlogger"
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

	require.NoError(t, services.StartAndAwaitRunning(t.Context(), f))
	t.Cleanup(func() {
		// t.Context() is already cancelled when the cleanup runs.
		assert.NoError(t, services.StopAndAwaitTerminated(context.Background(), f))
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
		go func() {
			assert.NoError(t, sendStreamingResponseWithErrorCapture(f, reservationTestUserID, m.QueryID, nil, newStringMessage("result")))
		}()
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
		# HELP cortex_query_frontend_queue_reservations_total Total number of queue reservation attempts that the query-frontend sent to a query-scheduler, by outcome. An attempt that the query cancels before the reply does not count.
		# TYPE cortex_query_frontend_queue_reservations_total counter
		cortex_query_frontend_queue_reservations_total{outcome="ok"} 1
		cortex_query_frontend_queue_reservations_total{outcome="scheduler_error"} 0
		cortex_query_frontend_queue_reservations_total{outcome="send_failed"} 0
		cortex_query_frontend_queue_reservations_total{outcome="shutting_down"} 0
		cortex_query_frontend_queue_reservations_total{outcome="too_many_requests"} 0
	`), "cortex_query_frontend_queue_reservations_total"))
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"enqueued": subRequests})
}

func TestQueueReservation_SlotsAreTheSmallerOfParallelismAndSubRequests(t *testing.T) {
	tests := map[string]struct {
		maxParallelism int
		subRequests    int
		expectedSlots  int
	}{
		"parallelism below the sub-request count": {maxParallelism: 2, subRequests: 3, expectedSlots: 2},
		"parallelism above the sub-request count": {maxParallelism: 5, subRequests: 3, expectedSlots: 3},
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
				go func() {
					assert.NoError(t, sendStreamingResponseWithErrorCapture(f, reservationTestUserID, m.QueryID, nil, newStringMessage("result")))
				}()
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

	// The plain request that triggers the restart fails on the old stream, and the frontend
	// sends it again on the new stream.
	upgrade.Store(true)
	triggerCtx := querymiddleware.ContextWithParallelismLimiter(user.InjectOrgID(t.Context(), reservationTestUserID), querymiddleware.NewParallelismLimiter(1))
	trigger, err := f.DoProtobufRequest(triggerCtx, &querierpb.EvaluateQueryRequest{}, time.Now(), time.Now())
	require.NoError(t, err)
	t.Cleanup(trigger.Close)
	test.Poll(t, 5*time.Second, 1, func() interface{} {
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
	breakStream := func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		return closeStreamReply
	}
	// Each failed attempt ends one stream. Two schedulers have more streams than the attempts, so
	// streams that support queue reservation stay, and the sub-request does not use the plain path.
	first := newReservationTestScheduler(t, true, breakStream)
	second := newReservationTestScheduler(t, true, breakStream)
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, first, second)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)
	requireStreamError(t, apierror.New(apierror.TypeInternal, "failed to enqueue request"), doGroupRequests(t, f, ctx, 1))
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"retries_exhausted": 1})

	// The frontend tries the RESERVE once per attempt, each time with a new reservation ID. The
	// scheduler can have granted each one before its stream broke, so the frontend aborts each one.
	lost := append(first.messages(schedulerpb.RESERVE), second.messages(schedulerpb.RESERVE)...)
	require.Len(t, lost, testFrontendWorkerConcurrency+1)
	var lostIDs []string
	for _, m := range lost {
		lostIDs = append(lostIDs, m.ReservationID)
	}
	require.Len(t, slices.Compact(slices.Sorted(slices.Values(lostIDs))), len(lostIDs), "each attempt must use a new reservation ID")
	test.Poll(t, 5*time.Second, slices.Sorted(slices.Values(lostIDs)), func() interface{} {
		return slices.Sorted(slices.Values(append(first.aborts(), second.aborts()...)))
	})
	require.Zero(t, totalMessages(schedulerpb.ENQUEUE, first, second))
}

func TestQueueReservation_RetriesReserve(t *testing.T) {
	tests := map[string]struct {
		firstReply      *schedulerpb.SchedulerToFrontend
		expectAbort     bool
		expectedOutcome string
	}{
		"transport error": {
			firstReply:      closeStreamReply,
			expectAbort:     true,
			expectedOutcome: "send_failed",
		},
		"shutting down": {
			firstReply:      &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SHUTTING_DOWN},
			expectAbort:     false,
			expectedOutcome: "shutting_down",
		},
		"unknown status": {
			firstReply:      &schedulerpb.SchedulerToFrontend{Status: schedulerpb.SchedulerToFrontendStatus(1000)},
			expectAbort:     true,
			expectedOutcome: "scheduler_error",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var reserves atomic.Int64
			s := newReservationTestScheduler(t, true, func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
				if reserves.Inc() == 1 {
					return tc.firstReply
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
			if tc.expectAbort {
				test.Poll(t, time.Second, []string{attempts[0].ReservationID}, func() interface{} {
					return s.aborts()
				})
			} else {
				require.Empty(t, s.aborts(), "the query-scheduler did not open the reservation, so the frontend must not abort it")
			}
			verifyEnqueueRequestsMetric(t, reg, map[string]int{"enqueued": 1})
			require.Equal(t, 1.0, testutil.ToFloat64(f.queueReservations.WithLabelValues(tc.expectedOutcome)))
			require.Equal(t, 1.0, testutil.ToFloat64(f.queueReservations.WithLabelValues(reservationOutcomeOK)))
		})
	}
}

func TestQueueReservation_TooManyRequestsIsFinal(t *testing.T) {
	reject := func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.TOO_MANY_REQUESTS_PER_TENANT}
	}
	first := newReservationTestScheduler(t, true, reject)
	second := newReservationTestScheduler(t, true, reject)
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, first, second)

	ctx, scope := newReservationTestContext(t, 1)
	defer scope.Close(false)
	requireStreamError(t, apierror.New(apierror.TypeTooManyRequests, "too many outstanding requests"), doGroupRequests(t, f, ctx, 2))
	// The failed reservation fails each sub-request of the evaluation.
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"too_many_requests": 2})

	require.Equal(t, 1, totalMessages(schedulerpb.RESERVE, first, second), "a rejected reservation must not go to another scheduler")
	require.Zero(t, totalMessages(schedulerpb.ENQUEUE, first, second))
}

func TestQueueReservation_ErrorUsesPlainPath(t *testing.T) {
	const subRequests = 2

	s := newReservationTestScheduler(t, true, func(*schedulerpb.FrontendToScheduler) *schedulerpb.SchedulerToFrontend {
		return &schedulerpb.SchedulerToFrontend{Status: schedulerpb.ERROR, Error: "the reservation requests more slots than the per-tenant queue cap"}
	})
	reg := prometheus.NewPedanticRegistry()
	f := setupReservationFrontend(t, reg, nil, s)

	ctx, scope := newReservationTestContext(t, subRequests)
	streams := doGroupRequests(t, f, ctx, subRequests)
	test.Poll(t, 5*time.Second, subRequests, func() interface{} {
		return len(s.messages(schedulerpb.ENQUEUE))
	})
	for _, m := range s.messages(schedulerpb.ENQUEUE) {
		require.Empty(t, m.ReservationID, "the sub-request must use the plain path")
	}
	requireStreamsSucceed(t, f, s, streams)
	scope.Close(false)

	require.Len(t, s.messages(schedulerpb.RESERVE), 1, "a RESERVE that got ERROR must not be retried")
	require.Empty(t, s.completes(), "the query-scheduler did not open the reservation")
	require.Empty(t, s.aborts(), "the query-scheduler did not open the reservation")
	verifyEnqueueRequestsMetric(t, reg, map[string]int{"enqueued": subRequests})
	require.Equal(t, 1.0, testutil.ToFloat64(f.queueReservations.WithLabelValues(reservationOutcomeSchedulerError)))
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

func TestQueueReservation_ReserveStopsWhenTheQueryContextEnds(t *testing.T) {
	f := &Frontend{
		log:       log.NewNopLogger(),
		reserveCh: make(chan *reserveRequest),
	}
	// No stream reads reserveCh, so the reservation waits for one until ctx ends.
	r := &queueReservation{f: f, userID: reservationTestUserID, slots: 1, done: make(chan struct{})}
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("query cancelled")
	cancel(cause)

	r.run(ctx)

	require.ErrorIs(t, r.err, cause)
	require.Nil(t, r.worker)
}

func TestQueueReservation_UsesPlainPathWhenNoStreamSupportsQueueReservation(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	f := &Frontend{
		log:        log.NewNopLogger(),
		cfg:        Config{WorkerConcurrency: 1},
		requestsCh: make(chan *frontendRequest, 1),
		reserveCh:  make(chan *reserveRequest),
		enqueueRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_frontend_enqueue_requests_total",
			Help: "Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.",
		}, []string{"outcome"}),
	}
	// All streams that support queue reservation ended, for example after a rollback of the
	// query-schedulers, so no stream reads reserveCh.
	allStreamsGone := make(chan struct{})
	close(allStreamsGone)
	r := &queueReservation{f: f, userID: reservationTestUserID, slots: 1, allStreamsGone: allStreamsGone, done: make(chan struct{})}

	r.run(t.Context())
	require.True(t, r.plainPath)
	require.NoError(t, r.err)

	spanLogger, _ := spanlogger.New(t.Context(), log.NewNopLogger(), tracer, "test")
	freq := &frontendRequest{enqueue: make(chan enqueueResult, 1), spanLogger: spanLogger}
	freq.enqueue <- enqueueResult{status: waitForResponse}
	_, err := r.enqueue(t.Context(), freq)
	require.NoError(t, err)
	require.Same(t, freq, <-f.requestsCh, "the sub-request must use the plain path")
	require.Empty(t, freq.reservationID)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
		# TYPE cortex_query_frontend_enqueue_requests_total counter
		cortex_query_frontend_enqueue_requests_total{outcome="enqueued"} 1
	`), "cortex_query_frontend_enqueue_requests_total"))
}

func TestQueueReservation_NoSlotsUsesPlainPath(t *testing.T) {
	f := &Frontend{}
	require.Nil(t, f.newQueueReservation(t.Context(), reservationTestUserID, 3, 0))
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

	w.cancelCh <- cancelRequest{queryID: 1}
	f.closeReservation(w, "reservation", true)
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

func newGrantedTestReservation(t testing.TB, reg prometheus.Registerer) (*queueReservation, *frontendSchedulerWorker) {
	f := &Frontend{
		log: log.NewNopLogger(),
		enqueueRequests: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_query_frontend_enqueue_requests_total",
			Help: "Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.",
		}, []string{"outcome"}),
	}
	w := &frontendSchedulerWorker{
		ctx:                t.Context(),
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
	r, w := newGrantedTestReservation(t, reg)
	cause := errors.New("query cancelled")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)

	// Without the ctx check, an attempt sends the ENQUEUE when two selects with a ready ctx.Done()
	// both pick the other ready case. That happens in one attempt out of four.
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
	r, w := newGrantedTestReservation(t, reg)
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
	require.Len(t, w.cancelCh, 1, "the failed sub-request aborts the reservation")
	require.Equal(t, cancelRequest{reservationID: "reservation"}, <-w.cancelCh)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
		# TYPE cortex_query_frontend_enqueue_requests_total counter
		cortex_query_frontend_enqueue_requests_total{outcome="cancelled"} 1
	`), "cortex_query_frontend_enqueue_requests_total"))
}

func TestQueueReservation_FailedEnqueueOutcome(t *testing.T) {
	tests := map[string]struct {
		result enqueueResult
		// beforeReply runs after the worker took the ENQUEUE and before its reply.
		beforeReply     func(r *queueReservation, streamsGone chan struct{})
		expectedErr     error
		expectedOutcome string
	}{
		"no open reservation": {
			result:          enqueueResult{status: reservationNotFound, schedulerErr: "no reservation is open with this ID"},
			expectedErr:     apierror.New(apierror.TypeInternal, "no reservation is open with this ID"),
			expectedOutcome: "reservation_timed_out",
		},
		"no open reservation after this query-frontend closed it": {
			result:          enqueueResult{status: reservationNotFound, schedulerErr: "no reservation is open with this ID"},
			beforeReply:     func(r *queueReservation, _ chan struct{}) { r.abort() },
			expectedErr:     apierror.New(apierror.TypeInternal, "no reservation is open with this ID"),
			expectedOutcome: "cancelled",
		},
		"no open reservation after all streams ended": {
			result:          enqueueResult{status: reservationNotFound, schedulerErr: "no reservation is open with this ID"},
			beforeReply:     func(_ *queueReservation, streamsGone chan struct{}) { close(streamsGone) },
			expectedErr:     apierror.New(apierror.TypeInternal, "no reservation is open with this ID"),
			expectedOutcome: "reservation_lost",
		},
		"client error": {
			result:          enqueueResult{status: failed, clientErr: errors.New("bad request")},
			expectedErr:     httpgrpc.Errorf(http.StatusBadRequest, "failed to enqueue request: bad request"),
			expectedOutcome: "invalid_request",
		},
		"lost stream or unknown status": {
			result:          enqueueResult{status: failed},
			expectedErr:     apierror.New(apierror.TypeInternal, "failed to enqueue request"),
			expectedOutcome: "reservation_lost",
		},
		"too many requests": {
			result:          enqueueResult{status: tooManyRequests},
			expectedErr:     apierror.New(apierror.TypeInternal, "failed to enqueue request"),
			expectedOutcome: "too_many_requests",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			r, w := newGrantedTestReservation(t, reg)
			streamsGone := make(chan struct{})
			r.streamsGone = streamsGone

			go func() {
				freq := <-w.reservedRequestsCh
				if tc.beforeReply != nil {
					tc.beforeReply(r, streamsGone)
				}
				freq.enqueue <- tc.result
			}()

			_, err := r.enqueue(t.Context(), &frontendRequest{enqueue: make(chan enqueueResult, 1)})
			require.Equal(t, tc.expectedErr, err)
			require.Len(t, w.cancelCh, 1, "the failed sub-request aborts the reservation")
			require.Equal(t, cancelRequest{reservationID: "reservation"}, <-w.cancelCh)
			require.Empty(t, w.cancelCh, "the reservation gets one abort")

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
				# TYPE cortex_query_frontend_enqueue_requests_total counter
				cortex_query_frontend_enqueue_requests_total{outcome=%q} 1
			`, tc.expectedOutcome)), "cortex_query_frontend_enqueue_requests_total"))
		})
	}
}

func TestQueueReservation_StoppedWorkerFailsSubRequest(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	r, w := newGrantedTestReservation(t, reg)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	w.ctx = ctx
	// No stream reads the channel of the stopped worker.
	w.reservedRequestsCh = nil

	_, err := r.enqueue(t.Context(), &frontendRequest{enqueue: make(chan enqueueResult, 1)})
	require.Equal(t, apierror.New(apierror.TypeInternal, "failed to enqueue request"), err)
	require.Len(t, w.cancelCh, 1, "the failed sub-request aborts the reservation")
	require.Equal(t, cancelRequest{reservationID: "reservation"}, <-w.cancelCh)

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_query_frontend_enqueue_requests_total Total number of requests that the query-frontend tried to enqueue to a query-scheduler, by outcome.
		# TYPE cortex_query_frontend_enqueue_requests_total counter
		cortex_query_frontend_enqueue_requests_total{outcome="reservation_lost"} 1
	`), "cortex_query_frontend_enqueue_requests_total"))
}

type failingSendLoopClient struct {
	schedulerpb.SchedulerForFrontend_FrontendLoopClient
	sendErr    error
	sent       []*schedulerpb.FrontendToScheduler
	recvCalled bool
}

func (c *failingSendLoopClient) Send(msg *schedulerpb.FrontendToScheduler) error {
	c.sent = append(c.sent, msg)
	return c.sendErr
}

func (c *failingSendLoopClient) Recv() (*schedulerpb.SchedulerToFrontend, error) {
	c.recvCalled = true
	return nil, errors.New("unexpected Recv")
}

func TestQueueReservation_FailedReserveSendRepliesWithTransportError(t *testing.T) {
	sendErr := errors.New("stream broken")
	loop := &failingSendLoopClient{sendErr: sendErr}
	w := &frontendSchedulerWorker{frontendAddr: "frontend:9095"}
	streamsGone := make(chan struct{})
	req := &reserveRequest{userID: reservationTestUserID, reservationID: "reservation", slots: 2, reply: make(chan reserveResult, 1)}

	require.ErrorIs(t, w.reserve(loop, req, streamsGone), sendErr)
	require.Len(t, loop.sent, 1)
	require.Equal(t, schedulerpb.RESERVE, loop.sent[0].Type)
	require.False(t, loop.recvCalled, "the worker must not wait for a reply to a RESERVE that it did not send")

	// run waits for this reply. Without it, the evaluation waits until the query times out.
	require.Len(t, req.reply, 1)
	res := <-req.reply
	require.Same(t, w, res.worker)
	require.ErrorIs(t, res.transportErr, sendErr)
	require.Equal(t, (<-chan struct{})(streamsGone), res.streamsGone)
}

// benchmarkReservationScheduler replies OK to every message and sends a response for each ENQUEUE.
// It does not keep the messages, so that a long benchmark does not grow its memory.
type benchmarkReservationScheduler struct {
	b         testing.TB
	f         *Frontend
	supported bool
	response  [][]byte
	streams   atomic.Int64
}

func (s *benchmarkReservationScheduler) FrontendLoop(stream schedulerpb.SchedulerForFrontend_FrontendLoopServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	s.streams.Inc()
	if err := stream.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK, QueueReservationSupported: s.supported}); err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if msg.Type == schedulerpb.ENQUEUE {
			go sendStreamingResponseFromEncodedMessages(s.b, s.f, msg.UserID, msg.QueryID, s.response...)
		}
		if err := stream.Send(&schedulerpb.SchedulerToFrontend{Status: schedulerpb.OK}); err != nil {
			return err
		}
	}
}

func setupBenchmarkReservationFrontend(b *testing.B, enabled, supported bool, response [][]byte) *Frontend {
	frontendListener, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	schedulerListener, err := net.Listen("tcp", "localhost:0")
	require.NoError(b, err)
	frontendHost, frontendPort, err := net.SplitHostPort(frontendListener.Addr().String())
	require.NoError(b, err)
	grpcPort, err := strconv.Atoi(frontendPort)
	require.NoError(b, err)

	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.SchedulerAddress = schedulerListener.Addr().String()
	cfg.WorkerConcurrency = testFrontendWorkerConcurrency
	cfg.Addr = frontendHost
	cfg.Port = grpcPort
	cfg.QueryStoreAfter = 12 * time.Hour
	cfg.QueueReservationEnabled = enabled

	f, err := NewFrontend(cfg, limits{queryIngestersWithin: 13 * time.Hour}, log.NewNopLogger(), nil, newTestCodec())
	require.NoError(b, err)

	frontendServer := grpc.NewServer(
		grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
		grpc.UnaryInterceptor(middleware.ServerUserHeaderInterceptor),
	)
	frontendv2pb.RegisterFrontendForQuerierServer(frontendServer, f)
	s := &benchmarkReservationScheduler{b: b, f: f, supported: supported, response: response}
	schedulerServer := grpc.NewServer()
	schedulerpb.RegisterSchedulerForFrontendServer(schedulerServer, s)
	go func() {
		_ = frontendServer.Serve(frontendListener)
	}()
	go func() {
		_ = schedulerServer.Serve(schedulerListener)
	}()
	b.Cleanup(func() {
		_ = frontendListener.Close()
		frontendServer.GracefulStop()
		_ = schedulerListener.Close()
		schedulerServer.GracefulStop()
	})

	require.NoError(b, services.StartAndAwaitRunning(b.Context(), f))
	b.Cleanup(func() {
		// b.Context() is already cancelled when the cleanup runs.
		assert.NoError(b, services.StopAndAwaitTerminated(context.Background(), f))
	})
	test.Poll(b, time.Second, int64(cfg.WorkerConcurrency), func() interface{} {
		return s.streams.Load()
	})
	return f
}

// BenchmarkQueueReservation_Evaluation measures one evaluation with one sub-request per group, from
// the first DoProtobufRequest to the close of the scope. The mock scheduler runs in the same
// process, so the allocations include the scheduler side, and the latency has no network delay.
func BenchmarkQueueReservation_Evaluation(b *testing.B) {
	response := encode(b, generateBenchmarkResponse(1, 0, 256))
	modes := map[string]struct {
		enabled   bool
		supported bool
	}{
		"disabled":                            {enabled: false, supported: true},
		"scheduler without queue reservation": {enabled: true, supported: false},
		"enabled":                             {enabled: true, supported: true},
	}

	for _, groups := range []int{1, 4, 16} {
		for name, mode := range modes {
			b.Run(fmt.Sprintf("groups=%d/%s", groups, name), func(b *testing.B) {
				f := setupBenchmarkReservationFrontend(b, mode.enabled, mode.supported, response)
				ctx := user.InjectOrgID(b.Context(), reservationTestUserID)
				ctx = querymiddleware.ContextWithParallelismLimiter(ctx, querymiddleware.NewParallelismLimiter(math.MaxInt))
				req := &querierpb.EvaluateQueryRequest{}
				members := make([]*querymiddleware.RemoteExecutionReservationMember, groups)
				streams := make([]ResponseStream, groups)

				for b.Loop() {
					// Like the query-frontend, only an enabled frontend creates a scope.
					var scope *querymiddleware.RemoteExecutionReservationScope
					evalCtx := ctx
					if mode.enabled {
						scope = querymiddleware.NewRemoteExecutionReservationScope()
						evalCtx = querymiddleware.ContextWithRemoteExecutionReservationScope(ctx, scope)
						// MQE registers every group before it sends the first sub-request.
						for i := range members {
							var err error
							if members[i], err = scope.Register(); err != nil {
								require.NoError(b, err)
							}
						}
					}
					for i := range streams {
						reqCtx := evalCtx
						if scope != nil {
							reqCtx = querymiddleware.ContextWithRemoteExecutionReservationMember(evalCtx, members[i])
						}
						resp, err := f.DoProtobufRequest(reqCtx, req, time.Now(), time.Now())
						if err != nil {
							require.NoError(b, err)
						}
						streams[i] = resp
					}
					for _, resp := range streams {
						for range response {
							msg, err := resp.Next(ctx)
							if err != nil {
								require.NoError(b, err)
							}
							msg.FreeBuffer()
						}
						resp.Close()
					}
					if scope != nil {
						scope.Close(false)
					}
				}
			})
		}
	}
}
