// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/promtest"
)

// testQueueMetrics holds the metrics of a test queue. They share one registry.
type testQueueMetrics struct {
	reg                  *prometheus.Registry
	queueLength          *prometheus.GaugeVec
	discardedRequests    *prometheus.CounterVec
	enqueueDuration      prometheus.Histogram
	reservationSlots     prometheus.Histogram
	rejectedReservations *prometheus.CounterVec
	idleSlots            *IdleSlotsGauge
}

// testQueueOptions overrides the defaults of the test queue builders. A zero value keeps them.
type testQueueOptions struct {
	logger         log.Logger
	maxQueueLength *MaxQueueLengthGauge
}

// newUnstartedTestQueueWith returns a Queue that the caller must start, if it needs dispatcherLoop.
func newUnstartedTestQueueWith(tb testing.TB, maxOutstandingPerTenant int, opts testQueueOptions) (*Queue, *testQueueMetrics) {
	reg := prometheus.NewPedanticRegistry()
	f := promauto.With(reg)
	m := &testQueueMetrics{
		reg:                  reg,
		queueLength:          f.NewGaugeVec(prometheus.GaugeOpts{Name: "test_queue_length"}, []string{"user"}),
		discardedRequests:    f.NewCounterVec(prometheus.CounterOpts{Name: "test_discarded_requests"}, []string{"user"}),
		enqueueDuration:      f.NewHistogram(prometheus.HistogramOpts{Name: "test_enqueue_duration"}),
		reservationSlots:     f.NewHistogram(prometheus.HistogramOpts{Name: "test_reservation_slots"}),
		rejectedReservations: f.NewCounterVec(prometheus.CounterOpts{Name: "test_rejected_reservations"}, []string{"user"}),
		idleSlots:            NewIdleSlotsGauge("test_idle_slots", "Idle slots."),
	}
	reg.MustRegister(m.idleSlots)

	logger := opts.logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	q, err := New(
		logger,
		maxOutstandingPerTenant,
		0,
		m.queueLength,
		opts.maxQueueLength,
		m.discardedRequests,
		m.enqueueDuration,
		&ReservationMetrics{Slots: m.reservationSlots, Rejected: m.rejectedReservations, IdleSlots: m.idleSlots},
	)
	require.NoError(tb, err)
	return q, m
}

func newUnstartedTestQueue(tb testing.TB, maxOutstandingPerTenant int) *Queue {
	q, _ := newUnstartedTestQueueWith(tb, maxOutstandingPerTenant, testQueueOptions{})
	return q
}

func newTestQueueWith(t *testing.T, maxOutstandingPerTenant int, opts testQueueOptions) (*Queue, *testQueueMetrics) {
	q, m := newUnstartedTestQueueWith(t, maxOutstandingPerTenant, opts)
	// The start context is the parent of the service context. t.Context() is canceled before the
	// cleanup below runs, so it would stop the queue before the cleanup can drain it.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), q))
	t.Cleanup(func() {
		if q.State() == services.Terminated {
			return
		}
		// On shutdown, dispatcherLoop waits until a consumer drains every queued item. A test can
		// end with queued items, also when it fails, so drain them first.
		q.EmptyQueue(t)
		// t.Context() is already canceled when cleanup runs, so stop with a background context.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), q))
	})
	return q, m
}

func newTestQueue(t *testing.T, maxOutstandingPerTenant int) (*Queue, *testQueueMetrics) {
	return newTestQueueWith(t, maxOutstandingPerTenant, testQueueOptions{})
}

// requireIdleSlots checks the idle slots gauge. A tenant that expected omits must have
// no series.
func requireIdleSlots(t *testing.T, m *testQueueMetrics, expected map[string]int) {
	t.Helper()
	var b strings.Builder
	if len(expected) > 0 {
		b.WriteString("# HELP test_idle_slots Idle slots.\n# TYPE test_idle_slots gauge\n")
		for tenantID, slots := range expected {
			fmt.Fprintf(&b, "test_idle_slots{user=%q} %d\n", tenantID, slots)
		}
	}
	require.NoError(t, testutil.GatherAndCompare(m.reg, strings.NewReader(b.String()), "test_idle_slots"))
}

// syncDispatcher returns after dispatcherLoop has finished its previous work, such as the slot
// return of a dequeue. A dequeue returns the slot after the consumer received the item.
func syncDispatcher(t *testing.T, q *Queue) {
	t.Helper()
	closed, err := q.SubmitAbortReservation(ReservationKey{Owner: "sync", ID: "sync"})
	require.NoError(t, err)
	require.Nil(t, closed)
}

func registerTestConsumer(t *testing.T, q *Queue) *ConsumerWorkerConn {
	t.Helper()
	conn := NewUnregisteredConsumerWorkerConn(t.Context(), "consumer-1")
	require.NoError(t, q.AwaitRegisterConsumerWorkerConn(conn))
	return conn
}

func dequeueTestItem(t *testing.T, q *Queue, conn *ConsumerWorkerConn) Item {
	t.Helper()
	item, _, err := q.AwaitItemForConsumer(NewConsumerWorkerDequeueRequest(conn, FirstTenant()))
	require.NoError(t, err)
	return item
}

var (
	testKeyA = ReservationKey{Owner: "frontend-1", ID: "reservation-a"}
	testKeyB = ReservationKey{Owner: "frontend-1", ID: "reservation-b"}
	testKeyC = ReservationKey{Owner: "frontend-2", ID: "reservation-c"}
)

func insertIntoReservation(t *testing.T, q *Queue, key ReservationKey, tenantID string, item Item) (*Reservation, error) {
	t.Helper()
	var member *Reservation
	err := q.SubmitItemIntoReservation(key, tenantID, item, "", 0, time.Now(), func(res *Reservation) { member = res })
	return member, err
}

func TestQueue_Reservation_DequeueReturnsTheSlotToTheReservation(t *testing.T) {
	q, metrics := newTestQueue(t, 5)
	conn := registerTestConsumer(t, q)

	require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 2, time.Now()))
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 2})

	for _, item := range []string{"item-1", "item-2"} {
		member, err := insertIntoReservation(t, q, testKeyA, "tenant-a", item)
		require.NoError(t, err)
		require.NotNil(t, member)
	}
	requireIdleSlots(t, metrics, nil)

	require.Equal(t, Item("item-1"), dequeueTestItem(t, q, conn))
	syncDispatcher(t, q)
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 1})

	// The returned slot admits the third item.
	_, err := insertIntoReservation(t, q, testKeyA, "tenant-a", "item-3")
	require.NoError(t, err)
	requireIdleSlots(t, metrics, nil)

	// The owner completes the reservation after its last item.
	completed, err := q.SubmitCompleteReservation(testKeyA)
	require.NoError(t, err)
	require.Equal(t, &ClosedReservation{ID: testKeyA.ID, TenantID: "tenant-a", Inserted: 3, Released: 0, Outcome: ReservationCompleted}, completed)
	requireIdleSlots(t, metrics, nil)

	// After the close, a dequeue does not return a slot.
	require.Equal(t, Item("item-2"), dequeueTestItem(t, q, conn))
	require.Equal(t, Item("item-3"), dequeueTestItem(t, q, conn))
	syncDispatcher(t, q)
	requireIdleSlots(t, metrics, nil)

	// The whole cap is free again.
	require.NoError(t, q.SubmitOpenReservation(testKeyB, "tenant-a", 5, time.Now()))
	require.NoError(t, promtest.HasSampleCount(metrics.reg, "test_enqueue_duration", 3))
}

func TestQueue_Reservation_NoIdleSlotIsRejected(t *testing.T) {
	q, metrics := newTestQueue(t, 5)
	conn := registerTestConsumer(t, q)

	require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 1, time.Now()))
	_, err := insertIntoReservation(t, q, testKeyA, "tenant-a", "item-1")
	require.NoError(t, err)

	// The cap has room, but a reservation with no idle slot still rejects the item.
	member, err := insertIntoReservation(t, q, testKeyA, "tenant-a", "item-2")
	require.ErrorIs(t, err, ErrReservationNoIdleSlot)
	require.Nil(t, member, "successFn must not run for a rejected item")
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.queueLength.WithLabelValues("tenant-a")))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.discardedRequests.WithLabelValues("tenant-a")))

	// The rejected item is not a member, so a dequeue frees the slot for the reservation's last item.
	require.Equal(t, Item("item-1"), dequeueTestItem(t, q, conn))
	member, err = insertIntoReservation(t, q, testKeyA, "tenant-a", "item-2")
	require.NoError(t, err)
	require.NotNil(t, member)
	completed, err := q.SubmitCompleteReservation(testKeyA)
	require.NoError(t, err)
	require.Equal(t, &ClosedReservation{ID: testKeyA.ID, TenantID: "tenant-a", Inserted: 2, Released: 0, Outcome: ReservationCompleted}, completed)
	requireIdleSlots(t, metrics, nil)
}

func TestQueue_Reservation_NotFoundIsRejected(t *testing.T) {
	q, metrics := newTestQueue(t, 5)
	require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 1, time.Now()))

	// The cap has room, but an unknown ID still rejects the item.
	member, err := insertIntoReservation(t, q, ReservationKey{Owner: "frontend-1", ID: "unknown"}, "tenant-a", "item-1")
	require.ErrorIs(t, err, ErrReservationNotFound)
	require.Nil(t, member, "successFn must not run for a rejected item")
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.queueLength.WithLabelValues("tenant-a")))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.discardedRequests.WithLabelValues("tenant-a")))
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 1})
}

func TestQueue_Reservation_OtherTenantIsRejected(t *testing.T) {
	q, metrics := newTestQueue(t, 2)
	require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 1, time.Now()))

	called := false
	err := q.SubmitItemIntoReservation(testKeyA, "tenant-b", "item-1", "", 0, time.Now(), func(*Reservation) { called = true })
	require.ErrorIs(t, err, ErrReservationOtherTenant)
	require.False(t, called)
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.queueLength.WithLabelValues("tenant-b")))
	require.Equal(t, 0.0, testutil.ToFloat64(metrics.discardedRequests.WithLabelValues("tenant-b")))
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 1})

	// The rejected item is not a member, so the reservation still has its idle slot.
	member, err := insertIntoReservation(t, q, testKeyA, "tenant-a", "item-2")
	require.NoError(t, err)
	require.NotNil(t, member)
	completed, err := q.SubmitCompleteReservation(testKeyA)
	require.NoError(t, err)
	require.Equal(t, &ClosedReservation{ID: testKeyA.ID, TenantID: "tenant-a", Inserted: 1, Released: 0, Outcome: ReservationCompleted}, completed)
}

func TestQueue_Reservation_CloseReleasesTheIdleSlots(t *testing.T) {
	const timeout = time.Second
	start := time.Unix(1000, 0)

	testCases := map[string]struct {
		close           func(t *testing.T, q *Queue) []ClosedReservation
		expectedOutcome ReservationOutcome
		// A reservation cannot time out with a queued member, because a member finishes only after
		// its dequeue.
		memberQueuedAtClose bool
	}{
		"complete": {
			close: func(t *testing.T, q *Queue) []ClosedReservation {
				closed, err := q.SubmitCompleteReservation(testKeyA)
				require.NoError(t, err)
				require.NotNil(t, closed)
				return []ClosedReservation{*closed}
			},
			expectedOutcome:     ReservationCompleted,
			memberQueuedAtClose: true,
		},
		"abort": {
			close: func(t *testing.T, q *Queue) []ClosedReservation {
				closed, err := q.SubmitAbortReservation(testKeyA)
				require.NoError(t, err)
				require.NotNil(t, closed)
				return []ClosedReservation{*closed}
			},
			expectedOutcome:     ReservationAborted,
			memberQueuedAtClose: true,
		},
		"owner close": {
			close: func(t *testing.T, q *Queue) []ClosedReservation {
				closed, err := q.SubmitCloseReservationsOfOwner(testKeyA.Owner)
				require.NoError(t, err)
				return closed
			},
			expectedOutcome:     ReservationOwnerDisconnected,
			memberQueuedAtClose: true,
		},
		"timeout": {
			close: func(t *testing.T, q *Queue) []ClosedReservation {
				closed, err := q.SubmitReapReservations(start.Add(timeout), timeout)
				require.NoError(t, err)
				return closed
			},
			expectedOutcome:     ReservationTimedOut,
			memberQueuedAtClose: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			const slots = 3
			q, metrics := newTestQueue(t, 5)
			conn := registerTestConsumer(t, q)

			require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", slots, start))
			var member *Reservation
			for _, item := range []string{"item-1", "item-2"} {
				err := q.SubmitItemIntoReservation(testKeyA, "tenant-a", item, "", 0, start, func(res *Reservation) { member = res })
				require.NoError(t, err)
			}
			require.Equal(t, Item("item-1"), dequeueTestItem(t, q, conn))
			member.ItemFinished(start)
			if !testCase.memberQueuedAtClose {
				require.Equal(t, Item("item-2"), dequeueTestItem(t, q, conn))
				member.ItemFinished(start)
			}
			queued := 0
			if testCase.memberQueuedAtClose {
				queued = 1
			}

			closed := testCase.close(t, q)
			require.Equal(t, []ClosedReservation{{
				ID: testKeyA.ID, TenantID: "tenant-a", Inserted: 2, Released: slots - queued, Outcome: testCase.expectedOutcome,
			}}, closed)
			requireIdleSlots(t, metrics, nil)

			if testCase.memberQueuedAtClose {
				// The queued member keeps its slot until its dequeue, and the dequeue does not return it.
				require.ErrorIs(t, q.SubmitOpenReservation(testKeyB, "tenant-a", 5, start), ErrTooManyRequests)
				require.Equal(t, Item("item-2"), dequeueTestItem(t, q, conn))
				syncDispatcher(t, q)
				requireIdleSlots(t, metrics, nil)
			}
			require.NoError(t, q.SubmitOpenReservation(testKeyB, "tenant-a", 5, start))

			// The closed reservation rejects a later insert.
			_, err := insertIntoReservation(t, q, testKeyA, "tenant-a", "item-3")
			require.ErrorIs(t, err, ErrReservationNotFound)
		})
	}
}

func TestQueue_SubmitCompleteAndAbortReservation_UnknownKeyIsANoOp(t *testing.T) {
	q, metrics := newTestQueue(t, 5)
	require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 2, time.Now()))

	closed, err := q.SubmitCompleteReservation(testKeyB)
	require.NoError(t, err)
	require.Nil(t, closed)
	closed, err = q.SubmitAbortReservation(testKeyB)
	require.NoError(t, err)
	require.Nil(t, closed)
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 2})
}

func TestQueue_SubmitCloseReservationsOfOwner_ClosesOnlyThatOwner(t *testing.T) {
	q, metrics := newTestQueue(t, 10)
	require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 2, time.Now()))
	require.NoError(t, q.SubmitOpenReservation(testKeyB, "tenant-b", 3, time.Now()))
	require.NoError(t, q.SubmitOpenReservation(testKeyC, "tenant-a", 4, time.Now()))

	closed, err := q.SubmitCloseReservationsOfOwner(testKeyA.Owner)
	require.NoError(t, err)
	require.ElementsMatch(t, []ClosedReservation{
		{ID: testKeyA.ID, TenantID: "tenant-a", Released: 2, Outcome: ReservationOwnerDisconnected},
		{ID: testKeyB.ID, TenantID: "tenant-b", Released: 3, Outcome: ReservationOwnerDisconnected},
	}, closed)
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 4})

	closed, err = q.SubmitCloseReservationsOfOwner(testKeyA.Owner)
	require.NoError(t, err)
	require.Empty(t, closed)
}

func TestQueue_SubmitOpenReservation_Rejections(t *testing.T) {
	var logs concurrency.SyncBuffer
	q, metrics := newTestQueueWith(t, 5, testQueueOptions{logger: log.NewLogfmtLogger(&logs)})

	require.NoError(t, q.SubmitItemToEnqueue("tenant-a", "plain-1", "", 0, func() {}))
	require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 3, time.Now()))
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 3})

	err := q.SubmitOpenReservation(testKeyB, "tenant-a", 2, time.Now())
	var rejected *ReservationRejectedError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, ReservationRejectedError{Requested: 2, Queued: 1, Idle: 3, MaxTenantQueueSize: 5}, *rejected)
	require.ErrorIs(t, err, ErrTooManyRequests)
	// The scheduler logs the rejection from the error. A log call in dispatcherLoop stalls every producer.
	require.Empty(t, logs.String())

	require.ErrorIs(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 1, time.Now()), ErrReservationExists)
	require.ErrorIs(t, q.SubmitOpenReservation(testKeyC, "tenant-b", 6, time.Now()), ErrReservationExceedsCap)
	require.ErrorIs(t, q.SubmitOpenReservation(testKeyC, "tenant-b", 0, time.Now()), errInvalidReservationSlots)
	require.ErrorIs(t, q.SubmitOpenReservation(testKeyC, "", 1, time.Now()), ErrInvalidTenantID)

	// Only the cap rejection discards, and it counts no discarded request. Only the accepted
	// reservation observes its slots.
	require.NoError(t, testutil.GatherAndCompare(metrics.reg, strings.NewReader(`
		# HELP test_rejected_reservations
		# TYPE test_rejected_reservations counter
		test_rejected_reservations{user="tenant-a"} 1
		# HELP test_reservation_slots
		# TYPE test_reservation_slots histogram
		test_reservation_slots_bucket{le="0.005"} 0
		test_reservation_slots_bucket{le="0.01"} 0
		test_reservation_slots_bucket{le="0.025"} 0
		test_reservation_slots_bucket{le="0.05"} 0
		test_reservation_slots_bucket{le="0.1"} 0
		test_reservation_slots_bucket{le="0.25"} 0
		test_reservation_slots_bucket{le="0.5"} 0
		test_reservation_slots_bucket{le="1"} 0
		test_reservation_slots_bucket{le="2.5"} 0
		test_reservation_slots_bucket{le="5"} 1
		test_reservation_slots_bucket{le="10"} 1
		test_reservation_slots_bucket{le="+Inf"} 1
		test_reservation_slots_sum 3
		test_reservation_slots_count 1
	`), "test_discarded_requests", "test_rejected_reservations", "test_reservation_slots"))
	requireIdleSlots(t, metrics, map[string]int{"tenant-a": 3})
	require.NoError(t, promtest.HasSampleCount(metrics.reg, "test_enqueue_duration", 1))
}

func TestQueue_SubmitReapReservations(t *testing.T) {
	const timeout = time.Second
	start := time.Unix(1000, 0)

	t.Run("an idle reservation closes when its idle time reaches the timeout", func(t *testing.T) {
		q, _ := newTestQueue(t, 5)
		require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 2, start))

		closed, err := q.SubmitReapReservations(start.Add(timeout-time.Nanosecond), timeout)
		require.NoError(t, err)
		require.Empty(t, closed)

		closed, err = q.SubmitReapReservations(start.Add(timeout), timeout)
		require.NoError(t, err)
		require.Equal(t, []ClosedReservation{{ID: testKeyA.ID, TenantID: "tenant-a", Released: 2, Outcome: ReservationTimedOut}}, closed)
	})

	t.Run("an inflight member keeps the reservation open, and its finish starts the idle time", func(t *testing.T) {
		q, _ := newTestQueue(t, 5)
		conn := registerTestConsumer(t, q)
		require.NoError(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 2, start))
		var member *Reservation
		err := q.SubmitItemIntoReservation(testKeyA, "tenant-a", "item-1", "", 0, start, func(res *Reservation) { member = res })
		require.NoError(t, err)

		closed, err := q.SubmitReapReservations(start.Add(10*timeout), timeout)
		require.NoError(t, err)
		require.Empty(t, closed, "a reservation with an inflight member must stay open")

		// The finish is half a timeout before the next check, so that check finds the reservation idle
		// for less than the timeout.
		require.Equal(t, Item("item-1"), dequeueTestItem(t, q, conn))
		member.ItemFinished(start.Add(10*timeout + timeout/2))

		closed, err = q.SubmitReapReservations(start.Add(11*timeout), timeout)
		require.NoError(t, err)
		require.Empty(t, closed)

		closed, err = q.SubmitReapReservations(start.Add(11*timeout+timeout/2-time.Nanosecond), timeout)
		require.NoError(t, err)
		require.Empty(t, closed)

		closed, err = q.SubmitReapReservations(start.Add(11*timeout+timeout/2), timeout)
		require.NoError(t, err)
		require.Equal(t, []ClosedReservation{{ID: testKeyA.ID, TenantID: "tenant-a", Inserted: 1, Released: 2, Outcome: ReservationTimedOut}}, closed)
	})

	t.Run("the reaper closes only the due reservations", func(t *testing.T) {
		const reservations = 100
		q, _ := newTestQueue(t, reservations)
		for i := 0; i < reservations; i++ {
			key := ReservationKey{Owner: "frontend-1", ID: fmt.Sprintf("reservation-%03d", i)}
			require.NoError(t, q.SubmitOpenReservation(key, "tenant-a", 1, start.Add(time.Duration(i))))
		}

		closed, err := q.SubmitReapReservations(start.Add(timeout+49), timeout)
		require.NoError(t, err)
		ids := make([]string, 0, len(closed))
		for _, c := range closed {
			ids = append(ids, c.ID)
		}
		expected := make([]string, 0, 50)
		for i := 0; i < 50; i++ {
			expected = append(expected, fmt.Sprintf("reservation-%03d", i))
		}
		require.ElementsMatch(t, expected, ids)

		closed, err = q.SubmitReapReservations(start.Add(timeout+reservations), timeout)
		require.NoError(t, err)
		require.Len(t, closed, 50)
	})

	t.Run("busy reservations that open together get different next checks within the timeout", func(t *testing.T) {
		const reservations = 100
		// The queue does not run dispatcherLoop, so the test reads the heap state directly.
		q := newUnstartedTestQueue(t, reservations)
		for i := 0; i < reservations; i++ {
			key := ReservationKey{Owner: "frontend-1", ID: fmt.Sprintf("reservation-%03d", i)}
			require.NoError(t, q.processProducerOperation(producerOperation{op: opOpenReservation, reservationKey: key, tenantID: "tenant-a", slots: 1, now: start}))
			q.reservations[key].inflight.Store(1)
		}

		require.NoError(t, q.processProducerOperation(producerOperation{op: opReapReservations, now: start, timeout: timeout, result: &producerOpResult{}}))

		nextChecks := map[time.Time]struct{}{}
		for _, res := range q.reservations {
			require.False(t, res.nextCheck.Before(start.Add(timeout/2)), "next check %v is earlier than half the timeout", res.nextCheck)
			require.False(t, res.nextCheck.After(start.Add(timeout)), "next check %v is later than the timeout", res.nextCheck)
			nextChecks[res.nextCheck] = struct{}{}
		}
		require.Greater(t, len(nextChecks), 1, "all busy reservations got the same next check")
	})

	t.Run("a timeout that is not greater than 0 is invalid", func(t *testing.T) {
		q, _ := newTestQueue(t, 5)
		_, err := q.SubmitReapReservations(start, 0)
		require.ErrorIs(t, err, errInvalidReservationTimeout)
	})
}

func TestQueue_Reservation_FailedSendKeepsTheSlot(t *testing.T) {
	// The queue does not run dispatcherLoop, so the test calls the dispatcher functions itself.
	q := newUnstartedTestQueue(t, 5)
	now := time.Now()
	require.NoError(t, q.processProducerOperation(producerOperation{
		op: opOpenReservation, reservationKey: testKeyA, tenantID: "tenant-a", slots: 2, now: now,
	}))
	require.NoError(t, q.processProducerOperation(producerOperation{
		op: opEnqueueIntoReservation, reservationKey: testKeyA, tenantID: "tenant-a", item: "item-1", now: now,
	}))
	res := q.reservations[testKeyA]
	require.Equal(t, 1, res.queued)
	require.Equal(t, 1, q.queueBroker.idleSlotsByTenant["tenant-a"])

	ctx, cancel := context.WithCancel(t.Context())
	conn := NewUnregisteredConsumerWorkerConn(ctx, "consumer-1")
	q.processRegisterConsumerWorkerConn(conn)
	// The canceled context makes the send fail, so the queue puts the item back at the front.
	cancel()
	require.True(t, q.trySendNextItemForConsumer(NewConsumerWorkerDequeueRequest(conn, FirstTenant())))

	require.Equal(t, 1, q.queueBroker.itemCount())
	require.Equal(t, 1, res.queued, "the item that went back to the queue still uses its slot")
	require.Equal(t, 1, q.queueBroker.idleSlotsByTenant["tenant-a"])
}

func TestQueue_Reservation_ReturnsOccupancyToItsPriorValueOnEveryClosePath(t *testing.T) {
	// The queue does not run dispatcherLoop, so the test calls the dispatcher functions itself.
	q := newUnstartedTestQueue(t, 10)
	now := time.Now()
	require.NoError(t, q.processProducerOperation(producerOperation{op: opEnqueue, tenantID: "tenant-a", item: "plain-1"}))
	before := q.queueBroker.tenantOccupancy("tenant-a")

	closes := []producerOperation{
		{op: opCompleteReservation, reservationKey: testKeyA},
		{op: opAbortReservation, reservationKey: testKeyA},
		{op: opCloseReservationsOfOwner, reservationKey: ReservationKey{Owner: testKeyA.Owner}},
		{op: opReapReservations, now: now.Add(time.Hour), timeout: time.Second},
	}
	for _, closeOp := range closes {
		require.NoError(t, q.processProducerOperation(producerOperation{
			op: opOpenReservation, reservationKey: testKeyA, tenantID: "tenant-a", slots: 4, now: now,
		}))
		require.Equal(t, before+4, q.queueBroker.tenantOccupancy("tenant-a"))

		closeOp.result = &producerOpResult{}
		require.NoError(t, q.processProducerOperation(closeOp))
		require.Len(t, closeOp.result.closed, 1)
		require.Equal(t, before, q.queueBroker.tenantOccupancy("tenant-a"), "op %d", closeOp.op)
		require.Empty(t, q.queueBroker.idleSlotsByTenant)
		require.Empty(t, q.reservations)
		require.Empty(t, q.reservationsByOwner)
		require.Empty(t, q.reservationHeap)
	}
}

func TestReservation_ItemFinished_NeverMovesTheActivityBack(t *testing.T) {
	res := &Reservation{opened: time.Unix(0, 0)}
	res.inflight.Store(2)
	res.ItemFinished(time.Unix(0, 20))
	res.ItemFinished(time.Unix(0, 10))
	require.Equal(t, int64(20), res.lastActivityAfterOpen.Load())
	require.Zero(t, res.inflight.Load())
}

func TestIdleSlotsGauge(t *testing.T) {
	g := NewIdleSlotsGauge("test_idle_slots", "Idle slots.")
	g.set("tenant-a", 3)
	g.set("tenant-b", 1)
	g.set("tenant-b", 0)
	require.NoError(t, testutil.CollectAndCompare(g, strings.NewReader(`
		# HELP test_idle_slots Idle slots.
		# TYPE test_idle_slots gauge
		test_idle_slots{user="tenant-a"} 3
	`)))
}

func TestQueue_StoppedQueue_RejectsReservationCalls(t *testing.T) {
	q, _ := newTestQueue(t, 5)
	require.NoError(t, services.StopAndAwaitTerminated(t.Context(), q))

	require.ErrorIs(t, q.SubmitOpenReservation(testKeyA, "tenant-a", 1, time.Now()), ErrStopped)
	err := q.SubmitItemIntoReservation(testKeyA, "tenant-a", "item-1", "", 0, time.Now(), func(*Reservation) {})
	require.ErrorIs(t, err, ErrStopped)
	_, err = q.SubmitCompleteReservation(testKeyA)
	require.ErrorIs(t, err, ErrStopped)
	_, err = q.SubmitAbortReservation(testKeyA)
	require.ErrorIs(t, err, ErrStopped)
	_, err = q.SubmitCloseReservationsOfOwner(testKeyA.Owner)
	require.ErrorIs(t, err, ErrStopped)
	_, err = q.SubmitReapReservations(time.Now(), time.Second)
	require.ErrorIs(t, err, ErrStopped)
}

// mixedLoadItem is the item of TestQueue_MixedPlainAndReservationLoad. successFn sets reservation on
// the dispatcher goroutine before any consumer can receive the item.
type mixedLoadItem struct {
	reservation *Reservation
}

func TestQueue_MixedPlainAndReservationLoad(t *testing.T) {
	const (
		// A small cap and slow consumers make the producers reach the cap. A producer waits for
		// consumerDelay after a rejection, so the test also admits many reservations.
		maxOutstandingPerTenant = 8
		consumerDelay           = 100 * time.Microsecond
		plainProducers          = 2
		plainItemsPerProducer   = 300
		reservationProducers    = 3
		reservationsPerProducer = 200
		maxSlots                = 3
		maxTotal                = 8
		consumers               = 2
	)
	tenants := []string{"tenant-a", "tenant-b", "tenant-c"}

	q, metrics := newTestQueue(t, maxOutstandingPerTenant)

	var (
		mtx                 sync.Mutex
		accepted            int
		dequeued            int
		rejected            atomic.Int64
		reservationRejected atomic.Int64
		admitted            atomic.Int64
		noIdleSlot          atomic.Int64
		maxOccupancy        atomic.Int64
	)
	// recordOccupancy runs in successFn on the dispatcher goroutine, so it can read the broker state
	// without a race.
	recordOccupancy := func(tenantID string) {
		occupancy := int64(q.queueBroker.tenantOccupancy(tenantID))
		for {
			current := maxOccupancy.Load()
			if occupancy <= current || maxOccupancy.CompareAndSwap(current, occupancy) {
				return
			}
		}
	}

	consumerCtx, stopConsumers := context.WithCancel(t.Context())
	defer stopConsumers()
	var consumersWG sync.WaitGroup
	for i := 0; i < consumers; i++ {
		conn := NewUnregisteredConsumerWorkerConn(consumerCtx, fmt.Sprintf("consumer-%d", i))
		require.NoError(t, q.AwaitRegisterConsumerWorkerConn(conn))
		consumersWG.Add(1)
		go func() {
			defer consumersWG.Done()
			idx := FirstTenant()
			for {
				item, next, err := q.AwaitItemForConsumer(NewConsumerWorkerDequeueRequest(conn, idx))
				if err != nil {
					return
				}
				idx = next
				time.Sleep(consumerDelay)
				if it, ok := item.(*mixedLoadItem); ok && it.reservation != nil {
					it.reservation.ItemFinished(time.Now())
				}
				mtx.Lock()
				dequeued++
				mtx.Unlock()
			}
		}()
	}

	errs := make(chan error, plainProducers+reservationProducers)
	var producersWG sync.WaitGroup
	for p := 0; p < plainProducers; p++ {
		producersWG.Add(1)
		go func(seed int64) {
			defer producersWG.Done()
			rnd := rand.New(rand.NewSource(seed))
			for i := 0; i < plainItemsPerProducer; i++ {
				tenantID := tenants[rnd.Intn(len(tenants))]
				err := q.SubmitItemToEnqueue(tenantID, i, "", 0, func() { recordOccupancy(tenantID) })
				switch {
				case err == nil:
					mtx.Lock()
					accepted++
					mtx.Unlock()
				case errors.Is(err, ErrTooManyRequests):
					rejected.Add(1)
					time.Sleep(consumerDelay)
				default:
					errs <- err
					return
				}
			}
		}(int64(p))
	}
	for p := 0; p < reservationProducers; p++ {
		producersWG.Add(1)
		go func(seed int64) {
			defer producersWG.Done()
			rnd := rand.New(rand.NewSource(seed))
			for b := 0; b < reservationsPerProducer; b++ {
				tenantID := tenants[rnd.Intn(len(tenants))]
				key := ReservationKey{Owner: fmt.Sprintf("producer-%d", seed), ID: fmt.Sprintf("reservation-%d", b)}
				slots := 1 + rnd.Intn(maxSlots)
				total := 1 + rnd.Intn(maxTotal)
				err := q.SubmitOpenReservation(key, tenantID, slots, time.Now())
				if errors.Is(err, ErrTooManyRequests) {
					reservationRejected.Add(1)
					time.Sleep(consumerDelay)
					continue
				}
				if err != nil {
					errs <- err
					return
				}
				admitted.Add(1)
				// Insert 0 to total items, then complete or abort the reservation at random.
				inserts := rnd.Intn(total + 1)
				for i := 0; i < inserts; i++ {
					it := &mixedLoadItem{}
					err := q.SubmitItemIntoReservation(key, tenantID, it, "", 0, time.Now(), func(res *Reservation) {
						it.reservation = res
						recordOccupancy(tenantID)
					})
					// The frontend limiter waits for a dequeue before the next insert. This producer
					// sends at once, gets the rejection, and retries the item after a pause.
					if errors.Is(err, ErrReservationNoIdleSlot) {
						noIdleSlot.Add(1)
						time.Sleep(consumerDelay)
						i--
						continue
					}
					if err != nil {
						errs <- err
						return
					}
					mtx.Lock()
					accepted++
					mtx.Unlock()
				}
				closeFn := q.SubmitAbortReservation
				if rnd.Intn(2) == 0 {
					closeFn = q.SubmitCompleteReservation
				}
				if _, err := closeFn(key); err != nil {
					errs <- err
					return
				}
			}
		}(int64(100 + p))
	}
	producersWG.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		mtx.Lock()
		defer mtx.Unlock()
		return dequeued == accepted
	}, 10*time.Second, 10*time.Millisecond, "every accepted item must be dequeued; none may get stuck")

	stopConsumers()
	consumersWG.Wait()
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), q))

	// These checks come after the drain. A failed check before it stops the test with items in the
	// queue, and then the queue cleanup waits for consumers that are gone.
	require.Positive(t, rejected.Load(), "the plain producers must reach the cap, or the test does not cover plain rejections")
	require.Positive(t, reservationRejected.Load(), "the reservation producers must reach the cap, or the test does not cover reservation rejections")
	require.Positive(t, admitted.Load(), "the test must admit reservations beside plain items")
	require.Positive(t, noIdleSlot.Load(), "the reservations must run out of idle slots, or the test does not cover that rejection")
	require.LessOrEqual(t, maxOccupancy.Load(), int64(maxOutstandingPerTenant), "queued items plus reservations must never exceed the cap")

	// The dispatcher goroutine has exited, so the broker state is safe to read here.
	requireIdleSlots(t, metrics, nil)
	require.Empty(t, q.queueBroker.idleSlotsByTenant)
	require.Empty(t, q.reservations)
	require.Empty(t, q.reservationsByOwner)
	require.Empty(t, q.reservationHeap)
	require.Empty(t, q.queueBroker.tenantConsumerAssignments.tenantsByID)
	for _, id := range q.queueBroker.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder() {
		require.Empty(t, id, "no tenant may stay in the tenant order after the queue drains")
	}
}
