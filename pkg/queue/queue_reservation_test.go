// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/promtest"
)

// testQueueMetrics holds the metrics of a test queue. They share one registry.
type testQueueMetrics struct {
	reg                   *prometheus.Registry
	queueLength           *prometheus.GaugeVec
	discardedRequests     *prometheus.CounterVec
	enqueueDuration       prometheus.Histogram
	reservationSize       prometheus.Histogram
	discardedReservations *prometheus.CounterVec
	unusedSlots           *prometheus.GaugeVec
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
		reg:                   reg,
		queueLength:           f.NewGaugeVec(prometheus.GaugeOpts{Name: "test_queue_length"}, []string{"user"}),
		discardedRequests:     f.NewCounterVec(prometheus.CounterOpts{Name: "test_discarded_requests"}, []string{"user"}),
		enqueueDuration:       f.NewHistogram(prometheus.HistogramOpts{Name: "test_enqueue_duration"}),
		reservationSize:       f.NewHistogram(prometheus.HistogramOpts{Name: "test_reservation_size"}),
		discardedReservations: f.NewCounterVec(prometheus.CounterOpts{Name: "test_discarded_reservations"}, []string{"user"}),
		unusedSlots:           f.NewGaugeVec(prometheus.GaugeOpts{Name: "test_unused_slots"}, []string{"user"}),
	}

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
		&ReservationMetrics{Size: m.reservationSize, Discarded: m.discardedReservations, UnusedSlots: m.unusedSlots},
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
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), q))
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanup runs, so stop with a background context.
		if q.State() != services.Terminated {
			require.NoError(t, services.StopAndAwaitTerminated(context.Background(), q))
		}
	})
	return q, m
}

func newTestQueue(t *testing.T, maxOutstandingPerTenant int) (*Queue, *prometheus.GaugeVec) {
	q, m := newTestQueueWith(t, maxOutstandingPerTenant, testQueueOptions{})
	return q, m.unusedSlots
}

func TestQueue_SubmitReserveSlots_ThenSubmitItemIntoSlot(t *testing.T) {
	q, metrics := newTestQueueWith(t, 5, testQueueOptions{})
	unusedSlots := metrics.unusedSlots
	// No consumer dequeues the two items below. Drain them before the newTestQueueWith cleanup stops
	// the queue. On shutdown, dispatcherLoop waits until a consumer drains every queued item, so
	// StopAndAwaitTerminated would never return.
	t.Cleanup(func() { q.EmptyQueue(t) })

	require.NoError(t, q.SubmitReserveSlots("tenant-a", 2))
	require.Equal(t, 2.0, testutil.ToFloat64(unusedSlots.WithLabelValues("tenant-a")))

	var called int
	require.NoError(t, q.SubmitItemIntoSlot("tenant-a", "item-1", "", 0, func() { called++ }))
	require.Equal(t, 1.0, testutil.ToFloat64(unusedSlots.WithLabelValues("tenant-a")))
	require.NoError(t, q.SubmitItemIntoSlot("tenant-a", "item-2", "", 0, func() { called++ }))
	require.Equal(t, 2, called)
	// Both reserved slots are used, so the gauge must return to exactly zero.
	require.Equal(t, 0.0, testutil.ToFloat64(unusedSlots.WithLabelValues("tenant-a")))
	// SubmitItemIntoSlot observes enqueueDuration, as SubmitItemToEnqueue does. SubmitReserveSlots
	// does not.
	require.NoError(t, promtest.HasSampleCount(metrics.reg, "test_enqueue_duration", 2))

	// No reserved slot is left, so a third item must fail.
	err := q.SubmitItemIntoSlot("tenant-a", "item-3", "", 0, func() { called++ })
	require.ErrorIs(t, err, ErrNoReservation)
	require.Equal(t, 2, called)
	require.Equal(t, 0.0, testutil.ToFloat64(unusedSlots.WithLabelValues("tenant-a")))
}

func TestQueue_SubmitReleaseSlots_ReturnsTheGaugeToZeroAndFreesTheCap(t *testing.T) {
	const maxOutstandingPerTenant = 5

	testCases := map[string]struct {
		reserved int
		release  int
	}{
		"exact release": {reserved: maxOutstandingPerTenant, release: maxOutstandingPerTenant},
		// The queue holds 2 slots, so it subtracts only 2 from the gauge and logs the other 3 as a leak.
		"over-release": {reserved: 2, release: 5},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			q, unusedSlots := newTestQueue(t, maxOutstandingPerTenant)

			require.NoError(t, q.SubmitReserveSlots("tenant-a", testCase.reserved))
			require.NoError(t, q.SubmitReleaseSlots("tenant-a", testCase.release))
			require.Zero(t, testutil.ToFloat64(unusedSlots.WithLabelValues("tenant-a")))
			require.NoError(t, q.SubmitReserveSlots("tenant-a", maxOutstandingPerTenant))
		})
	}
}

func TestQueue_SubmitReserveSlots_RejectionIsCountedAndReturnsOccupancy(t *testing.T) {
	var logs concurrency.SyncBuffer
	q, metrics := newTestQueueWith(t, 5, testQueueOptions{logger: log.NewLogfmtLogger(&logs)})
	t.Cleanup(func() { q.EmptyQueue(t) })

	require.NoError(t, q.SubmitItemToEnqueue("tenant-a", "plain-1", "", 0, func() {}))
	require.NoError(t, q.SubmitReserveSlots("tenant-a", 3))
	err := q.SubmitReserveSlots("tenant-a", 3)

	var rejected *ReservationRejectedError
	require.ErrorAs(t, err, &rejected)
	require.Equal(t, ReservationRejectedError{Requested: 3, Queued: 1, Reserved: 3, MaxTenantQueueSize: 5}, *rejected)
	require.ErrorIs(t, err, ErrTooManyRequests)
	// The scheduler logs the rejection from the error. A log call in dispatcherLoop stalls every producer.
	require.Empty(t, logs.String())

	// The queue observes reservationSize only for an admitted reservation, so the rejected
	// reservation adds no sample.
	requireReservationSize := func() {
		t.Helper()
		var m dto.Metric
		require.NoError(t, metrics.reservationSize.Write(&m))
		require.Equal(t, uint64(1), m.GetHistogram().GetSampleCount())
		require.Equal(t, 3.0, m.GetHistogram().GetSampleSum())
	}
	requireReservationSize()
	require.Equal(t, 3.0, testutil.ToFloat64(metrics.discardedRequests.WithLabelValues("tenant-a")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.discardedReservations.WithLabelValues("tenant-a")))

	// An invalid count is a caller bug, not a full queue, so it discards nothing.
	require.ErrorIs(t, q.SubmitReserveSlots("tenant-a", 0), errInvalidReservationCount)
	requireReservationSize()
	require.Equal(t, 3.0, testutil.ToFloat64(metrics.discardedRequests.WithLabelValues("tenant-a")))
	require.Equal(t, 1.0, testutil.ToFloat64(metrics.discardedReservations.WithLabelValues("tenant-a")))
}

func TestQueue_SubmitItemIntoSlot_ItemIsDequeuable(t *testing.T) {
	q, _ := newTestQueue(t, 5)
	require.NoError(t, q.SubmitReserveSlots("tenant-a", 1))
	require.NoError(t, q.SubmitItemIntoSlot("tenant-a", "item-1", "", 0, func() {}))

	conn := NewUnregisteredConsumerWorkerConn(t.Context(), "consumer-1")
	require.NoError(t, q.AwaitRegisterConsumerWorkerConn(conn))

	dequeueReq := NewConsumerWorkerDequeueRequest(conn, FirstTenant())
	item, _, err := q.AwaitItemForConsumer(dequeueReq)
	require.NoError(t, err)
	require.Equal(t, Item("item-1"), item)
}

func TestQueue_SubmitDeleteUnusedSlotsGauge_DefersDeleteUntilLastSlotIsReleased(t *testing.T) {
	q, unusedSlots := newTestQueue(t, 5)
	require.NoError(t, q.SubmitReserveSlots("tenant-a", 2))

	require.NoError(t, q.SubmitDeleteUnusedSlotsGauge("tenant-a"))
	require.Equal(t, 1, testutil.CollectAndCount(unusedSlots), "a reserved tenant must keep its gauge")

	require.NoError(t, q.SubmitReleaseSlots("tenant-a", 1))
	require.Equal(t, 1, testutil.CollectAndCount(unusedSlots), "the tenant still holds a slot")

	// No later cleanup comes for the tenant, so the release of its last slot deletes the gauge.
	require.NoError(t, q.SubmitReleaseSlots("tenant-a", 1))
	require.Zero(t, testutil.CollectAndCount(unusedSlots), "the deferred delete must run when the last slot goes")

	// The dispatcher has exited, so the test can read its state.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), q))
	require.Empty(t, q.deferredUnusedSlotsGaugeDeletes, "the deferred delete must forget the tenant")
}

func TestQueue_SubmitDeleteUnusedSlotsGauge_DeletesUnreservedSeries(t *testing.T) {
	q, unusedSlots := newTestQueue(t, 5)
	require.NoError(t, q.SubmitReserveSlots("tenant-a", 2))
	require.NoError(t, q.SubmitReleaseSlots("tenant-a", 2))
	require.Equal(t, 1, testutil.CollectAndCount(unusedSlots))

	require.NoError(t, q.SubmitDeleteUnusedSlotsGauge("tenant-a"))
	require.Zero(t, testutil.CollectAndCount(unusedSlots), "a tenant with no reservation must lose its gauge")
}

func TestQueue_SubmitDeleteUnusedSlotsGauge_LastItemRunsDeferredDelete(t *testing.T) {
	q, unusedSlots := newTestQueue(t, 5)
	t.Cleanup(func() { q.EmptyQueue(t) })
	require.NoError(t, q.SubmitReserveSlots("tenant-a", 1))
	require.NoError(t, q.SubmitDeleteUnusedSlotsGauge("tenant-a"))

	require.NoError(t, q.SubmitItemIntoSlot("tenant-a", "item-1", "", 0, func() {}))
	require.Zero(t, testutil.CollectAndCount(unusedSlots), "the insert that uses the last slot must run the deferred delete")
}

func TestQueue_SubmitDeleteUnusedSlotsGauge_NewReservationCancelsDeferredDelete(t *testing.T) {
	q, unusedSlots := newTestQueue(t, 5)
	require.NoError(t, q.SubmitReserveSlots("tenant-a", 2))
	require.NoError(t, q.SubmitDeleteUnusedSlotsGauge("tenant-a"))

	// The tenant is active again, so the gauge stays for the tenant's next cleanup.
	require.NoError(t, q.SubmitReserveSlots("tenant-a", 1))
	require.NoError(t, q.SubmitReleaseSlots("tenant-a", 3))
	require.Equal(t, 1, testutil.CollectAndCount(unusedSlots))
	require.Zero(t, testutil.ToFloat64(unusedSlots.WithLabelValues("tenant-a")))
}

func TestQueue_SubmitItemIntoSlot_UpdatesTenantSeriesBeforeSuccessFn(t *testing.T) {
	maxQueueLength := NewMaxQueueLengthGauge("test_max_queue_length", "", nil)
	q, metrics := newTestQueueWith(t, 5, testQueueOptions{maxQueueLength: maxQueueLength})
	t.Cleanup(func() { q.EmptyQueue(t) })

	require.NoError(t, q.SubmitReserveSlots("tenant-a", 1))
	// successFn marks the tenant active, and the inactive-user cleanup deletes these series. So each
	// series must already include the item when successFn runs on the dispatcher goroutine.
	var depth int
	var length, reserved float64
	require.NoError(t, q.SubmitItemIntoSlot("tenant-a", "item-1", "", 0, func() {
		length = testutil.ToFloat64(metrics.queueLength.WithLabelValues("tenant-a"))
		reserved = testutil.ToFloat64(metrics.unusedSlots.WithLabelValues("tenant-a"))
		maxQueueLength.mtx.Lock()
		defer maxQueueLength.mtx.Unlock()
		if d := maxQueueLength.tenants["tenant-a"]; d != nil {
			depth = d.current
		}
	}))
	require.Equal(t, 1, depth, "the max queue length must include the item before successFn")
	require.Equal(t, 1.0, length, "the queue length must include the item before successFn")
	require.Zero(t, reserved, "the reserved slots must exclude the used slot before successFn")
}

func TestQueue_StoppedQueue_RejectsReservationCalls(t *testing.T) {
	q, _ := newTestQueue(t, 5)
	require.NoError(t, services.StopAndAwaitTerminated(t.Context(), q))

	require.ErrorIs(t, q.SubmitReserveSlots("tenant-a", 1), ErrStopped)
	require.ErrorIs(t, q.SubmitReleaseSlots("tenant-a", 1), ErrStopped)
	require.ErrorIs(t, q.SubmitItemIntoSlot("tenant-a", "item-1", "", 0, func() {}), ErrStopped)
	require.ErrorIs(t, q.SubmitDeleteUnusedSlotsGauge("tenant-a"), ErrStopped)
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
		maxReservationSize      = 5
		consumers               = 2
	)
	tenants := []string{"tenant-a", "tenant-b", "tenant-c"}

	q, unusedSlots := newTestQueue(t, maxOutstandingPerTenant)

	var (
		mtx                 sync.Mutex
		accepted            int
		dequeued            int
		rejected            atomic.Int64
		reservationRejected atomic.Int64
		admitted            atomic.Int64
		maxOccupancy        atomic.Int64
	)
	// checkOccupancy runs as the successFn of each insert. successFn runs on the dispatcher
	// goroutine, so it can read the broker state without a race.
	checkOccupancy := func(tenantID string) func() {
		return func() {
			occupancy := int64(q.queueBroker.tenantOccupancy(tenantID))
			for {
				current := maxOccupancy.Load()
				if occupancy <= current || maxOccupancy.CompareAndSwap(current, occupancy) {
					return
				}
			}
		}
	}

	consumerCtx, stopConsumers := context.WithCancel(context.Background())
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
				_, next, err := q.AwaitItemForConsumer(NewConsumerWorkerDequeueRequest(conn, idx))
				if err != nil {
					return
				}
				idx = next
				mtx.Lock()
				dequeued++
				mtx.Unlock()
				time.Sleep(consumerDelay)
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
				err := q.SubmitItemToEnqueue(tenantID, i, "", 0, checkOccupancy(tenantID))
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
				size := 1 + rnd.Intn(maxReservationSize)
				err := q.SubmitReserveSlots(tenantID, size)
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
				// Insert 0 to size items, then release the unused slots, as an abort does.
				inserted := rnd.Intn(size + 1)
				for i := 0; i < inserted; i++ {
					if err := q.SubmitItemIntoSlot(tenantID, i, "", 0, checkOccupancy(tenantID)); err != nil {
						errs <- fmt.Errorf("a reserved item was rejected: %w", err)
						return
					}
					mtx.Lock()
					accepted++
					mtx.Unlock()
				}
				if err := q.SubmitReleaseSlots(tenantID, size-inserted); err != nil {
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
	require.LessOrEqual(t, maxOccupancy.Load(), int64(maxOutstandingPerTenant), "queued items plus reservations must never exceed the cap")

	// The dispatcher goroutine has exited, so the broker state is safe to read here.
	for _, tenantID := range tenants {
		require.Zero(t, testutil.ToFloat64(unusedSlots.WithLabelValues(tenantID)), tenantID)
	}
	require.Empty(t, q.queueBroker.reservedByTenant)
	require.Empty(t, q.queueBroker.tenantConsumerAssignments.tenantsByID)
	for _, id := range q.queueBroker.tenantConsumerAssignments.queuingAlgorithm.TenantIDOrder() {
		require.Empty(t, id, "no tenant may stay in the tenant order after the queue drains")
	}
}
