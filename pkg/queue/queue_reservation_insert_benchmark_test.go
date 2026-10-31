// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
)

// The production scale of a reservation, for the benchmarks below. benchSlots is the
// -querier.max-query-parallelism default. The open-reservation counts are the 7-day peaks of in-flight parent queries per query-scheduler.
const (
	benchSlots                 = 14
	benchOpenReservationsQuery = 6000
	benchOpenReservationsRuler = 25000
)

// lockingSuccessFn takes a mutex, as the scheduler's successFn does through addRequestToPending.
func lockingSuccessFn() func() {
	var mtx sync.Mutex
	return func() {
		mtx.Lock()
		mtx.Unlock() //nolint:staticcheck
	}
}

// BenchmarkQueueAdmission measures the caller-side cost to admit one parent query of `items` items:
// the channel round trips to the dispatcher loop plus the work inside it. path=plain uses one SubmitItemToEnqueue per
// item. path=reservation uses one SubmitOpenReservation, one SubmitItemIntoReservation per item and one
// SubmitCompleteReservation. Compare the two paths with `benchstat -col /path`.
//
// Each item is a separate producer operation on both paths, as the scheduler submits it. The timer
// covers the whole loop. The queue drains outside the timer once drainThreshold items are queued.
// BenchmarkProcessProducerOperation measures the in-loop cost per item.
func BenchmarkQueueAdmission(b *testing.B) {
	const drainThreshold = 10000

	for _, path := range []string{"plain", "reservation"} {
		for _, items := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("path=%s/items=%d", path, items), func(b *testing.B) {
				q := newUnstartedTestQueue(b, 1<<30)
				require.NoError(b, services.StartAndAwaitRunning(context.Background(), q))
				b.Cleanup(func() {
					require.NoError(b, services.StopAndAwaitTerminated(context.Background(), q))
				})
				conn := NewUnregisteredConsumerWorkerConn(context.Background(), "bench-consumer")
				require.NoError(b, q.AwaitRegisterConsumerWorkerConn(conn))
				b.Cleanup(func() { q.SubmitUnregisterConsumerWorkerConn(conn) })
				successFn := lockingSuccessFn()
				reservedSuccessFn := func(*Reservation) { successFn() }

				queued := 0
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if path == "reservation" {
						key := ReservationKey{Owner: "bench-frontend", ID: strconv.Itoa(i)}
						require.NoError(b, q.SubmitOpenReservation(key, "tenant-a", items, time.Now()))
						for j := 0; j < items; j++ {
							require.NoError(b, q.SubmitItemIntoReservation(key, "tenant-a", j, "", 0, time.Now(), reservedSuccessFn))
						}
						_, err := q.SubmitCompleteReservation(key)
						require.NoError(b, err)
					} else {
						for j := 0; j < items; j++ {
							require.NoError(b, q.SubmitItemToEnqueue("tenant-a", j, "", 0, successFn))
						}
					}
					queued += items

					if queued >= drainThreshold {
						b.StopTimer()
						drainQueueForBenchmark(b, q, conn, queued)
						b.StartTimer()
						queued = 0
					}
				}

				// A stopping queue waits for a consumer to drain every queued item.
				b.StopTimer()
				drainQueueForBenchmark(b, q, conn, queued)
			})
		}
	}
}

// BenchmarkProcessProducerOperation measures the work that one producer operation does inside the
// dispatcher loop, with no channel hand-off. Each iteration moves on to the next tenant. The Queue
// is not started, so this goroutine is the only one that touches the queue state.
//
// op=reserveRejected measures a reservation that the cap rejects, for tenants that sit at the cap.
func BenchmarkProcessProducerOperation(b *testing.B) {
	const (
		// drainEvery exceeds the largest tenant count, so every tenant holds queued items when the
		// loop rotates over them.
		drainEvery = 20000
		// rejectCap is the per-tenant cap of op=reserveRejected. The cost of a rejection does not depend on it.
		rejectCap = 5
	)

	for _, tenants := range []int{1, 1000, 10000} {
		tenantIDs := make([]string, tenants)
		for i := range tenantIDs {
			tenantIDs[i] = "tenant-" + strconv.Itoa(i)
		}

		for _, opName := range []string{"enqueue", "enqueueIntoReservation", "reserveRejected"} {
			b.Run(fmt.Sprintf("tenants=%d/op=%s", tenants, opName), func(b *testing.B) {
				maxOutstandingPerTenant := 1 << 30
				if opName == "reserveRejected" {
					maxOutstandingPerTenant = rejectCap
				}
				q := newUnstartedTestQueue(b, maxOutstandingPerTenant)
				conn := NewUnregisteredConsumerWorkerConn(context.Background(), "bench-consumer")
				q.queueBroker.addConsumerWorkerConn(conn)
				successFn := lockingSuccessFn()
				reservedSuccessFn := func(*Reservation) { successFn() }
				now := time.Now()
				keys := make([]ReservationKey, tenants)
				for i := range keys {
					keys[i] = ReservationKey{Owner: "bench-frontend", ID: tenantIDs[i]}
				}
				var op producerOpKind
				var slots int
				switch opName {
				case "enqueue":
					op = opEnqueue
				case "enqueueIntoReservation":
					op = opEnqueueIntoReservation
					for i, tenantID := range tenantIDs {
						size := b.N/tenants + 1
						require.NoError(b, q.processProducerOperation(producerOperation{op: opOpenReservation, reservationKey: keys[i], tenantID: tenantID, slots: size, now: now}))
					}
				case "reserveRejected":
					op = opOpenReservation
					slots = 1
					for i, tenantID := range tenantIDs {
						require.NoError(b, q.processProducerOperation(producerOperation{op: opEnqueue, tenantID: tenantID, item: 0, successFn: successFn}))
						require.NoError(b, q.processProducerOperation(producerOperation{op: opOpenReservation, reservationKey: keys[i], tenantID: tenantID, slots: rejectCap - 1, now: now}))
						// The rejected reservations use another key, because an open key fails the duplicate check first.
						keys[i].ID += "-rejected"
					}
					require.ErrorIs(b, q.processProducerOperation(producerOperation{op: op, reservationKey: keys[0], tenantID: tenantIDs[0], slots: slots, now: now}), ErrTooManyRequests)
				}
				wantRejected := opName == "reserveRejected"

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					t := i % tenants
					err := q.processProducerOperation(producerOperation{
						op: op, reservationKey: keys[t], tenantID: tenantIDs[t], item: i, slots: slots,
						now: now, successFn: successFn, reservedSuccessFn: reservedSuccessFn,
					})
					if (err != nil) != wantRejected {
						b.Fatalf("unexpected result: %v", err)
					}

					if !wantRejected && (i+1)%drainEvery == 0 {
						// Drain outside the timer, so the queue depth stays bounded.
						b.StopTimer()
						for !q.queueBroker.isEmpty() {
							_, _, _, err := q.queueBroker.dequeueItemForConsumer(NewConsumerWorkerDequeueRequest(conn, FirstTenant()))
							require.NoError(b, err)
						}
						b.StartTimer()
					}
				}
			})
		}
	}
}

// drainQueueForBenchmark dequeues count items through the running dispatcher loop.
func drainQueueForBenchmark(b *testing.B, q *Queue, conn *ConsumerWorkerConn, count int) {
	lastTenantIndex := FirstTenant()
	for i := 0; i < count; i++ {
		_, idx, err := q.AwaitItemForConsumer(NewConsumerWorkerDequeueRequest(conn, lastTenantIndex))
		require.NoError(b, err)
		lastTenantIndex = idx
	}
}

// BenchmarkDequeueReservationMember measures the dispatcher work to dequeue one item, for a plain
// item and for a member that returns its slot to an open reservation. reservations=0 dequeues
// plain items. Otherwise the items rotate over that many open reservations, so a large count
// shows the cache misses of the production scale. The Queue is not started, so this goroutine is
// the only one that touches the queue state.
func BenchmarkDequeueReservationMember(b *testing.B) {
	const refill = 10000

	for _, reservations := range []int{0, 1, benchOpenReservationsQuery, benchOpenReservationsRuler} {
		b.Run(fmt.Sprintf("reservations=%d", reservations), func(b *testing.B) {
			q := newUnstartedTestQueue(b, 1<<30)
			conn := NewUnregisteredConsumerWorkerConn(context.Background(), "bench-consumer")
			q.queueBroker.addConsumerWorkerConn(conn)
			now := time.Now()
			// One reservation must hold a whole refill in its slots.
			slots := benchSlots
			if reservations == 1 {
				slots = refill
			}
			keys := openBenchReservations(b, q, reservations, 1, slots, now)
			fill := func() {
				for i := 0; i < refill; i++ {
					op := producerOperation{op: opEnqueue, tenantID: "tenant-0", item: i}
					if reservations > 0 {
						op = producerOperation{op: opEnqueueIntoReservation, reservationKey: keys[i%reservations], tenantID: "tenant-0", item: i, now: now}
					}
					require.NoError(b, q.processProducerOperation(op))
				}
			}
			fill()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if q.queueBroker.isEmpty() {
					b.StopTimer()
					fill()
					b.StartTimer()
				}
				dequeueForBenchmark(b, q, conn)
			}
		})
	}
}

// BenchmarkEnqueueIntoReservationAtScale measures one opEnqueueIntoReservation inside the
// dispatcher loop while many reservations are open. Each iteration moves on to the next
// reservation, and reservation i belongs to tenant i%tenants. Every insert uses a slot. The Queue
// is not started, so this goroutine is the only one that touches the queue state.
func BenchmarkEnqueueIntoReservationAtScale(b *testing.B) {
	// drainEvery queues at most ceil(drainEvery/reservations) items of one reservation between
	// drains, which is less than benchSlots, so every insert finds an idle slot.
	const drainEvery = 20000

	for _, reservations := range []int{benchOpenReservationsQuery, benchOpenReservationsRuler} {
		for _, tenants := range []int{1, 1000, 10000} {
			if tenants > reservations {
				// Some tenants would hold no reservation, so the run would repeat a smaller tenant count.
				continue
			}
			b.Run(fmt.Sprintf("reservations=%d/tenants=%d", reservations, tenants), func(b *testing.B) {
				q := newUnstartedTestQueue(b, 1<<30)
				conn := NewUnregisteredConsumerWorkerConn(context.Background(), "bench-consumer")
				q.queueBroker.addConsumerWorkerConn(conn)
				successFn := lockingSuccessFn()
				reservedSuccessFn := func(*Reservation) { successFn() }
				now := time.Now()
				keys := openBenchReservations(b, q, reservations, tenants, benchSlots, now)
				tenantIDs := make([]string, reservations)
				for i := range tenantIDs {
					tenantIDs[i] = "tenant-" + strconv.Itoa(i%tenants)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					r := i % reservations
					err := q.processProducerOperation(producerOperation{
						op: opEnqueueIntoReservation, reservationKey: keys[r], tenantID: tenantIDs[r], item: i,
						now: now, reservedSuccessFn: reservedSuccessFn,
					})
					if err != nil {
						b.Fatalf("unexpected error: %v", err)
					}

					if (i+1)%drainEvery == 0 {
						b.StopTimer()
						for !q.queueBroker.isEmpty() {
							dequeueForBenchmark(b, q, conn)
						}
						b.StartTimer()
					}
				}
			})
		}
	}
}

// openBenchReservations opens count reservations, where reservation i belongs to tenant i%tenants.
func openBenchReservations(b *testing.B, q *Queue, count, tenants, slots int, now time.Time) []ReservationKey {
	keys := make([]ReservationKey, count)
	for i := range keys {
		keys[i] = ReservationKey{Owner: "bench-frontend", ID: strconv.Itoa(i)}
		tenantID := "tenant-" + strconv.Itoa(i%tenants)
		require.NoError(b, q.processProducerOperation(producerOperation{op: opOpenReservation, reservationKey: keys[i], tenantID: tenantID, slots: slots, now: now}))
	}
	return keys
}

// dequeueForBenchmark does the queue work of one dequeue in trySendNextItemForConsumer, without the
// hand-off to a consumer.
func dequeueForBenchmark(b *testing.B, q *Queue, conn *ConsumerWorkerConn) {
	dequeued, _, _, err := q.queueBroker.dequeueItemForConsumer(NewConsumerWorkerDequeueRequest(conn, FirstTenant()))
	if err != nil {
		b.Fatal(err)
	}
	if dequeued.reservation != nil {
		q.returnReservedSlot(dequeued.reservation)
	}
}

// BenchmarkReapBusyCohort measures reaper ticks after a cohort of busy reservations opens in one
// tick, with the reaper interval of the scheduler and the default timeout. Every reservation keeps
// an inflight member. The benchmark reports the slowest tick as max-ns/tick, because a stall of
// the dispatcher loop matters more than the mean. b.N must cover several timeouts for the maximum
// to show the steady state.
func BenchmarkReapBusyCohort(b *testing.B) {
	const (
		tick    = 250 * time.Millisecond
		timeout = 5 * time.Second
	)

	for _, reservations := range []int{benchOpenReservationsQuery, benchOpenReservationsRuler} {
		b.Run(fmt.Sprintf("reservations=%d", reservations), func(b *testing.B) {
			q := newUnstartedTestQueue(b, 1<<30)
			start := time.Unix(1000, 0)
			for _, key := range openBenchReservations(b, q, reservations, 1, benchSlots, start) {
				q.reservations[key].inflight.Store(1)
			}
			result := &producerOpResult{}
			// The first tick reschedules the whole cohort, so the timed ticks start after it.
			require.NoError(b, q.processProducerOperation(producerOperation{op: opReapReservations, now: start, timeout: timeout, result: result}))

			var slowest time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				began := time.Now()
				if err := q.processProducerOperation(producerOperation{op: opReapReservations, now: start.Add(time.Duration(i+1) * tick), timeout: timeout, result: result}); err != nil {
					b.Fatal(err)
				}
				slowest = max(slowest, time.Since(began))
			}
			b.StopTimer()
			require.Empty(b, result.closed)
			b.ReportMetric(float64(slowest.Nanoseconds()), "max-ns/tick")
		})
	}
}

// BenchmarkReapReservations measures one reaper tick inside the dispatcher loop. state=noneDue is
// the steady state, where no reservation is due. state=allDueBusy is the worst case, where every
// reservation is due and has an inflight member, so the reaper reschedules all of them.
func BenchmarkReapReservations(b *testing.B) {
	const timeout = time.Second

	for _, reservations := range []int{1, benchOpenReservationsQuery, benchOpenReservationsRuler} {
		for _, state := range []string{"noneDue", "allDueBusy"} {
			b.Run(fmt.Sprintf("reservations=%d/state=%s", reservations, state), func(b *testing.B) {
				q := newUnstartedTestQueue(b, 1<<30)
				start := time.Unix(1000, 0)
				for i := 0; i < reservations; i++ {
					key := ReservationKey{Owner: "bench-frontend", ID: strconv.Itoa(i)}
					require.NoError(b, q.processProducerOperation(producerOperation{op: opOpenReservation, reservationKey: key, tenantID: "tenant-a", slots: benchSlots, now: start}))
					if state == "allDueBusy" {
						q.reservations[key].inflight.Store(1)
					}
				}
				// The first tick moves every reservation to its real next check.
				result := &producerOpResult{}
				require.NoError(b, q.processProducerOperation(producerOperation{op: opReapReservations, now: start, timeout: timeout, result: result}))
				require.Empty(b, result.closed)

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					now := start
					if state == "allDueBusy" {
						now = start.Add(time.Duration(i+1) * timeout)
					}
					if err := q.processProducerOperation(producerOperation{op: opReapReservations, now: now, timeout: timeout, result: result}); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				require.Empty(b, result.closed)
			})
		}
	}
}
