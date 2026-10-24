// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
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
// item. path=reservation uses one SubmitReserveSlots plus one SubmitItemIntoSlot per item. Compare
// the two paths with `benchstat -col /path`.
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

				queued := 0
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if path == "reservation" {
						require.NoError(b, q.SubmitReserveSlots("tenant-a", items))
						for j := 0; j < items; j++ {
							require.NoError(b, q.SubmitItemIntoSlot("tenant-a", j, "", 0, successFn))
						}
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

		for _, opName := range []string{"enqueue", "enqueueIntoSlot", "reserveRejected"} {
			b.Run(fmt.Sprintf("tenants=%d/op=%s", tenants, opName), func(b *testing.B) {
				maxOutstandingPerTenant := 1 << 30
				if opName == "reserveRejected" {
					maxOutstandingPerTenant = rejectCap
				}
				q := newUnstartedTestQueue(b, maxOutstandingPerTenant)
				conn := NewUnregisteredConsumerWorkerConn(context.Background(), "bench-consumer")
				q.queueBroker.addConsumerWorkerConn(conn)
				successFn := lockingSuccessFn()

				var op producerOpKind
				var count int
				switch opName {
				case "enqueue":
					op = opEnqueue
				case "enqueueIntoSlot":
					op = opEnqueueIntoSlot
					for _, tenantID := range tenantIDs {
						require.NoError(b, q.processProducerOperation(producerOperation{op: opReserveSlots, tenantID: tenantID, count: b.N/tenants + 1}))
					}
				case "reserveRejected":
					op = opReserveSlots
					count = 1
					for _, tenantID := range tenantIDs {
						require.NoError(b, q.processProducerOperation(producerOperation{op: opEnqueue, tenantID: tenantID, item: 0, successFn: successFn}))
						require.NoError(b, q.processProducerOperation(producerOperation{op: opReserveSlots, tenantID: tenantID, count: rejectCap - 1}))
					}
					require.ErrorIs(b, q.processProducerOperation(producerOperation{op: op, tenantID: tenantIDs[0], count: count}), ErrTooManyRequests)
				}
				wantRejected := opName == "reserveRejected"

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					err := q.processProducerOperation(producerOperation{op: op, tenantID: tenantIDs[i%tenants], item: i, count: count, successFn: successFn})
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
