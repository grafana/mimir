// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/queue"
)

func newTestSchedulerQueueWithLimits(t *testing.T, l Limits) *schedulerQueue {
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = 5
	return newTestSchedulerQueueWithConfig(t, cfg, l)
}

func newTestSchedulerQueueWithConfig(t *testing.T, cfg Config, l Limits) *schedulerQueue {
	sq, err := newSchedulerQueue(
		cfg,
		l,
		log.NewNopLogger(),
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{Name: "test_gauge"}, []string{"user"}),
		nil,
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{Name: "test_counter"}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{Name: "test_histogram"}),
		promauto.With(nil).NewSummaryVec(prometheus.SummaryOpts{Name: "test_summary"}, []string{"query_component"}),
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), sq))
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanup runs, so stop with a background context.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), sq))
	})
	return sq
}

func TestSchedulerQueue_EnqueueIntoReservation_AppliesMaxQueriersPerTenant(t *testing.T) {
	const subRequests = 10
	// One querier per tenant, out of two connected queriers.
	sq := newTestSchedulerQueueWithLimits(t, &limits{queriers: 1})

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	received := make(chan string)
	var conns []*queue.ConsumerWorkerConn
	for _, querierID := range []string{"querier-1", "querier-2"} {
		conn := queue.NewUnregisteredConsumerWorkerConn(ctx, querierID)
		require.NoError(t, sq.AwaitRegisterQuerierWorkerConn(conn))
		conns = append(conns, conn)
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := queue.FirstTenant()
			for {
				_, next, err := sq.AwaitRequestForQuerier(queue.NewConsumerWorkerDequeueRequest(conn, idx))
				if err != nil {
					return
				}
				idx = next
				select {
				case received <- querierID:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
		for _, conn := range conns {
			sq.SubmitUnregisterQuerierWorkerConn(conn)
		}
	})

	receivers := map[string]int{}
	for i := 0; i < subRequests; i++ {
		key := queue.ReservationKey{Owner: "frontend-1", ID: strconv.Itoa(i)}
		require.NoError(t, sq.OpenReservation(key, "tenant-a", "", 1, time.Now()))
		require.NoError(t, sq.EnqueueIntoReservation(key, &SchedulerRequest{UserID: "tenant-a", QueryID: uint64(i)}, time.Now(), func(*queue.Reservation) {}))
		receivers[<-received]++
		_, err := sq.CompleteReservation(key)
		require.NoError(t, err)
	}
	require.Len(t, receivers, 1, "every sub-request must go to the tenant's only querier, got %v", receivers)
}

// With -query-scheduler.fixed-query-component-priority-enabled, worker 0 of a querier dequeues from the ingester queue first.
// Without it, worker 0 dequeues from the queue that the tree created first.
func TestSchedulerQueue_FixedQueryComponentPriority(t *testing.T) {
	for _, tc := range []struct {
		enabled  bool
		expected string
	}{
		{enabled: false, expected: storeGatewayQueueDimension},
		{enabled: true, expected: ingesterQueueDimension},
	} {
		t.Run(fmt.Sprintf("enabled=%t", tc.enabled), func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			cfg.MaxOutstandingPerTenant = 5
			cfg.FixedQueryComponentPriorityEnabled = tc.enabled
			sq := newTestSchedulerQueueWithConfig(t, cfg, &limits{})

			for i, dim := range []string{storeGatewayQueueDimension, ingesterQueueDimension} {
				require.NoError(t, sq.Enqueue(&SchedulerRequest{UserID: "tenant-a", QueryID: uint64(i), AdditionalQueueDimensions: []string{dim}}, func() {}))
			}
			conn := queue.NewUnregisteredConsumerWorkerConn(t.Context(), "querier-1")
			require.NoError(t, sq.AwaitRegisterQuerierWorkerConn(conn))
			t.Cleanup(func() { sq.SubmitUnregisterQuerierWorkerConn(conn) })

			// Dequeue both requests, because the queue does not stop while it holds a request.
			var dequeued []string
			idx := queue.FirstTenant()
			for range 2 {
				item, next, err := sq.AwaitRequestForQuerier(queue.NewConsumerWorkerDequeueRequest(conn, idx))
				require.NoError(t, err)
				req, ok := item.(*SchedulerRequest)
				require.True(t, ok)
				dequeued = append(dequeued, req.ExpectedQueryComponentName())
				idx = next
			}
			require.Equal(t, tc.expected, dequeued[0])
		})
	}
}
