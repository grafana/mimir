// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"sync"
	"testing"

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

func TestSchedulerQueue_EnqueueIntoSlot_AppliesMaxQueriersPerTenant(t *testing.T) {
	const subRequests = 10
	// One querier per tenant, out of two connected queriers.
	sq := newTestSchedulerQueueWithLimits(t, &limits{queriers: 1})

	ctx, cancel := context.WithCancel(context.Background())
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
		require.NoError(t, sq.ReserveSlots("tenant-a", 1))
		require.NoError(t, sq.EnqueueIntoSlot(&SchedulerRequest{UserID: "tenant-a", QueryID: uint64(i)}, func() {}))
		receivers[<-received]++
	}
	require.Len(t, receivers, 1, "every sub-request must go to the tenant's only querier, got %v", receivers)
}
