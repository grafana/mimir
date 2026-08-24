// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
)

// BenchmarkScheduler_ObserveQueueMaxWait measures the cost of a single call to
// observeQueueMaxWait at varying inflight sizes. Multiply the per-op time by the
// configured ticker rate (250ms in production) to estimate steady-state overhead.
func BenchmarkScheduler_ObserveQueueMaxWait(b *testing.B) {
	for _, n := range []int{100, 1_000, 10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("inflight=%d", n), func(b *testing.B) {
			s := newBenchScheduler(b, true)
			fillInflight(b, s, n)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.observeQueueMaxWait()
			}
		})
	}
}

// BenchmarkScheduler_InflightChurn measures throughput of the enqueue/dequeue paths
// (addRequestToPending + cancelRequestAndRemoveFromPending) under concurrency, with
// and without the queue-max-wait observer running on its production 250ms cadence.
//
// Use a long -benchtime (e.g. 5s) so the ticker fires enough times for the
// observer's impact to land in the measurement window.
func BenchmarkScheduler_InflightChurn(b *testing.B) {
	const (
		frontendAddr     = "frontend-bench"
		observerInterval = 250 * time.Millisecond
	)

	for _, inflight := range []int{100, 1_000, 10_000} {
		for _, observer := range []bool{false, true} {
			name := fmt.Sprintf("inflight=%d/observer=%v", inflight, observer)
			b.Run(name, func(b *testing.B) {
				s := newBenchScheduler(b, observer)
				fillInflight(b, s, inflight)

				if observer {
					stop := make(chan struct{})
					var wg sync.WaitGroup
					wg.Go(func() {
						t := time.NewTicker(observerInterval)
						defer t.Stop()
						for {
							select {
							case <-stop:
								return
							case <-t.C:
								s.observeQueueMaxWait()
							}
						}
					})
					b.Cleanup(func() {
						close(stop)
						wg.Wait()
					})
				}

				var nextID atomic.Uint64
				nextID.Store(uint64(inflight))

				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						req := newBenchRequest(frontendAddr, nextID.Add(1))
						s.addRequestToPending(req)
						s.cancelRequestAndRemoveFromPending(req.Key(), "bench")
					}
				})
			})
		}
	}
}

// newBenchScheduler builds a minimal Scheduler with only the state the
// benchmarked inflight-tracking methods touch. We avoid NewScheduler because
// it spins up a services.Manager whose listener goroutines would leak unless
// the service is started and stopped — overhead we don't want in benchmarks.
func newBenchScheduler(_ *testing.B, observerEnabled bool) *Scheduler {
	s := &Scheduler{
		cfg:                           Config{QueueMaxWaitMetricEnabled: observerEnabled},
		schedulerInflightRequests:     map[RequestKey]*SchedulerRequest{},
		schedulerInflightRequestCount: atomic.NewInt64(0),
	}
	if observerEnabled {
		r := prometheus.NewRegistry()
		s.queueMaxWait = promauto.With(r).NewGauge(prometheus.GaugeOpts{Name: "bench_queue_max_wait_seconds"})
	}
	return s
}

func newBenchRequest(frontendAddr string, queryID uint64) *SchedulerRequest {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &SchedulerRequest{
		FrontendAddr: frontendAddr,
		QueryID:      queryID,
		EnqueueTime:  time.Now(),
		Ctx:          ctx,
		CancelFunc:   cancel,
	}
}

func fillInflight(b *testing.B, s *Scheduler, n int) {
	b.Helper()
	for i := range n {
		s.addRequestToPending(newBenchRequest("frontend-prefill", uint64(i)))
	}
}
