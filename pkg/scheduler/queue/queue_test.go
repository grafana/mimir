// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/scheduler/queue/queue_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	util_test "github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	util_test.VerifyNoLeakTestMain(m)
}

func BenchmarkConcurrentQueueOperations(b *testing.B) {
	req := "the query request"
	maxQueriers := 0

	for _, numTenants := range []int{1, 10, 1000} {
		b.Run(fmt.Sprintf("%v tenants", numTenants), func(b *testing.B) {
			for _, numProducers := range []int{10, 25} { // Query-frontends run 5 parallel streams per scheduler by default, and we typically see 2-5 frontends running at any one time.
				b.Run(fmt.Sprintf("%v concurrent producers", numProducers), func(b *testing.B) {
					for _, numConsumers := range []int{16, 160, 1600} { // Queriers run with parallelism of 8 when query sharding is enabled.
						b.Run(fmt.Sprintf("%v concurrent consumers", numConsumers), func(b *testing.B) {
							queueLength := promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"})
							discardedRequests := promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"})
							enqueueDuration := promauto.With(nil).NewHistogram(prometheus.HistogramOpts{})
							queue := NewRequestQueue(100, 0, queueLength, discardedRequests, enqueueDuration)

							start := make(chan struct{})
							producersAndConsumers, ctx := errgroup.WithContext(context.Background())
							require.NoError(b, queue.starting(ctx))
							b.Cleanup(func() {
								require.NoError(b, queue.stop(nil))
							})

							requestCount := func(total int, instanceCount int, instanceIdx int) int {
								count := total / instanceCount
								totalCountWithoutAdjustment := count * instanceCount

								if totalCountWithoutAdjustment >= total {
									return count
								}

								additionalRequests := total - totalCountWithoutAdjustment

								if instanceIdx < additionalRequests {
									// If we can't perfectly spread requests across all instances, assign remaining requests to the first instances.
									return count + 1
								}

								return count
							}

							runProducer := func(producerIdx int) error {
								requestCount := requestCount(b.N, numProducers, producerIdx)
								tenantID := producerIdx % numTenants
								<-start

								for i := 0; i < requestCount; i++ {
									for {
										err := queue.EnqueueRequest(strconv.Itoa(tenantID), req, maxQueriers, func() {})
										if err == nil {
											break
										}

										// Keep retrying if we've hit the max queue length.
										if !errors.Is(err, ErrTooManyRequests) {
											return err
										}
									}

									tenantID = (tenantID + 1) % numTenants
								}

								return nil
							}

							for producerIdx := 0; producerIdx < numProducers; producerIdx++ {
								producerIdx := producerIdx
								producersAndConsumers.Go(func() error {
									return runProducer(producerIdx)
								})
							}

							runConsumer := func(consumerIdx int) error {
								requestCount := requestCount(b.N, numConsumers, consumerIdx)
								lastTenantIndex := FirstUser()
								querierID := fmt.Sprintf("consumer-%v", consumerIdx)
								queue.RegisterQuerierConnection(querierID)
								defer queue.UnregisterQuerierConnection(querierID)

								<-start

								for i := 0; i < requestCount; i++ {
									_, idx, err := queue.GetNextRequestForQuerier(ctx, lastTenantIndex, querierID)
									if err != nil {
										return err
									}

									lastTenantIndex = idx
								}

								return nil
							}

							for consumerIdx := 0; consumerIdx < numConsumers; consumerIdx++ {
								consumerIdx := consumerIdx
								producersAndConsumers.Go(func() error {
									return runConsumer(consumerIdx)
								})
							}

							b.ResetTimer()
							close(start)
							err := producersAndConsumers.Wait()
							if err != nil {
								require.NoError(b, err)
							}
						})
					}
				})
			}
		})
	}
}

func TestRequestQueue_GetNextRequestForQuerier_ShouldGetRequestAfterReshardingBecauseQuerierHasBeenForgotten(t *testing.T) {
	const forgetDelay = 3 * time.Second

	queue := NewRequestQueue(1, forgetDelay,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}))

	// Start the queue service.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, queue))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, queue))
	})

	// Two queriers connect.
	queue.RegisterQuerierConnection("querier-1")
	queue.RegisterQuerierConnection("querier-2")

	// Querier-2 waits for a new request.
	querier2wg := sync.WaitGroup{}
	querier2wg.Add(1)
	go func() {
		defer querier2wg.Done()
		_, _, err := queue.GetNextRequestForQuerier(ctx, FirstUser(), "querier-2")
		require.NoError(t, err)
	}()

	// Querier-1 crashes (no graceful shutdown notification).
	queue.UnregisterQuerierConnection("querier-1")

	// Enqueue a request from an user which would be assigned to querier-1.
	// NOTE: "user-1" hash falls in the querier-1 shard.
	require.NoError(t, queue.EnqueueRequest("user-1", "request", 1, nil))

	startTime := time.Now()
	querier2wg.Wait()
	waitTime := time.Since(startTime)

	// We expect that querier-2 got the request only after querier-1 forget delay is passed.
	assert.GreaterOrEqual(t, waitTime.Milliseconds(), forgetDelay.Milliseconds())
}
