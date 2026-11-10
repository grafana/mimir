// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// While one query component is slow, its queue does not empty.
// Requests for the other query component arrive in bursts.
// Their queue empties between bursts.
// With and without fixed priority queue dimensions, the queue must dequeue every request.
// The test logs the mean queue wait of the fast query component for each algorithm.
// It does not compare the means, because they change from run to run.
func TestQueue_FixedPriorityQueueDimensions_FastQueueWaitWhileOtherQueryComponentIsSlow(t *testing.T) {
	fixedPriorityQueueDimensions := []string{ingesterQueueDimension, storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension, UnknownDimension}

	for _, tc := range []struct {
		name               string
		fastQueueDimension string
		slowQueueDimension string
		sparseEvery        int
	}{
		{name: "store-gateway slow", fastQueueDimension: ingesterQueueDimension, slowQueueDimension: storeGatewayQueueDimension},
		{name: "ingester slow", fastQueueDimension: storeGatewayQueueDimension, slowQueueDimension: ingesterQueueDimension},
		{name: "store-gateway slow, sparse other dimensions", fastQueueDimension: ingesterQueueDimension, slowQueueDimension: storeGatewayQueueDimension, sparseEvery: 20},
		{name: "ingester slow, sparse other dimensions", fastQueueDimension: storeGatewayQueueDimension, slowQueueDimension: ingesterQueueDimension, sparseEvery: 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				dynamicWaits := fastQueueWaitsWhileOtherQueryComponentIsSlow(t, nil, tc.fastQueueDimension, tc.slowQueueDimension, tc.sparseEvery)
				fixedWaits := fastQueueWaitsWhileOtherQueryComponentIsSlow(t, fixedPriorityQueueDimensions, tc.fastQueueDimension, tc.slowQueueDimension, tc.sparseEvery)

				t.Logf("fast queue wait mean: dynamic priority %.4fs, fixed priority %.4fs", mean(dynamicWaits), mean(fixedWaits))
			})
		})
	}
}

// fastQueueWaitsWhileOtherQueryComponentIsSlow returns the queue wait in seconds of each request for fastQueueDimension.
func fastQueueWaitsWhileOtherQueryComponentIsSlow(t *testing.T, fixedPriorityQueueDimensions []string, fastQueueDimension, slowQueueDimension string, sparseEvery int) []float64 {
	const (
		tenantID     = "0"
		numConsumers = 4
		// numWorkersPerConsumer matches a querier with -querier.max-concurrent=8 and 2 query-schedulers.
		numWorkersPerConsumer = 4
		numWorkers            = numConsumers * numWorkersPerConsumer
		// maxConsumersPerTenant disables shuffle sharding.
		maxConsumersPerTenant = 0

		fastLatency = 1 * time.Millisecond
		slowLatency = 60 * fastLatency

		// burstSize requests every burstInterval need 5 of the 16 workers.
		burstSize     = 10
		burstInterval = 2 * time.Millisecond
		numBursts     = 150
		numSparse     = 2 * numBursts
		// slowBacklog is 2 times what all workers can complete while the bursts arrive.
		slowBacklog = 2 * numWorkers * int(numBursts*burstInterval/slowLatency)
	)

	queue, err := New(
		log.NewNopLogger(),
		slowBacklog+numBursts*burstSize+numSparse,
		0,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		nil,
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
		nil,
		fixedPriorityQueueDimensions,
	)
	require.NoError(t, err)
	require.NoError(t, queue.starting(t.Context()))

	fastDimension := func(string) []string { return []string{fastQueueDimension} }
	sparseDimensions := []string{ingesterAndStoreGatewayQueueDimension, UnknownDimension}
	sparseCount := 0
	slowDimension := func(string) []string { return []string{slowQueueDimension} }
	produceBurst := func() error {
		for range burstSize {
			if err := queueProduce(queue, maxConsumersPerTenant, tenantID, fastDimension); err != nil {
				return err
			}
		}
		return nil
	}
	for range slowBacklog {
		require.NoError(t, queueProduce(queue, maxConsumersPerTenant, tenantID, slowDimension))
	}
	require.NoError(t, produceBurst())

	var (
		mu        sync.Mutex
		fastWaits []float64
	)
	consume := func(request Item) error {
		req := request.(*testItem)
		if req.ExpectedQueryComponentName() != fastQueueDimension {
			time.Sleep(slowLatency)
			return nil
		}
		mu.Lock()
		fastWaits = append(fastWaits, time.Since(req.EnqueueTime).Seconds())
		mu.Unlock()
		time.Sleep(fastLatency)
		return nil
	}
	numSparseItems := 0
	if sparseEvery > 0 {
		numSparseItems = len(sparseDimensions) * ((numBursts-2)/sparseEvery + 1)
	}
	consumers, startConsumers := makeQueueConsumerGroup(t.Context(), queue, slowBacklog+numBursts*burstSize+numSparseItems, numConsumers, numWorkersPerConsumer, consume)
	close(startConsumers)

	var producers errgroup.Group
	producers.Go(func() error {
		for i := range numBursts - 1 {
			time.Sleep(burstInterval)
			if err := produceBurst(); err != nil {
				return err
			}
			if sparseEvery > 0 && i%sparseEvery == 0 {
				for _, dim := range sparseDimensions {
					if err := queueProduce(queue, maxConsumersPerTenant, tenantID, func(string) []string { return []string{dim} }); err != nil {
						return err
					}
					sparseCount++
				}
			}
		}
		return nil
	})
	require.NoError(t, producers.Wait())
	require.NoError(t, consumers.Wait())
	require.NoError(t, queue.stop(nil))

	require.Len(t, fastWaits, numBursts*burstSize)
	require.Equal(t, numSparseItems, sparseCount)
	return fastWaits
}

// With fixed priority, the queue counts the connected consumer-workers by their fixed priority queue dimension.
func TestQueue_FixedPriorityQueueDimensions_ConnectedConsumerWorkers(t *testing.T) {
	dimensions := []string{ingesterQueueDimension, storeGatewayQueueDimension, ingesterAndStoreGatewayQueueDimension, UnknownDimension}
	queue, err := New(
		log.NewNopLogger(),
		1,
		0,
		promauto.With(nil).NewGaugeVec(prometheus.GaugeOpts{}, []string{"user"}),
		nil,
		promauto.With(nil).NewCounterVec(prometheus.CounterOpts{}, []string{"user"}),
		promauto.With(nil).NewHistogram(prometheus.HistogramOpts{}),
		nil,
		dimensions,
	)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), queue))
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanup runs, so stop with a background context.
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), queue))
	})

	connected := func() map[string]float64 {
		m := map[string]float64{}
		for _, dim := range dimensions {
			m[dim] = queue.GetConnectedConsumerWorkersByFixedPriorityMetric(dim)
		}
		return m
	}

	var conns []*ConsumerWorkerConn
	for range 6 {
		conn := NewUnregisteredConsumerWorkerConn(t.Context(), "consumer-1")
		require.NoError(t, queue.AwaitRegisterConsumerWorkerConn(conn))
		conns = append(conns, conn)
	}
	require.Equal(t, map[string]float64{ingesterQueueDimension: 2, storeGatewayQueueDimension: 2, ingesterAndStoreGatewayQueueDimension: 1, UnknownDimension: 1}, connected())

	// The queue unregisters a connection asynchronously.
	queue.SubmitUnregisterConsumerWorkerConn(conns[0])
	require.Eventually(t, func() bool {
		return queue.GetConnectedConsumerWorkersByFixedPriorityMetric(ingesterQueueDimension) == 1
	}, time.Second, time.Millisecond)
	require.Equal(t, map[string]float64{ingesterQueueDimension: 1, storeGatewayQueueDimension: 2, ingesterAndStoreGatewayQueueDimension: 1, UnknownDimension: 1}, connected())
}
