package eventokendistributor

import (
	"container/heap"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	minTokenWeight = 100
	maxTokenWeight = 1000
)

func getRandomTokenWeight() float64 {
	return (maxTokenWeight-minTokenWeight)*rand.Float64() + minTokenWeight
}

func createPriorityQueueWithoutInitialization(size int, max bool) (*PriorityQueue, float64, float64) {
	pq := newPriorityQueue(size, max)

	minWeight := float64(math.MaxUint32)
	maxWeight := 0.0
	for i := 1; i <= size-1; i++ {
		randomWeight := getRandomTokenWeight()
		if randomWeight > maxWeight {
			maxWeight = randomWeight
		}
		if randomWeight < minWeight {
			minWeight = randomWeight
		}
		ti := NewRingToken(5, 1)
		oi := newOwnershipInfo(randomWeight)
		oi.ringItem = ti
		pq.Add(oi)
	}
	return pq, minWeight, maxWeight
}

func TestMaxPriorityQueue_PushPopPeek(t *testing.T) {
	size := 10
	pq, minWeight, maxWeight := createPriorityQueueWithoutInitialization(size, true)

	// Initialize PriorityQueue
	heap.Init(pq)

	// Check that the highest priority is maxWeight, but don't remove it
	require.Equal(t, maxWeight, pq.Peek().ownership)

	newMaxWeight := maxWeight + 1.0
	// Push to pq an element with the priority higher than the current maximal priority
	weightedNavigableToken := newOwnershipInfo(newMaxWeight)
	ringInstance := NewRingInstance(1, "a-1")
	weightedNavigableToken.ringItem = ringInstance
	heap.Push(pq, weightedNavigableToken)

	// Check that the highest priority is now newMaxWeight, but don't remove it
	require.Equal(t, newMaxWeight, pq.Peek().ownership)

	// Push to pq an element with the priority lower than the current minimal priority
	newMinWeight := minWeight - 1.0
	weightedNavigableToken = newOwnershipInfo(newMinWeight)
	heap.Push(pq, weightedNavigableToken)

	// Check that the maximal priority is newMaxWeight and remove it
	item := heap.Pop(pq).(*OwnershipInfo)
	require.Equal(t, newMaxWeight, item.ownership)

	// Check that the highest priority is again maxWeight, but don't remove it
	require.Equal(t, maxWeight, pq.Peek().ownership)

	// Check that all other elements except the last one are sorted correctly
	currWeight := math.MaxFloat64
	for pq.Len() > 1 {
		weightedNavigableToken := heap.Pop(pq).(*OwnershipInfo)
		require.Less(t, weightedNavigableToken.ownership, currWeight)
		currWeight = weightedNavigableToken.ownership
	}

	// Check that the minimal priority is newMinWeight
	item = heap.Pop(pq).(*OwnershipInfo)
	require.Equal(t, newMinWeight, item.ownership)
}

func TestMaxPriorityQueue_Update(t *testing.T) {
	first := newOwnershipInfo(3.0)
	second := newOwnershipInfo(5.0)
	third := newOwnershipInfo(4.0)

	pq := newPriorityQueue(3, true)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(weightedNavigableToken *OwnershipInfo) {
		weightedNavigableToken.ownership *= 2
	})

	// Check that now first has the highest priority
	require.Equal(t, first, pq.Peek())
}

func TestMinPriorityQueue_PushPopPeek(t *testing.T) {
	size := 10
	pq, minWeight, maxWeight := createPriorityQueueWithoutInitialization(size, false)

	// Initialize PriorityQueue
	heap.Init(pq)

	// Check that the highest priority is minWeight, but don't remove it
	require.Equal(t, minWeight, pq.Peek().ownership)

	newMinWeight := minWeight - 1.0
	// Push to pq an element with the ownership lower than the current minimal ownership
	weightedNavigableToken := newOwnershipInfo(newMinWeight)
	heap.Push(pq, weightedNavigableToken)

	// Check that the highest priority is now newMinWeight, but don't remove it
	require.Equal(t, newMinWeight, pq.Peek().ownership)

	// Push to pq an element with the ownership higher than the current maximal priority
	newMaxWeight := maxWeight + 1.0
	weightedNavigableToken = newOwnershipInfo(newMaxWeight)
	heap.Push(pq, weightedNavigableToken)

	// Check that the highest priority is newMinWeight and remove it
	item := heap.Pop(pq).(*OwnershipInfo)
	require.Equal(t, newMinWeight, item.ownership)

	// Check that the highest priority is again minWeight, but don't remove it
	require.Equal(t, minWeight, pq.Peek().ownership)

	// Check that all other elements except the last one are sorted correctly
	currWeight := math.SmallestNonzeroFloat64
	for pq.Len() > 1 {
		candidateTokenOwnership := heap.Pop(pq).(*OwnershipInfo)
		require.Greater(t, candidateTokenOwnership.ownership, currWeight)
		currWeight = candidateTokenOwnership.ownership
	}

	// Check that the minimal priority is newMaxWeight
	item = heap.Pop(pq).(*OwnershipInfo)
	require.Equal(t, newMaxWeight, item.ownership)
}

func TestMinPriorityQueue_Update(t *testing.T) {
	first := newOwnershipInfo(4.0)
	second := newOwnershipInfo(3.0)
	third := newOwnershipInfo(5.0)

	pq := newPriorityQueue(3, false)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority, i.e., the minimal ownership
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(weightedNavigableToken *OwnershipInfo) {
		weightedNavigableToken.ownership /= 2
	})

	// Check that now first has the highest priority
	require.Equal(t, first, pq.Peek())
}
