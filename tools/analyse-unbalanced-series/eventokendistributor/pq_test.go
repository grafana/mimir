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

func getRandomTokenWeight() uint32 {
	return (maxTokenWeight-minTokenWeight)*rand.Uint32() + minTokenWeight
}

func createPriorityQueueWithoutInitialization(size int, max bool) (*PriorityQueue, uint32, uint32) {
	pq := newPriorityQueue(size, max)

	minWeight := uint32(math.MaxUint32)
	maxWeight := uint32(0)
	for i := 1; i <= size-1; i++ {
		randomWeight := getRandomTokenWeight()
		if randomWeight > maxWeight {
			maxWeight = randomWeight
		}
		if randomWeight < minWeight {
			minWeight = randomWeight
		}
		pq.Add(newWeightedToken(0, randomWeight))
	}
	return pq, minWeight, maxWeight
}

func TestMaxPriorityQueue_PushPopPeek(t *testing.T) {
	size := 10
	pq, minWeight, maxWeight := createPriorityQueueWithoutInitialization(size, true)

	// Initialize PriorityQueue
	heap.Init(pq)

	// Check that the highest priority is maxWeight, but don't remove it
	require.Equal(t, maxWeight, pq.Peek().weight)

	newMaxWeight := maxWeight + 1.0
	// Push to pq an element with the priority higher than the current maximal priority
	weightedNavigableToken := newWeightedToken(0, newMaxWeight)
	heap.Push(pq, weightedNavigableToken)

	// Check that the highest priority is now newMaxWeight, but don't remove it
	require.Equal(t, newMaxWeight, pq.Peek().weight)

	// Push to pq an element with the priority lower than the current minimal priority
	newMinWeight := minWeight - 1.0
	weightedNavigableToken = newWeightedToken(0, newMinWeight)
	heap.Push(pq, weightedNavigableToken)

	// Check that the maximal priority is newMaxWeight and remove it
	item := heap.Pop(pq).(*WeightedToken)
	require.Equal(t, newMaxWeight, item.weight)

	// Check that the highest priority is again maxWeight, but don't remove it
	require.Equal(t, maxWeight, pq.Peek().weight)

	// Check that all other elements except the last one are sorted correctly
	currWeight := uint32(math.MaxUint32)
	for pq.Len() > 1 {
		weightedNavigableToken := heap.Pop(pq).(*WeightedToken)
		require.Less(t, weightedNavigableToken.weight, currWeight)
		currWeight = weightedNavigableToken.weight
	}

	// Check that the minimal priority is newMinWeight
	item = heap.Pop(pq).(*WeightedToken)
	require.Equal(t, newMinWeight, item.weight)
}

func TestMaxPriorityQueue_Update(t *testing.T) {
	first := newWeightedToken(0, 3.0)
	second := newWeightedToken(0, 5.0)
	third := newWeightedToken(0, 4.0)

	pq := newPriorityQueue(3, true)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(weightedNavigableToken *WeightedToken) {
		weightedNavigableToken.weight *= 2
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
	require.Equal(t, minWeight, pq.Peek().weight)

	newMinWeight := minWeight - 1.0
	// Push to pq an element with the weight lower than the current minimal weight
	weightedNavigableToken := newWeightedToken(0, newMinWeight)
	heap.Push(pq, weightedNavigableToken)

	// Check that the highest priority is now newMinWeight, but don't remove it
	require.Equal(t, newMinWeight, pq.Peek().weight)

	// Push to pq an element with the weight higher than the current maximal priority
	newMaxWeight := maxWeight + 1.0
	weightedNavigableToken = newWeightedToken(0, newMaxWeight)
	heap.Push(pq, weightedNavigableToken)

	// Check that the highest priority is newMinWeight and remove it
	item := heap.Pop(pq).(*WeightedToken)
	require.Equal(t, newMinWeight, item.weight)

	// Check that the highest priority is again minWeight, but don't remove it
	require.Equal(t, minWeight, pq.Peek().weight)

	// Check that all other elements except the last one are sorted correctly
	currWeight := uint32(0)
	for pq.Len() > 1 {
		candidateTokenOwnership := heap.Pop(pq).(*WeightedToken)
		require.Greater(t, candidateTokenOwnership.weight, currWeight)
		currWeight = candidateTokenOwnership.weight
	}

	// Check that the minimal priority is newMaxWeight
	item = heap.Pop(pq).(*WeightedToken)
	require.Equal(t, newMaxWeight, item.weight)
}

func TestMinPriorityQueue_Update(t *testing.T) {
	first := newWeightedToken(0, 4.0)
	second := newWeightedToken(0, 3.0)
	third := newWeightedToken(0, 5.0)

	pq := newPriorityQueue(3, false)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority, i.e., the minimal weight
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(weightedNavigableToken *WeightedToken) {
		weightedNavigableToken.weight /= 2
	})

	// Check that now first has the highest priority
	require.Equal(t, first, pq.Peek())
}
