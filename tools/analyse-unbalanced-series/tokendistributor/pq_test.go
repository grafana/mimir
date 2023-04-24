package tokendistributor

import (
	"container/heap"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	minTokenOwnership = 100
	maxTokenOwnership = 1000
)

func getRandomTokenOwnership() float64 {
	return (maxTokenOwnership-minTokenOwnership)*rand.Float64() + minTokenOwnership
}

func createCandidateTokenInfoOwnership(candTokenInfo *candidateTokenInfo, ownership float64) *CandidateTokenInfoImprovement {
	candidateTokenInfoOwnership := newCandidateTokenInfoImprovement(candTokenInfo, candTokenInfo.getReplicatedOwnership())
	candidateTokenInfoOwnership.improvement = ownership
	return candidateTokenInfoOwnership
}

func createPriorityQueueWithoutInitialization(size int, max bool) (*PriorityQueue, float64, float64) {
	instanceInfo := newInstanceInfo("instance", newZoneInfo("zone"), 4)
	candidateTokenInfo := newCandidateTokenInfo(instanceInfo, Token(1), nil)
	pq := newPriorityQueue(size, max)

	minOwnership := math.MaxFloat64
	maxOwnership := math.SmallestNonzeroFloat64
	for i := 1; i <= size-1; i++ {
		randomOwnership := getRandomTokenOwnership()
		if randomOwnership > maxOwnership {
			maxOwnership = randomOwnership
		}
		if randomOwnership < minOwnership {
			minOwnership = randomOwnership
		}
		pq.Add(createCandidateTokenInfoOwnership(candidateTokenInfo, randomOwnership))
	}
	return pq, minOwnership, maxOwnership
}

func TestMaxPriorityQueue_PushPopPeek(t *testing.T) {
	size := 10
	pq, minOwnership, maxOwnership := createPriorityQueueWithoutInitialization(size, true)

	// Initialize PriorityQueue
	heap.Init(pq)

	// Check that the highest priority is maxOwnership, but don't remove it
	require.Equal(t, maxOwnership, pq.Peek().improvement)

	candidateTokenInfo := pq.items[0].candidateTokenInfo
	newMaxOwnership := maxOwnership + 1.0
	// Push to pq an element with the priority higher than the current maximal priority
	candidateTokenInfoOwnership := createCandidateTokenInfoOwnership(candidateTokenInfo, newMaxOwnership)
	heap.Push(pq, candidateTokenInfoOwnership)

	// Check that the highest priority is now newMaxOwnership, but don't remove it
	require.Equal(t, newMaxOwnership, pq.Peek().improvement)

	// Push to pq an element with the priority lower than the current minimal priority
	newMinOwnership := minOwnership - 1.0
	candidateTokenInfoOwnership = createCandidateTokenInfoOwnership(candidateTokenInfo, newMinOwnership)
	heap.Push(pq, candidateTokenInfoOwnership)

	// Check that the maximal priority is newMaxOwnership and remove it
	item := heap.Pop(pq).(*CandidateTokenInfoImprovement)
	require.Equal(t, newMaxOwnership, item.improvement)

	// Check that the highest priority is again maxOwnership, but don't remove it
	require.Equal(t, maxOwnership, pq.Peek().improvement)

	// Check that all other elements except the last one are sorted correctly
	currOwnership := math.MaxFloat64
	for pq.Len() > 1 {
		candidateTokenOwnership := heap.Pop(pq).(*CandidateTokenInfoImprovement)
		require.Less(t, candidateTokenOwnership.improvement, currOwnership)
		currOwnership = candidateTokenOwnership.improvement
	}

	// Check that the minimal priority is newMinOwnership
	item = heap.Pop(pq).(*CandidateTokenInfoImprovement)
	require.Equal(t, newMinOwnership, item.improvement)
}

func TestMaxPriorityQueue_Update(t *testing.T) {
	instanceInfo := newInstanceInfo("instance", newZoneInfo("zone"), 4)
	candidateTokenInfo := newCandidateTokenInfo(instanceInfo, Token(1), nil)

	first := createCandidateTokenInfoOwnership(candidateTokenInfo, 3.0)
	second := createCandidateTokenInfoOwnership(candidateTokenInfo, 5.0)
	third := createCandidateTokenInfoOwnership(candidateTokenInfo, 4.0)

	pq := newPriorityQueue(3, true)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(candidateTokenInfoOwnership *CandidateTokenInfoImprovement) {
		candidateTokenInfoOwnership.improvement *= 2
	})

	// Check that now first has the highest priority
	require.Equal(t, first, pq.Peek())
}

func TestMinPriorityQueue_PushPopPeek(t *testing.T) {
	size := 10
	pq, minOwnership, maxOwnership := createPriorityQueueWithoutInitialization(size, false)

	// Initialize PriorityQueue
	heap.Init(pq)

	// Check that the highest priority is minOwnership, but don't remove it
	require.Equal(t, minOwnership, pq.Peek().improvement)

	candidateTokenInfo := pq.items[0].candidateTokenInfo
	newMinOwnership := minOwnership - 1.0
	// Push to pq an element with the improvement lower than the current minimal improvement
	candidateTokenInfoOwnership := createCandidateTokenInfoOwnership(candidateTokenInfo, newMinOwnership)
	heap.Push(pq, candidateTokenInfoOwnership)

	// Check that the highest priority is now newMinOwnership, but don't remove it
	require.Equal(t, newMinOwnership, pq.Peek().improvement)

	// Push to pq an element with the improvement higher than the current maximal priority
	newMaxOwnership := maxOwnership + 1.0
	candidateTokenInfoOwnership = createCandidateTokenInfoOwnership(candidateTokenInfo, newMaxOwnership)
	heap.Push(pq, candidateTokenInfoOwnership)

	// Check that the highest priority is newMinOwnership and remove it
	item := heap.Pop(pq).(*CandidateTokenInfoImprovement)
	require.Equal(t, newMinOwnership, item.improvement)

	// Check that the highest priority is again minOwnership, but don't remove it
	require.Equal(t, minOwnership, pq.Peek().improvement)

	// Check that all other elements except the last one are sorted correctly
	currOwnership := math.SmallestNonzeroFloat64
	for pq.Len() > 1 {
		candidateTokenOwnership := heap.Pop(pq).(*CandidateTokenInfoImprovement)
		require.Greater(t, candidateTokenOwnership.improvement, currOwnership)
		currOwnership = candidateTokenOwnership.improvement
	}

	// Check that the minimal priority is newMaxOwnership
	item = heap.Pop(pq).(*CandidateTokenInfoImprovement)
	require.Equal(t, newMaxOwnership, item.improvement)
}

func TestMinPriorityQueue_Update(t *testing.T) {
	instanceInfo := newInstanceInfo("instance", newZoneInfo("zone"), 4)
	candidateTokenInfo := newCandidateTokenInfo(instanceInfo, Token(1), nil)

	first := createCandidateTokenInfoOwnership(candidateTokenInfo, 4.0)
	second := createCandidateTokenInfoOwnership(candidateTokenInfo, 3.0)
	third := createCandidateTokenInfoOwnership(candidateTokenInfo, 5.0)

	pq := newPriorityQueue(3, false)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority, i.e., the minimal improvement
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(candidateTokenInfoOwnership *CandidateTokenInfoImprovement) {
		candidateTokenInfoOwnership.improvement /= 2
	})

	// Check that now first has the highest priority
	require.Equal(t, first, pq.Peek())
}
