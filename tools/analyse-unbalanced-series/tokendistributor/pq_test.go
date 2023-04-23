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

func createCandidateTokenInfoOwnership(candTokenInfo *candidateTokenInfo, ownership float64) *CandidateTokenInfoOwnership {
	candidateTokenInfoOwnership := newCandidateTokenInfoOwnership(candTokenInfo)
	candidateTokenInfoOwnership.ownership = ownership
	return candidateTokenInfoOwnership
}

func createPriorityQueueWithoutInitialization(size int) (*PriorityQueue, float64, float64) {
	instanceInfo := newInstanceInfo("instance", newZoneInfo("zone"), 4)
	candidateTokenInfo := newCandidateTokenInfo(instanceInfo, Token(1), nil)
	pq := newPriorityQueue(size)

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

func TestPriorityQueue_PushPopPeek(t *testing.T) {
	size := 10
	pq, minOwnership, maxOwnership := createPriorityQueueWithoutInitialization(size)

	// Initialize PriorityQueue
	heap.Init(pq)

	// Check that the highest priority is maxOwnership, but don't remove it
	require.Equal(t, maxOwnership, pq.Peek().ownership)

	candidateTokenInfo := (*pq)[0].candidateTokenInfo
	newMaxOwnership := maxOwnership + 1.0
	// Push to pq an element with the priority higher than the current maximal priority
	candidateTokenInfoOwnership := createCandidateTokenInfoOwnership(candidateTokenInfo, newMaxOwnership)
	heap.Push(pq, candidateTokenInfoOwnership)

	// Check that the highest priority is now newMaxOwnership, but don't remove it
	require.Equal(t, newMaxOwnership, pq.Peek().ownership)

	// Push to pq an element with the priority lower than the current minimal priority
	newMinOwnership := minOwnership - 1.0
	candidateTokenInfoOwnership = createCandidateTokenInfoOwnership(candidateTokenInfo, newMinOwnership)
	heap.Push(pq, candidateTokenInfoOwnership)

	// Check that the maximal priority is newMaxOwnership and remove it
	item := heap.Pop(pq).(*CandidateTokenInfoOwnership)
	require.Equal(t, newMaxOwnership, item.ownership)

	// Check that the highest priority is again maxOwnership, but don't remove it
	require.Equal(t, maxOwnership, pq.Peek().ownership)

	// Check that all other elements except the last one are sorted correctly
	currOwnership := math.MaxFloat64
	for pq.Len() > 1 {
		candidateTokenOwnership := heap.Pop(pq).(*CandidateTokenInfoOwnership)
		require.Less(t, candidateTokenOwnership.ownership, currOwnership)
		currOwnership = candidateTokenOwnership.ownership
	}

	// Check that the minimal priority is minOwnership
	item = heap.Pop(pq).(*CandidateTokenInfoOwnership)
	require.Equal(t, newMinOwnership, item.ownership)
}

func TestPriorityQueue_Update(t *testing.T) {
	instanceInfo := newInstanceInfo("instance", newZoneInfo("zone"), 4)
	candidateTokenInfo := newCandidateTokenInfo(instanceInfo, Token(1), nil)

	first := createCandidateTokenInfoOwnership(candidateTokenInfo, 3.0)
	second := createCandidateTokenInfoOwnership(candidateTokenInfo, 5.0)
	third := createCandidateTokenInfoOwnership(candidateTokenInfo, 4.0)

	pq := newPriorityQueue(3)
	pq.Add(first)
	pq.Add(second)
	pq.Add(third)

	heap.Init(pq)

	// Check that second has the highest priority
	require.Equal(t, second, pq.Peek())

	// Update the value of first and assign it the highest priority
	pq.Update(first, func(candidateTokenInfoOwnership *CandidateTokenInfoOwnership) {
		candidateTokenInfoOwnership.ownership *= 2
	})

	// Check that now first has the highest priority
	require.Equal(t, first, pq.Peek())
}
