package tokendistributor

import (
	"container/heap"
	"fmt"
	"math"
)

type WeightedNavigableToken[T navigableTokenInterface] struct {
	navigableToken *navigableToken[T]
	weight         float64
	index          int
}

func newWeightedNavigableToken[T navigableTokenInterface](improvement float64) *WeightedNavigableToken[T] {
	return &WeightedNavigableToken[T]{
		weight: improvement,
	}
}

func setNavigableToken[T navigableTokenInterface](weightedNavigableToken *WeightedNavigableToken[T], navigableToken *navigableToken[T]) {
	weightedNavigableToken.navigableToken = navigableToken
}

func getNavigableToken[T navigableTokenInterface](weightedNavigableToken *WeightedNavigableToken[T]) *navigableToken[T] {
	return weightedNavigableToken.navigableToken
}

type PriorityQueue[T navigableTokenInterface] struct {
	items []*WeightedNavigableToken[T]
	max   bool
}

func newPriorityQueue[T navigableTokenInterface](len int, max bool) *PriorityQueue[T] {
	pq := &PriorityQueue[T]{
		items: make([]*WeightedNavigableToken[T], 0, len),
		max:   max,
	}

	return pq
}

func (pq PriorityQueue[T]) Len() int {
	return len(pq.items)
}

func (pq PriorityQueue[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i // pq[i] is actually the old pq[j], so pq[i].index should be updated to i
	pq.items[j].index = j // pq[j] is actually the old pq[i], so pq[j].index should be updated to j
}

func (pq PriorityQueue[T]) Less(i, j int) bool {
	if pq.max {
		// we are implementing a max heap priority queue, so we are using > here
		// Since we compare float64, NaN values must be placed at the end
		return pq.items[i].weight > pq.items[j].weight || (math.IsNaN(pq.items[j].weight) && !math.IsNaN(pq.items[i].weight))
	}
	return pq.items[i].weight < pq.items[j].weight || (math.IsNaN(pq.items[i].weight) && !math.IsNaN(pq.items[j].weight))
}

// Push implements heap.Push(any). It pushes the element item onto PriorityQueue.
// The complexity is O(log n) where n = PriorityQueue.Len().
func (pq *PriorityQueue[T]) Push(item any) {
	n := len(pq.items)
	weightedNavigableToken := item.(*WeightedNavigableToken[T])
	weightedNavigableToken.index = n
	pq.items = append(pq.items, weightedNavigableToken)
}

// Pop implements heap.Pop(). It removes and returns the element with the highest priority from PriorityQueue.
// The complexity is O(log n) where n = PriorityQueue.Len().
func (pq *PriorityQueue[T]) Pop() any {
	old := pq.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	pq.items = old[0 : n-1]
	return item
}

// Peek the returns the element with the highest priority from PriorityQueue, but it does not remove it from the latter.
// The complexity is O(1).
func (pq *PriorityQueue[T]) Peek() *WeightedNavigableToken[T] {
	return (pq.items)[0]
}

// Update updates the element weightedNavigableToken passed as parameter by applying to it  the updating function
// update passed as parameter, and propagate this modification to PriorityQueue. Element weightedNavigableToken
// must be already present on PriorityQueue.
func (pq *PriorityQueue[T]) Update(weightedNavigableToken *WeightedNavigableToken[T], update func(*WeightedNavigableToken[T])) {
	update(weightedNavigableToken)
	heap.Fix(pq, weightedNavigableToken.index)
}

// Add adds an element at the end of the queue, but it does not take into account the weight value.
// In order to re-stabilize the priority queue property it is necessary to call heap.Init() on this queue.
func (pq *PriorityQueue[T]) Add(weightedNavigableToken *WeightedNavigableToken[T]) {
	pq.items = append(pq.items, weightedNavigableToken)
}

func (pq *PriorityQueue[T]) String() string {
	if pq.Len() == 0 {
		return "[]"
	}

	str := "["
	for i, item := range pq.items {
		str += fmt.Sprintf("%d: %d-%.2f", i, item.navigableToken.getData().getToken(), item.weight)
		if i < pq.Len()-1 {
			str += ","
		}
	}
	str += "]"
	return str
}
