package eventokendistributor

import (
	"container/heap"
	"fmt"
)

type WeightedToken struct {
	token  uint32
	weight uint32
	index  int
}

func newWeightedToken(token uint32, weight uint32) *WeightedToken {
	return &WeightedToken{
		token:  token,
		weight: weight,
	}
}

type PriorityQueue struct {
	items []*WeightedToken
	max   bool
}

func newPriorityQueue(len int, max bool) *PriorityQueue {
	pq := &PriorityQueue{
		items: make([]*WeightedToken, 0, len),
		max:   max,
	}

	return pq
}

func (pq PriorityQueue) Len() int {
	return len(pq.items)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i // pq[i] is actually the old pq[j], so pq[i].index should be updated to i
	pq.items[j].index = j // pq[j] is actually the old pq[i], so pq[j].index should be updated to j
}

func (pq PriorityQueue) Less(i, j int) bool {
	if pq.max {
		return pq.items[i].weight > pq.items[j].weight
	}
	return pq.items[i].weight < pq.items[j].weight
}

// Push implements heap.Push(any). It pushes the element item onto PriorityQueue.
// The complexity is O(log n) where n = PriorityQueue.Len().
func (pq *PriorityQueue) Push(item any) {
	n := len(pq.items)
	weightedNavigableToken := item.(*WeightedToken)
	weightedNavigableToken.index = n
	pq.items = append(pq.items, weightedNavigableToken)
}

// Pop implements heap.Pop(). It removes and returns the element with the highest priority from PriorityQueue.
// The complexity is O(log n) where n = PriorityQueue.Len().
func (pq *PriorityQueue) Pop() any {
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
func (pq *PriorityQueue) Peek() *WeightedToken {
	return (pq.items)[0]
}

// Update updates the element weightedNavigableToken passed as parameter by applying to it  the updating function
// update passed as parameter, and propagate this modification to PriorityQueue. Element weightedNavigableToken
// must be already present on PriorityQueue.
func (pq *PriorityQueue) Update(weightedNavigableToken *WeightedToken, update func(*WeightedToken)) {
	update(weightedNavigableToken)
	heap.Fix(pq, weightedNavigableToken.index)
}

// Add adds an element at the end of the queue, but it does not take into account the weight value.
// In order to re-stabilize the priority queue property it is necessary to call heap.Init() on this queue.
func (pq *PriorityQueue) Add(weightedNavigableToken *WeightedToken) {
	pq.items = append(pq.items, weightedNavigableToken)
}

func (pq *PriorityQueue) Clear() {
	pq.items = pq.items[:0]
}

func (pq *PriorityQueue) String() string {
	if pq.Len() == 0 {
		return "[]"
	}

	str := "["
	for i, item := range pq.items {
		str += fmt.Sprintf("%d: %d-%d", i, item.token, item.weight)
		if i < pq.Len()-1 {
			str += ","
		}
	}
	str += "]"
	return str
}
