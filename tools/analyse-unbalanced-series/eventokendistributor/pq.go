package eventokendistributor

import (
	"container/heap"
	"fmt"
	"math"
)

type RingItem interface {
	GetID() int
}

type RingInstance struct {
	instanceID int
	instance   string
}

func NewRingInstance(instanceID int, instance string) *RingInstance {
	return &RingInstance{
		instanceID: instanceID,
		instance:   instance,
	}
}

func (ri *RingInstance) GetID() int {
	return ri.instanceID
}

func (ri *RingInstance) String() string {
	return fmt.Sprintf("[instanceID: %d, instance: %s]", ri.instanceID, ri.instance)
}

type RingToken struct {
	token     uint32
	prevToken uint32
}

func NewRingToken(token, prevToken uint32) *RingToken {
	return &RingToken{
		token:     token,
		prevToken: prevToken,
	}
}

func (rt *RingToken) GetID() int {
	return int(rt.token)
}

func (rt *RingToken) String() string {
	return fmt.Sprintf("[token: %d, prevToken: %d]", rt.token, rt.prevToken)
}

type OwnershipInfo struct {
	ringItem  RingItem
	ownership float64
	index     int
}

func newOwnershipInfo(ownership float64) *OwnershipInfo {
	return &OwnershipInfo{
		ownership: ownership,
	}
}

func (oi *OwnershipInfo) SetRingItem(ringItem RingItem) {
	oi.ringItem = ringItem
}

type PriorityQueue struct {
	items []*OwnershipInfo
	max   bool
}

func newPriorityQueue(len int, max bool) *PriorityQueue {
	pq := &PriorityQueue{
		items: make([]*OwnershipInfo, 0, len),
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
		// we are implementing a max heap priority queue, so we are using > here
		// Since we compare float64, NaN values must be placed at the end
		return pq.items[i].ownership > pq.items[j].ownership || (math.IsNaN(pq.items[j].ownership) && !math.IsNaN(pq.items[i].ownership))
	}
	return pq.items[i].ownership < pq.items[j].ownership || (math.IsNaN(pq.items[i].ownership) && !math.IsNaN(pq.items[j].ownership))
}

// Push implements heap.Push(any). It pushes the element item onto PriorityQueue.
// The complexity is O(log n) where n = PriorityQueue.Len().
func (pq *PriorityQueue) Push(item any) {
	n := len(pq.items)
	ownershipInfo := item.(*OwnershipInfo)
	ownershipInfo.index = n
	pq.items = append(pq.items, ownershipInfo)
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
func (pq *PriorityQueue) Peek() *OwnershipInfo {
	return (pq.items)[0]
}

// Update updates the element ownershipInfo passed as parameter by applying to it  the updating function
// update passed as parameter, and propagate this modification to PriorityQueue. Element ownershipInfo
// must be already present on PriorityQueue.
func (pq *PriorityQueue) Update(ownershipInfo *OwnershipInfo, update func(*OwnershipInfo)) {
	update(ownershipInfo)
	heap.Fix(pq, ownershipInfo.index)
}

// Add adds an element at the end of the queue, but it does not take into account the ownership value.
// In order to re-stabilize the priority queue property it is necessary to call heap.Init() on this queue.
func (pq *PriorityQueue) Add(ownershipInfo *OwnershipInfo) {
	pq.items = append(pq.items, ownershipInfo)
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
		str += fmt.Sprintf("%d: %s-%15.3f", i, item.ringItem, item.ownership)
		if i < pq.Len()-1 {
			str += ","
		}
	}
	str += "]"
	return str
}
