// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"container/heap"
	"sync"
	"time"

	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// conversionTask represents a block conversion task with a set priority. The Priority is based on the ULID timestamp,
// allowing for prioritization of more recent blocks.
type conversionTask struct {
	UserID     string
	Meta       *block.Meta
	Bucket     objstore.InstrumentedBucket
	Priority   int64
	EnqueuedAt time.Time
}

func newConversionTask(userID string, meta *block.Meta, bucket objstore.InstrumentedBucket) *conversionTask {
	return &conversionTask{
		UserID:     userID,
		Meta:       meta,
		Bucket:     bucket,
		Priority:   int64(meta.ULID.Time()),
		EnqueuedAt: time.Now(),
	}
}

// conversionHeap implements heap.Interface for conversionTask priority queue see https://pkg.go.dev/container/heap
// for more details. The heap is a min-heap, meaning that the task with the lowest priority (oldest ULID
// timestamp) is at the top.
type conversionHeap []*conversionTask

func (h *conversionHeap) Len() int           { return len(*h) }
func (h *conversionHeap) Less(i, j int) bool { return (*h)[i].Priority < (*h)[j].Priority } // Min-heap: lower priority first. The priority is based on ULID timestamp, so we want the oldest (lowest timestamp) first.
func (h *conversionHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *conversionHeap) Push(x interface{}) {
	*h = append(*h, x.(*conversionTask))
}

func (h *conversionHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// priorityQueue is a thread-safe priority queue for conversionTask using container/heap
type priorityQueue struct {
	heap   conversionHeap
	mu     sync.RWMutex
	closed bool
}

func newPriorityQueue() *priorityQueue {
	pq := &priorityQueue{
		heap: make(conversionHeap, 0),
	}
	heap.Init(&pq.heap)
	return pq
}

func (pq *priorityQueue) Push(task *conversionTask) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return false
	}

	heap.Push(&pq.heap, task)
	return true
}

func (pq *priorityQueue) Pop() (*conversionTask, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.heap.Len() == 0 {
		return nil, false
	}

	task := heap.Pop(&pq.heap).(*conversionTask)
	return task, true
}

func (pq *priorityQueue) Size() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.heap.Len()
}

func (pq *priorityQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.closed = true
}
