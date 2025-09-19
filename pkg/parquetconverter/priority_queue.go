// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"container/heap"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// PriorityCriteria defines how blocks should be prioritized in the conversion queue.
type PriorityCriteria string

const (
	// OlderFirst processes older blocks first (default behavior, based on ULID timestamp).
	OlderFirst PriorityCriteria = "older-first"
	// NewerFirst processes newer blocks first (reverse of ULID timestamp).
	NewerFirst PriorityCriteria = "newer-first"
	// Random processes blocks in random order.
	Random PriorityCriteria = "random"
	// FIFO processes blocks in first-in-first-out order (based on enqueue time).
	FIFO PriorityCriteria = "fifo"
)

// String returns the string representation of the priority criteria.
func (pc PriorityCriteria) String() string {
	return string(pc)
}

// Set implements the flag.Value interface for CLI flag parsing.
func (pc *PriorityCriteria) Set(s string) error {
	value := strings.ToLower(s)
	switch PriorityCriteria(value) {
	case OlderFirst:
		*pc = OlderFirst
	case NewerFirst:
		*pc = NewerFirst
	case Random:
		*pc = Random
	case FIFO:
		*pc = FIFO
	default:
		return fmt.Errorf("invalid priority criteria %q, valid options are: older-first, newer-first, random, fifo", s)
	}
	return nil
}

// conversionTask represents a block conversion task with a set priority. The Priority is calculated based on the configured criteria.
type conversionTask struct {
	UserID     string
	Meta       *block.Meta
	Bucket     objstore.InstrumentedBucket
	Priority   int64
	EnqueuedAt time.Time
}

func newConversionTask(userID string, meta *block.Meta, bucket objstore.InstrumentedBucket, criteria PriorityCriteria) *conversionTask {
	task := &conversionTask{
		UserID:     userID,
		Meta:       meta,
		Bucket:     bucket,
		EnqueuedAt: time.Now(),
	}

	switch criteria {
	case OlderFirst:
		task.Priority = int64(meta.ULID.Time())
	case NewerFirst:
		task.Priority = -int64(meta.ULID.Time()) // Negate to reverse order
	case Random:
		task.Priority = rand.Int63()
	case FIFO:
		task.Priority = task.EnqueuedAt.UnixNano()
	default:
		// Default to older-first behavior
		task.Priority = int64(meta.ULID.Time())
	}

	return task
}

// conversionHeap implements heap.Interface for conversionTask priority queue see https://pkg.go.dev/container/heap
// for more details. The heap is a min-heap, meaning that the task with the lowest priority value is at the top.
type conversionHeap []*conversionTask

func (h *conversionHeap) Len() int           { return len(*h) }
func (h *conversionHeap) Less(i, j int) bool { return (*h)[i].Priority < (*h)[j].Priority } // Min-heap: lower priority value first.
func (h *conversionHeap) Swap(i, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

func (h *conversionHeap) Push(x any) {
	*h = append(*h, x.(*conversionTask))
}

func (h *conversionHeap) Pop() any {
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
