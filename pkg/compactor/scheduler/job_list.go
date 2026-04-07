// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/heap"
	"container/list"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// jobList wraps container/list.List and tracks list mutations without immediately updating
// Prometheus gauges. Call UpdateMetrics (typically via defer) after all mutations in a
// logical operation to write the final state to the gauges once.
type jobList struct {
	list       list.List
	countGauge prometheus.Gauge
	bytesGauge prometheus.Gauge // global bytes shared across pending and active lists
	bytesDelta int64            // accumulated bytes change, flushed in UpdateMetrics
	sizeHeap   *jobSizeHeap     // global max bytes shared across pending and active lists
}

func newJobList(countGauge, bytesGauge prometheus.Gauge, sizeHeap *jobSizeHeap) *jobList {
	return &jobList{countGauge: countGauge, bytesGauge: bytesGauge, sizeHeap: sizeHeap}
}

func (jl *jobList) PushBack(job TrackedJob) *list.Element {
	jl.onAdd(job)
	return jl.list.PushBack(job)
}

func (jl *jobList) PushFront(job TrackedJob) *list.Element {
	jl.onAdd(job)
	return jl.list.PushFront(job)
}

func (jl *jobList) Remove(e *list.Element) {
	jl.onRemove(e.Value.(TrackedJob))
	jl.list.Remove(e)
}

// Reset clears the list.
func (jl *jobList) Reset() {
	for e := jl.list.Front(); e != nil; e = e.Next() {
		jl.onRemove(e.Value.(TrackedJob))
	}
	jl.list.Init()
}

// UpdateMetrics writes the current list state to the Prometheus gauges.
// It should be called after all mutations in a logical operation, typically via defer.
func (jl *jobList) UpdateMetrics() {
	jl.countGauge.Set(float64(jl.list.Len()))
	if jl.bytesDelta != 0 {
		jl.bytesGauge.Add(float64(jl.bytesDelta))
		jl.bytesDelta = 0
	}
	jl.sizeHeap.updateGauge()
}

func (jl *jobList) onAdd(job TrackedJob) {
	cj, ok := job.(*TrackedCompactionJob)
	if !ok {
		return
	}
	jl.bytesDelta += cj.totalBlockBytes
	jl.sizeHeap.push(cj)
}

func (jl *jobList) onRemove(job TrackedJob) {
	cj, ok := job.(*TrackedCompactionJob)
	if !ok {
		return
	}
	jl.bytesDelta -= cj.totalBlockBytes
	jl.sizeHeap.remove(cj)
}

func (jl *jobList) Len() int {
	return jl.list.Len()
}

func (jl *jobList) Front() *list.Element {
	return jl.list.Front()
}

func (jl *jobList) MoveToBack(e *list.Element) {
	jl.list.MoveToBack(e)
}

// jobSizeHeap is a thread-safe max-heap that tracks the largest totalBlockBytes across all
// incomplete compaction jobs. It is shared across the pending and active jobLists of all tenants.
// The gauge is updated via UpdateMetrics, not synchronously on every push and remove.
type jobSizeHeap struct {
	mu    sync.Mutex
	h     maxBytesHeap
	items map[*TrackedCompactionJob]*bytesHeapItem
	gauge prometheus.Gauge
}

func newJobSizeHeap(gauge prometheus.Gauge) *jobSizeHeap {
	return &jobSizeHeap{
		items: make(map[*TrackedCompactionJob]*bytesHeapItem),
		gauge: gauge,
	}
}

func (sh *jobSizeHeap) push(job *TrackedCompactionJob) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	item := &bytesHeapItem{bytes: job.totalBlockBytes}
	sh.items[job] = item
	heap.Push(&sh.h, item)
}

func (sh *jobSizeHeap) remove(job *TrackedCompactionJob) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	item, ok := sh.items[job]
	if !ok {
		return
	}
	heap.Remove(&sh.h, item.index)
	delete(sh.items, job)
}

func (sh *jobSizeHeap) updateGauge() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if len(sh.h) == 0 {
		sh.gauge.Set(0)
		return
	}
	sh.gauge.Set(float64(sh.h[0].bytes))
}

// bytesHeapItem is an element of the maxBytesHeap.
type bytesHeapItem struct {
	bytes int64
	index int // position in the heap slice, maintained by maxBytesHeap.Swap
}

// maxBytesHeap implements heap.Interface for a max-heap over bytesHeapItem.
type maxBytesHeap []*bytesHeapItem

func (h maxBytesHeap) Len() int           { return len(h) }
func (h maxBytesHeap) Less(i, j int) bool { return h[i].bytes > h[j].bytes }
func (h maxBytesHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *maxBytesHeap) Push(x any) {
	item := x.(*bytesHeapItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *maxBytesHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}
