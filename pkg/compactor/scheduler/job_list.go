// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"

	"github.com/prometheus/client_golang/prometheus"
)

// jobList wraps container/list.List and tracks list mutations without immediately updating
// Prometheus gauges. Call UpdateMetrics (typically via defer) after all mutations in a
// logical operation to write the final state to the gauges once.
type jobList struct {
	list       list.List
	countGauge prometheus.Gauge
}

func newJobList(countGauge prometheus.Gauge) *jobList {
	return &jobList{countGauge: countGauge}
}

func (jl *jobList) PushBack(job TrackedJob) *list.Element {
	return jl.list.PushBack(job)
}

func (jl *jobList) PushFront(job TrackedJob) *list.Element {
	return jl.list.PushFront(job)
}

func (jl *jobList) Remove(e *list.Element) {
	jl.list.Remove(e)
}

// Reset clears the list.
func (jl *jobList) Reset() {
	jl.list.Init()
}

// UpdateMetrics writes the current list state to the Prometheus gauges.
// It should be called after all mutations in a logical operation, typically via defer.
func (jl *jobList) UpdateMetrics() {
	jl.countGauge.Set(float64(jl.list.Len()))
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
