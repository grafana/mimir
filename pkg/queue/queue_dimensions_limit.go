// SPDX-License-Identifier: AGPL-3.0-only

package queue

// dimensionInflightLimit counts the items of one queue dimension that consumer-workers
// received and did not release yet.
//
// maxRatio limits these items to a share of the connected consumer-workers. The limit
// stops slow or blocking items of the dimension from holding most consumer-workers,
// so that consumer-workers still take items of the other dimensions.
//
// A maxRatio of 0 disables the limit.
type dimensionInflightLimit struct {
	maxRatio float64
	inflight int
}

// atLimit reports whether the dimension is at its limit. At the limit, a dequeue skips
// the dimension.
func (l *dimensionInflightLimit) atLimit(connectedConsumerWorkers int64) bool {
	if l.maxRatio <= 0 {
		return false
	}
	// The floor of 1 lets the limited dimension make progress with few connected consumer-workers.
	limit := max(1, int(l.maxRatio*float64(connectedConsumerWorkers)))
	return l.inflight >= limit
}

// inc increments the number of in-flight items.
func (l *dimensionInflightLimit) inc() {
	l.inflight++
}

// dec decrements the number of in-flight items.
func (l *dimensionInflightLimit) dec() {
	if l.inflight > 0 {
		l.inflight--
	}
}
