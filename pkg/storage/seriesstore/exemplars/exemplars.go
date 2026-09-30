// SPDX-License-Identifier: AGPL-3.0-only

// Package exemplars stores per-tenant exemplars with the behavior of Prometheus's
// `CircularExemplarStorage`: a fixed number of exemplars per tenant, evicted oldest-inserted
// first, each series keeping its exemplars sorted by timestamp.
package exemplars

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
)

// Label is an exemplar label.
type Label struct {
	Name, Value string
}

// Exemplar is an exemplar as the ingester receives and serves it.
type Exemplar struct {
	Labels      []Label
	Value       float64
	TimestampMs int64
}

// Equal compares like protobuf message equality: NaN values differ.
func (e *Exemplar) Equal(other *Exemplar) bool {
	return e.TimestampMs == other.TimestampMs && e.Value == other.Value && slices.Equal(e.Labels, other.Labels)
}

// Rejection is why an exemplar was not stored.
type Rejection int

const (
	Accepted Rejection = iota
	Disabled
	LabelLength
	OutOfOrder
)

const MaxLabelSetLength = 128

func labelSetLength(e *Exemplar) int {
	n := 0
	for _, label := range e.Labels {
		n += utf8.RuneCountInString(label.Name) + utf8.RuneCountInString(label.Value)
	}
	return n
}

func outOfOrder(newest, e *Exemplar, windowMs int64) bool {
	return (e.TimestampMs < newest.TimestampMs && e.TimestampMs <= newest.TimestampMs-windowMs) ||
		(e.TimestampMs == newest.TimestampMs && e.Value < newest.Value) ||
		(e.TimestampMs == newest.TimestampMs && e.Value == newest.Value && labelHash(e) < labelHash(newest))
}

// Validate is Prometheus's `ValidateExemplar` against a series whose newest stored exemplar is
// newest (nil when none): what the head appender checks when the exemplar is appended, before the
// commit adds it.
func Validate(capacity int, newest *Exemplar, e *Exemplar, windowMs int64) Rejection {
	if capacity == 0 {
		return Disabled
	}
	if labelSetLength(e) > MaxLabelSetLength {
		return LabelLength
	}
	if newest == nil || newest.Equal(e) {
		return Accepted
	}
	if outOfOrder(newest, e, windowMs) {
		return OutOfOrder
	}
	return Accepted
}

type stored struct {
	sequence uint64
	exemplar Exemplar
}

type seriesExemplars[L any] struct {
	labels L
	// Sorted by timestamp.
	exemplars []stored
	newest    int
}

type orderEntry struct {
	seriesID, sequence uint64
}

// TenantExemplars are one tenant's exemplars, by series id.
type TenantExemplars[L any] struct {
	capacity     int
	nextSequence uint64
	// Insertion order across series, for eviction: a queue over order[head:].
	order  []orderEntry
	head   int
	series map[uint64]*seriesExemplars[L]
}

func New[L any](capacity int) *TenantExemplars[L] {
	return &TenantExemplars[L]{capacity: capacity, series: map[uint64]*seriesExemplars[L]{}}
}

func (t *TenantExemplars[L]) Len() int         { return len(t.order) - t.head }
func (t *TenantExemplars[L]) IsEmpty() bool    { return t.Len() == 0 }
func (t *TenantExemplars[L]) SeriesCount() int { return len(t.series) }
func (t *TenantExemplars[L]) Capacity() int    { return t.capacity }

// OldestTimestamp returns the timestamp of the exemplar that will be evicted next, as Prometheus
// reports it.
func (t *TenantExemplars[L]) OldestTimestamp() (int64, bool) {
	if t.Len() == 0 {
		return 0, false
	}
	front := t.order[t.head]
	series, ok := t.series[front.seriesID]
	if !ok {
		return 0, false
	}
	for _, s := range series.exemplars {
		if s.sequence == front.sequence {
			return s.exemplar.TimestampMs, true
		}
	}
	return 0, false
}

// Resize changes the capacity, keeping the newest exemplars like Prometheus's `Resize`.
func (t *TenantExemplars[L]) Resize(capacity int) {
	t.capacity = capacity
	for t.Len() > capacity {
		t.evictOldest()
	}
}

func (t *TenantExemplars[L]) popFront() (orderEntry, bool) {
	if t.Len() == 0 {
		return orderEntry{}, false
	}
	front := t.order[t.head]
	t.head++
	// Compacted once half is consumed, so the queue's memory stays bounded by the capacity.
	if t.head > 64 && t.head > len(t.order)/2 {
		t.order = append(t.order[:0], t.order[t.head:]...)
		t.head = 0
	}
	return front, true
}

func (t *TenantExemplars[L]) evictOldest() {
	front, ok := t.popFront()
	if !ok {
		return
	}
	series, ok := t.series[front.seriesID]
	if !ok {
		return
	}
	for index, s := range series.exemplars {
		if s.sequence != front.sequence {
			continue
		}
		series.exemplars = slices.Delete(series.exemplars, index, index+1)
		if series.newest == index {
			series.newest = max(len(series.exemplars)-1, 0)
		} else if series.newest > index {
			series.newest--
		}
		break
	}
	if len(series.exemplars) == 0 {
		delete(t.series, front.seriesID)
	}
}

// Newest returns the series' newest exemplar, which later exemplars are validated against.
func (t *TenantExemplars[L]) Newest(seriesID uint64) *Exemplar {
	series, ok := t.series[seriesID]
	if !ok {
		return nil
	}
	return &series.exemplars[series.newest].exemplar
}

// Add adds an exemplar for the series identified by seriesID, returning whether it was stored: a
// duplicate of the series' newest exemplar or of a stored timestamp is accepted without storing it
// again.
func (t *TenantExemplars[L]) Add(seriesID uint64, labels func() L, e Exemplar, windowMs int64) (bool, Rejection) {
	if t.capacity == 0 {
		return false, Disabled
	}
	if labelSetLength(&e) > MaxLabelSetLength {
		return false, LabelLength
	}
	if series, ok := t.series[seriesID]; ok {
		newest := &series.exemplars[series.newest].exemplar
		if newest.Equal(&e) {
			return false, Accepted
		}
		if outOfOrder(newest, &e, windowMs) {
			return false, OutOfOrder
		}
		oldest := series.exemplars[0].exemplar.TimestampMs
		if e.TimestampMs >= oldest && e.TimestampMs < newest.TimestampMs {
			for _, s := range series.exemplars {
				if s.exemplar.TimestampMs == e.TimestampMs {
					return false, Accepted
				}
			}
		}
	}
	if t.Len() >= t.capacity {
		t.evictOldest()
	}
	sequence := t.nextSequence
	t.nextSequence++
	t.order = append(t.order, orderEntry{seriesID, sequence})
	series, ok := t.series[seriesID]
	if !ok {
		series = &seriesExemplars[L]{labels: labels()}
		t.series[seriesID] = series
	}
	// After every exemplar at or before its timestamp.
	index, _ := slices.BinarySearchFunc(series.exemplars, e.TimestampMs, func(s stored, ts int64) int {
		if s.exemplar.TimestampMs <= ts {
			return -1
		}
		return 1
	})
	isNewest := len(series.exemplars) == 0 || e.TimestampMs >= series.exemplars[series.newest].exemplar.TimestampMs
	series.exemplars = slices.Insert(series.exemplars, index, stored{sequence, detached(e)})
	if isNewest {
		series.newest = index
	} else if series.newest >= index {
		series.newest++
	}
	return true, Accepted
}

// Selected is a series' exemplars in a range, in timestamp order.
type Selected[L any] struct {
	Labels    L
	Exemplars []Exemplar
}

// Select returns the series with exemplars in [start, end].
func (t *TenantExemplars[L]) Select(start, end int64, matches func(*L) bool) []Selected[L] {
	var out []Selected[L]
	for _, series := range t.series {
		if series.exemplars[0].exemplar.TimestampMs > end || series.exemplars[series.newest].exemplar.TimestampMs < start {
			continue
		}
		if !matches(&series.labels) {
			continue
		}
		var exemplars []Exemplar
		for _, s := range series.exemplars {
			if s.exemplar.TimestampMs >= start && s.exemplar.TimestampMs <= end {
				exemplars = append(exemplars, s.exemplar)
			}
		}
		if len(exemplars) > 0 {
			out = append(out, Selected[L]{Labels: series.labels, Exemplars: exemplars})
		}
	}
	return out
}

// Inserted is an exemplar with its series, for snapshots.
type Inserted[L any] struct {
	SeriesID uint64
	Labels   L
	Exemplar Exemplar
}

// InInsertionOrder returns every exemplar in insertion order, for snapshots.
func (t *TenantExemplars[L]) InInsertionOrder() []Inserted[L] {
	var out []Inserted[L]
	for _, entry := range t.order[t.head:] {
		series, ok := t.series[entry.seriesID]
		if !ok {
			continue
		}
		for _, s := range series.exemplars {
			if s.sequence == entry.sequence {
				out = append(out, Inserted[L]{entry.seriesID, series.labels, s.exemplar})
				break
			}
		}
	}
	return out
}

// labelHash orders exemplars with the same timestamp and value, like Prometheus's labels.Hash.
func labelHash(e *Exemplar) uint64 {
	var b []byte
	for _, label := range e.Labels {
		b = append(b, label.Name...)
		b = append(b, 0xff)
		b = append(b, label.Value...)
		b = append(b, 0xff)
	}
	return xxhash.Sum64(b)
}

// detached copies the labels: decoded labels share their Kafka record's buffer, which a stored
// exemplar would otherwise keep alive for as long as it is stored.
func detached(e Exemplar) Exemplar {
	labels := make([]Label, len(e.Labels))
	for i, label := range e.Labels {
		labels[i] = Label{strings.Clone(label.Name), strings.Clone(label.Value)}
	}
	e.Labels = labels
	return e
}
