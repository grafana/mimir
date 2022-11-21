// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/postingsstats.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"math"
	"sort"
)

// Stat holds values for a single cardinality statistic.
type Stat struct {
	Name  string
	Count uint64
}

type maxHeap struct {
	maxLength int
	minValue  uint64
	minIndex  int
	Items     []Stat
}

func (m *maxHeap) init(len int) {
	m.maxLength = len
	m.minValue = math.MaxUint64
	m.Items = make([]Stat, 0, len)
}

func (m *maxHeap) push(item Stat) {
	if len(m.Items) < m.maxLength {
		if item.Count < m.minValue {
			m.minValue = item.Count
			m.minIndex = len(m.Items)
		}
		m.Items = append(m.Items, item)
		return
	}
	if item.Count < m.minValue {
		return
	}

	m.Items[m.minIndex] = item
	m.minValue = item.Count

	for i, stat := range m.Items {
		if stat.Count < m.minValue {
			m.minValue = stat.Count
			m.minIndex = i
		}
	}
}

func (m *maxHeap) get() []Stat {
	sort.Slice(m.Items, func(i, j int) bool {
		return m.Items[i].Count > m.Items[j].Count
	})
	return m.Items
}
