// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"context"
	"math"
	"slices"
	"time"
	"unsafe"

	boom "github.com/tylertreat/BoomFilters"
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

func (m *maxHeap) init(length int) {
	m.maxLength = length
	m.minValue = math.MaxUint64
	m.Items = make([]Stat, 0, length)
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
	slices.SortFunc(m.Items, func(a, b Stat) int {
		switch {
		case b.Count < a.Count:
			return -1
		case b.Count > a.Count:
			return 1
		default:
			return 0
		}
	})
	return m.Items
}

// LabelsValuesSketches contains count-min sketches of the values for each label name.
// It implements index.Statistics, which can be used to inform query plan generation.
type LabelsValuesSketches struct {
	labelNames          map[string]*LabelValuesSketch
	updateLastCompleted int64
}

type LabelValuesSketch struct {
	s              *boom.CountMinSketch
	distinctValues uint64
	lastUpdated    int64 // epoch time of last update
}

// LabelsValuesSketches builds a LabelsValuesSketches based on the label names and values
// currently in MemPostings. The postings mutex (p.mtx) is taken independently to generate a list of label names,
// and then on a per-label-name basis to produce a count-min sketch for each label name,
// because this process should be interruptable by, e.g., head compaction.
// If MemPostings is updated between generation of the label name list and sketch generation, that some new label
// names will be missed.
func (p *MemPostings) LabelsValuesSketches() LabelsValuesSketches {
	names := p.LabelNames()
	labelNames := make(map[string]*LabelValuesSketch, len(names))
	for _, name := range names {
		labelNames[name] = p.labelValuesSketchForLabelName(name)
	}
	return LabelsValuesSketches{
		labelNames:          labelNames,
		updateLastCompleted: time.Now().Unix(),
	}
}

// labelValuesSketchForLabelName returns a count-min sketch of distinct values for a given label name.
// If name is not in MemPostings, nil is returned.
func (p *MemPostings) labelValuesSketchForLabelName(name string) *LabelValuesSketch {
	if name == "" {
		return nil
	}
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	// This may be nil if, e.g., the postings have been updated since we read all label names.
	// Maybe a compaction happened; in any case, we ignore it since it's not currently in MemPostings.
	labelValuesPostings, ok := p.m[name]
	if !ok {
		return nil
	}

	sketch := LabelValuesSketch{
		s:              boom.NewCountMinSketch(0.01, 0.01),
		distinctValues: uint64(len(labelValuesPostings)),
		lastUpdated:    time.Now().Unix(),
	}
	for value, postings := range labelValuesPostings {
		valBytes := yoloBytes(value)
		sketch.s.AddN(valBytes, uint64(len(postings)))
	}

	return &sketch
}

// LabelValuesCount returns the number of values for a given label name.
func (lvs *LabelsValuesSketches) LabelValuesCount(_ context.Context, name string) (uint64, error) {
	return lvs.labelNames[name].distinctValues, nil
}

// LabelValuesCardinality calculates the cardinality of a given label name according to a count-min sketch.
// If values are provided, it returns the combined cardinality of all given values; otherwise,
// it returns the total cardinality across all values for the label name.
func (lvs *LabelsValuesSketches) LabelValuesCardinality(_ context.Context, name string, values ...string) (uint64, error) {
	sketch, ok := lvs.labelNames[name]
	if !ok {
		return 0, nil
	}

	if len(values) == 0 {
		return sketch.s.TotalCount(), nil
	}
	totalCount := uint64(0)
	for _, val := range values {
		valBytes := yoloBytes(val)
		totalCount += sketch.s.Count(valBytes)
	}
	return totalCount, nil
}

func yoloBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
