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
	"fmt"
	"math"
	"slices"
	"unsafe"

	"github.com/tylertreat/BoomFilters"
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

type labelValueSketch struct {
	s              *boom.CountMinSketch
	distinctValues int64
}

type LabelValuesSketches struct {
	labelNames map[string]labelValueSketch
}

func (l LabelValuesSketches) LabelValuesCount(ctx context.Context, name string) (int64, error) {
	s, ok := l.labelNames[name]
	if !ok {
		return 0, fmt.Errorf("no sketch found for label %q", name)
	}
	return int64(s.distinctValues), nil
}

func (l LabelValuesSketches) LabelValuesCardinality(ctx context.Context, name string, values ...string) (int64, error) {
	valueSketch, ok := l.labelNames[name]
	if !ok {
		return 0, fmt.Errorf("no sketch found for label %q", name)
	}
	totalCount := 0
	if len(values) == 0 {
		return int64(valueSketch.s.TotalCount()), nil
	}
	for _, value := range values {
		valBytes := yoloBytes(value)
		totalCount += int(valueSketch.s.Count(valBytes))
	}
	return int64(totalCount), nil
}

func yoloBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func (p *MemPostings) LabelValuesSketches() LabelValuesSketches {
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	sketches := LabelValuesSketches{}

	sketches.labelNames = make(map[string]labelValueSketch, len(p.m))

	for name, m := range p.m {
		if name == "" {
			continue
		}
		sketch := labelValueSketch{
			s:              boom.NewCountMinSketch(0.01, 0.01),
			distinctValues: int64(len(m)),
		}
		for value, postings := range m {
			valBytes := yoloBytes(value)
			for range postings {
				sketch.s.Add(valBytes)
			}
		}
		sketches.labelNames[name] = sketch
	}
	return sketches
}
