// Copyright 2025 Grafana Labs
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
	"unsafe"

	boom "github.com/tylertreat/BoomFilters"
)

const countMinEpsilon = 0.005

type Statistics interface {
	// TotalSeries returns the number of series in the TSDB component.
	TotalSeries() uint64

	// LabelValuesCount returns the number of values for a label name. If the given label name does not exist,
	// it is valid to return 0.
	LabelValuesCount(ctx context.Context, name string) uint64

	// LabelValuesCardinality returns the cardinality of a given label name (i.e., the number of series which
	// contain that label name). If values are provided, it returns the combined cardinality of all given values;
	// otherwise, it returns the total cardinality across all values for the label name. If the label name does not exist,
	// it is valid to return 0.
	LabelValuesCardinality(ctx context.Context, name string, values ...string) uint64
}

// LabelsValuesSketches contains count-min sketches of the values for each label name.
// It implements index.Statistics, which can be used to inform query plan generation.
type LabelsValuesSketches struct {
	labelNames map[string]*LabelValuesSketch
}

type LabelValuesSketch struct {
	s              *boom.CountMinSketch
	distinctValues uint64
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
		labelNames: labelNames,
	}
}

// labelValuesSketchForLabelName returns a count-min sketch of distinct values for a given label name.
// If name is not in MemPostings, nil is returned.
func (p *MemPostings) labelValuesSketchForLabelName(name string) *LabelValuesSketch {
	if name == "" {
		return nil
	}

	// The read lock is acquired on a per-label-name basis to prevent holding the lock for a long time.
	// This way, other processes which require a read lock, such as head compaction,
	// can take it in a timely manner. As a consequence, producing count-min sketches across all label values
	// in MemPostings can end up taking a longer time.
	p.mtx.RLock()
	defer p.mtx.RUnlock()

	// This may be nil if, e.g., the postings have been updated since we read all label names.
	// Maybe a compaction happened; in any case, we ignore it since it's not currently in MemPostings.
	labelValuesPostings, ok := p.m[name]
	if !ok {
		return nil
	}

	sketch := LabelValuesSketch{
		s:              boom.NewCountMinSketch(countMinEpsilon, 0.01),
		distinctValues: uint64(len(labelValuesPostings)),
	}
	for value, postings := range labelValuesPostings {
		valBytes := yoloBytes(value)
		sketch.s.AddN(valBytes, uint64(len(postings)))
	}

	return &sketch
}

// LabelValuesCount returns the number of values for a given label name.
func (lvs *LabelsValuesSketches) LabelValuesCount(_ context.Context, name string) uint64 {
	sketch, ok := lvs.labelNames[name]
	if !ok {
		// If we don't find a sketch for a label name, we return 0 but no error, since we assume that the nonexistence
		// of a sketch is equivalent to the nonexistence of values for the label name.
		return 0
	}

	return sketch.distinctValues
}

// LabelValuesCardinality calculates the cardinality of a given label name according to a count-min sketch.
// If values are provided, it returns the combined cardinality of all given values (i.e.,
// the count of all series which contain any value for the label name); otherwise,
// it returns the total cardinality across all values for the label name (i.e.,
// the count of all series which have matching label values for the given label name).
func (lvs *LabelsValuesSketches) LabelValuesCardinality(_ context.Context, name string, values ...string) uint64 {
	sketch, ok := lvs.labelNames[name]
	if !ok {
		// If we don't find a sketch for a label name, we return 0 but no error, since we assume that the nonexistence
		// of a label name is equivalent to 0 cardinality
		return 0
	}

	if len(values) == 0 {
		return sketch.s.TotalCount()
	}
	totalCount := uint64(0)
	for _, val := range values {
		valBytes := yoloBytes(val)
		totalCount += sketch.s.Count(valBytes)
	}
	return totalCount
}

func yoloBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
