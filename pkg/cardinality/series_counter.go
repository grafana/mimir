// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
)

// CounterOptions says what a SeriesCounter counts.
type CounterOptions struct {
	// MinT and MaxT are the window in milliseconds, [MinT, MaxT).
	MinT, MaxT int64
	// GroupBy is the label to group by, __name__ when empty.
	GroupBy string
	// Hashes keeps each group's label-set hashes instead of a count.
	Hashes bool
}

// ChunkRange is the time range of one chunk, both ends included.
type ChunkRange struct {
	MinTime, MaxTime int64
}

// SeriesCounter counts series per value of one label over a window, from
// chunk time ranges. It is not safe for concurrent use, apart from Counted.
type SeriesCounter struct {
	opts    CounterOptions
	counted atomic.Int64
	// hashes is the distinct hashes held, with Hashes.
	hashes int64
	counts map[string]int64
	sets   map[string]map[uint64]struct{}
}

// NewSeriesCounter validates opts and returns a counter for them.
func NewSeriesCounter(opts CounterOptions) (*SeriesCounter, error) {
	if opts.MaxT <= opts.MinT {
		return nil, fmt.Errorf("the window end %d must be after its start %d", opts.MaxT, opts.MinT)
	}
	if opts.GroupBy == "" {
		opts.GroupBy = model.MetricNameLabel
	}
	return &SeriesCounter{opts: opts, counts: map[string]int64{}, sets: map[string]map[uint64]struct{}{}}, nil
}

// Add counts one series. It reports whether the series was counted: not if no
// chunk overlaps the window.
func (c *SeriesCounter) Add(lset labels.Labels, chunks []ChunkRange) bool {
	if !c.InWindow(chunks) {
		return false
	}
	var hash uint64
	if c.opts.Hashes {
		hash = labels.StableHash(lset)
	}
	c.AddGroupInWindow(lset.Get(c.opts.GroupBy), hash)
	return true
}

// InWindow reports whether one of the chunks overlaps the window.
func (c *SeriesCounter) InWindow(chunks []ChunkRange) bool {
	for _, r := range chunks {
		if r.MaxTime >= c.opts.MinT && r.MinTime < c.opts.MaxT {
			return true
		}
	}
	return false
}

// AddGroupInWindow counts a series known to have a chunk in the window, with
// its group-by value and, when counting hashes, its labels.StableHash. value is
// cloned for a new group.
func (c *SeriesCounter) AddGroupInWindow(value string, hash uint64) {
	c.counted.Add(1)
	if !c.opts.Hashes {
		if _, ok := c.counts[value]; !ok {
			value = strings.Clone(value)
		}
		c.counts[value]++
		return
	}
	set := c.sets[value]
	if set == nil {
		set = map[uint64]struct{}{}
		c.sets[strings.Clone(value)] = set
	}
	if _, ok := set[hash]; !ok {
		set[hash] = struct{}{}
		c.hashes++
	}
}

// AddCount adds n series to value's group. Only a count without hashes can take it.
func (c *SeriesCounter) AddCount(value string, n int64) error {
	if c.opts.Hashes {
		return errors.New("a count with hashes adds series one by one")
	}
	if n <= 0 {
		return nil
	}
	c.counted.Add(n)
	if _, ok := c.counts[value]; !ok {
		value = strings.Clone(value)
	}
	c.counts[value] += n
	return nil
}

// GroupBy is the label the counter groups by.
func (c *SeriesCounter) GroupBy() string { return c.opts.GroupBy }

// Counted is the number of series counted so far, including repeated hashes.
// It may be read while another goroutine adds.
func (c *SeriesCounter) Counted() int64 { return c.counted.Load() }

// HashesHeld is the distinct hashes the counter holds, with Hashes.
func (c *SeriesCounter) HashesHeld() int64 { return c.hashes }

// GroupCount is the number of distinct group-by values so far.
func (c *SeriesCounter) GroupCount() int { return len(c.counts) + len(c.sets) }

// Groups returns every group sorted by value. Exactly one of Counts and Hashes
// is set on each; Counts holds one count.
func (c *SeriesCounter) Groups() []*cardinalitypb.SeriesCountGroup {
	out := make([]*cardinalitypb.SeriesCountGroup, 0, len(c.counts)+len(c.sets))
	for v, n := range c.counts {
		out = append(out, &cardinalitypb.SeriesCountGroup{Value: v, Counts: []int64{n}})
	}
	for v, set := range c.sets {
		g := &cardinalitypb.SeriesCountGroup{Value: v, Hashes: make([]uint64, 0, len(set))}
		for h := range set {
			g.Hashes = append(g.Hashes, h)
		}
		slices.Sort(g.Hashes)
		out = append(out, g)
	}
	slices.SortFunc(out, func(a, b *cardinalitypb.SeriesCountGroup) int { return strings.Compare(a.Value, b.Value) })
	return out
}
