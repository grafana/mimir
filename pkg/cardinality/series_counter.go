// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/axiomhq/hyperloglog"
	"github.com/prometheus/prometheus/model/labels"
)

// SketchPrecision is the HyperLogLog precision of the sketches a series
// counter sends for large groups. Every source of a cardinality estimate
// must use the same precision, so the querier can merge their sketches.
const SketchPrecision = 11

// MaxCounterBuckets caps the buckets one count can ask for, so a request
// can't ask for a huge count slice per group.
const MaxCounterBuckets = 1440

// CounterOptions says what a SeriesCounter counts.
type CounterOptions struct {
	// MinT and MaxT are the window in milliseconds, [MinT, MaxT).
	MinT, MaxT int64
	// Step, when positive, cuts the window into buckets of this width, and
	// each group gets one count per bucket.
	Step int64
	// GroupBy is the label to group by, __name__ when empty.
	GroupBy string
	// Hashes keeps each group's label-set hashes instead of a count, so a
	// caller can union them with another source's. Not allowed with Step.
	Hashes bool
	// SketchAbove, with Hashes, sends a group with more series than this as
	// an HLL sketch instead of its hashes. 0 means always hashes.
	SketchAbove int64
}

// ChunkRange is the time range of one chunk, both ends included, as in
// chunk metas.
type ChunkRange struct {
	MinTime, MaxTime int64
}

// CounterGroup is one group's result. Exactly one of Counts, Hashes and
// Sketch is set.
type CounterGroup struct {
	Value  string
	Counts []int64
	Hashes []uint64
	// Sketch is a marshalled axiomhq/hyperloglog sketch at SketchPrecision.
	Sketch []byte
}

// SeriesCounter counts series per value of one label over a window, from
// each series' labels and chunk time ranges, without reading samples. It is
// the counting core shared by every source of a cardinality estimate, so
// their counts, buckets and hashes agree. It is not safe for concurrent use.
type SeriesCounter struct {
	opts    CounterOptions
	buckets int
	counted int64
	counts  map[string][]int64
	sets    map[string]map[uint64]struct{}
	seen    []bool // scratch, one flag per bucket
}

// NewSeriesCounter validates opts and returns a counter for them.
func NewSeriesCounter(opts CounterOptions) (*SeriesCounter, error) {
	if opts.MaxT <= opts.MinT {
		return nil, fmt.Errorf("the window end %d must be after its start %d", opts.MaxT, opts.MinT)
	}
	if opts.GroupBy == "" {
		opts.GroupBy = labels.MetricName
	}
	if opts.Step < 0 {
		return nil, errors.New("step can't be negative")
	}
	if opts.SketchAbove < 0 {
		return nil, errors.New("sketch_above can't be negative")
	}
	c := &SeriesCounter{opts: opts, buckets: 1, counts: map[string][]int64{}, sets: map[string]map[uint64]struct{}{}}
	if opts.Step > 0 {
		if opts.Hashes {
			return nil, errors.New("hashes can't be combined with a step")
		}
		if (opts.MaxT-opts.MinT)%opts.Step != 0 {
			return nil, fmt.Errorf("step %d doesn't divide the window [%d, %d)", opts.Step, opts.MinT, opts.MaxT)
		}
		c.buckets = int((opts.MaxT - opts.MinT) / opts.Step)
		if c.buckets > MaxCounterBuckets {
			return nil, fmt.Errorf("the window has %d buckets of the step, more than %d", c.buckets, MaxCounterBuckets)
		}
		c.seen = make([]bool, c.buckets)
	}
	return c, nil
}

// Add counts one series with the given chunks. A series with no chunk that
// overlaps the window is not counted, and a series counts once per bucket
// however many of its chunks touch that bucket. It reports whether the
// series was counted.
func (c *SeriesCounter) Add(lset labels.Labels, chunks []ChunkRange) bool {
	inWindow := false
	for _, r := range chunks {
		if r.MaxTime >= c.opts.MinT && r.MinTime < c.opts.MaxT {
			inWindow = true
			break
		}
	}
	if !inWindow {
		return false
	}
	c.counted++
	v := lset.Get(c.opts.GroupBy)
	if c.opts.Hashes {
		set := c.sets[v]
		if set == nil {
			set = map[uint64]struct{}{}
			c.sets[v] = set
		}
		set[labels.StableHash(lset)] = struct{}{}
		return true
	}
	counts := c.counts[v]
	if counts == nil {
		counts = make([]int64, c.buckets)
		c.counts[v] = counts
	}
	if c.opts.Step == 0 {
		counts[0]++
		return true
	}
	clear(c.seen)
	for _, r := range chunks {
		if r.MaxTime < c.opts.MinT || r.MinTime >= c.opts.MaxT {
			continue
		}
		first := max(0, (r.MinTime-c.opts.MinT)/c.opts.Step)
		last := min(int64(c.buckets-1), (r.MaxTime-c.opts.MinT)/c.opts.Step)
		for i := first; i <= last; i++ {
			c.seen[i] = true
		}
	}
	for i, hit := range c.seen {
		if hit {
			counts[i]++
		}
	}
	return true
}

// Counted is the number of series counted so far.
func (c *SeriesCounter) Counted() int64 { return c.counted }

// Groups returns every group's result, sorted by value.
func (c *SeriesCounter) Groups() ([]CounterGroup, error) {
	out := make([]CounterGroup, 0, len(c.counts)+len(c.sets))
	for v, counts := range c.counts {
		out = append(out, CounterGroup{Value: v, Counts: counts})
	}
	for v, set := range c.sets {
		g := CounterGroup{Value: v}
		if c.opts.SketchAbove > 0 && int64(len(set)) > c.opts.SketchAbove {
			sk, err := hyperloglog.NewSketch(SketchPrecision, false)
			if err != nil {
				return nil, err
			}
			for h := range set {
				sk.InsertHash(h)
			}
			if g.Sketch, err = sk.MarshalBinary(); err != nil {
				return nil, err
			}
		} else {
			g.Hashes = make([]uint64, 0, len(set))
			for h := range set {
				g.Hashes = append(g.Hashes, h)
			}
			slices.Sort(g.Hashes)
		}
		out = append(out, g)
	}
	slices.SortFunc(out, func(a, b CounterGroup) int { return strings.Compare(a.Value, b.Value) })
	return out, nil
}
