// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/axiomhq/hyperloglog"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
)

// SketchPrecision is the HyperLogLog precision of every sketch, so sketches from different sources can be merged.
const SketchPrecision = 11

// ErrHashesNeedSketchAbove refuses hashes without sketch_above, since a group's hashes go in one message.
var ErrHashesNeedSketchAbove = errors.New("hashes need a positive sketch_above, since a group over it is sent as a sketch and a group's hashes go in one message")

// MaxCounterBuckets caps the buckets one count can ask for.
const MaxCounterBuckets = 1440

// CounterOptions says what a SeriesCounter counts.
type CounterOptions struct {
	// MinT and MaxT are the window in milliseconds, [MinT, MaxT).
	MinT, MaxT int64
	// Step, when positive, starts a bucket every Step.
	Step int64
	// Window is each bucket's width, 0 means Step. Only with Step.
	Window int64
	// GroupBy is the label to group by, __name__ when empty.
	GroupBy string
	// Hashes keeps each group's label-set hashes instead of a count. Not allowed with Step.
	Hashes bool
	// SketchAbove, with Hashes, sends a group with more series than this as a sketch. Must be positive with Hashes.
	SketchAbove int64
}

// ChunkRange is the time range of one chunk, both ends included.
type ChunkRange struct {
	MinTime, MaxTime int64
}

// SeriesCounter counts series per value of one label over a window, from
// chunk time ranges. It is not safe for concurrent use, apart from Counted.
type SeriesCounter struct {
	opts    CounterOptions
	buckets int
	counted atomic.Int64
	// hashes is the distinct hashes held, with Hashes.
	hashes int64
	counts map[string][]int64
	sets   map[string]map[uint64]struct{}
	marks  *BucketMarks // scratch for one series' buckets
}

// NewSeriesCounter validates opts and returns a counter for them.
func NewSeriesCounter(opts CounterOptions) (*SeriesCounter, error) {
	if opts.MaxT <= opts.MinT {
		return nil, fmt.Errorf("the window end %d must be after its start %d", opts.MaxT, opts.MinT)
	}
	if opts.GroupBy == "" {
		opts.GroupBy = model.MetricNameLabel
	}
	if opts.Step < 0 {
		return nil, errors.New("step can't be negative")
	}
	if opts.SketchAbove < 0 {
		return nil, errors.New("sketch_above can't be negative")
	}
	if opts.Hashes && opts.SketchAbove == 0 {
		return nil, ErrHashesNeedSketchAbove
	}
	if opts.Window != 0 && opts.Step == 0 {
		return nil, errors.New("a window needs a step")
	}
	c := &SeriesCounter{opts: opts, buckets: 1, counts: map[string][]int64{}, sets: map[string]map[uint64]struct{}{}}
	if opts.Step > 0 {
		if opts.Hashes {
			return nil, errors.New("hashes can't be combined with a step")
		}
		var err error
		if c.buckets, err = Buckets(opts.MinT, opts.MaxT, opts.Step, opts.Window); err != nil {
			return nil, err
		}
		c.marks = NewBucketMarks(c.buckets)
	}
	return c, nil
}

// Add counts one series, once per bucket its chunks touch. It reports whether
// the series was counted: not if no chunk overlaps the window.
func (c *SeriesCounter) Add(lset labels.Labels, chunks []ChunkRange) bool {
	var hash uint64
	if c.opts.Hashes {
		hash = labels.StableHash(lset)
	}
	return c.AddGroup(lset.Get(c.opts.GroupBy), hash, chunks)
}

// AddGroup is Add with the group-by value and, when counting hashes, the
// labels.StableHash already known. value is cloned for a new group.
func (c *SeriesCounter) AddGroup(value string, hash uint64, chunks []ChunkRange) bool {
	if !c.InWindow(chunks) {
		return false
	}
	counts := c.group(value, hash)
	if counts == nil {
		return true
	}
	if c.opts.Step == 0 {
		counts[0]++
		return true
	}
	for _, r := range chunks {
		if r.MaxTime < c.opts.MinT || r.MinTime >= c.opts.MaxT {
			continue
		}
		c.marks.Mark(BucketSpan(c.opts.MinT, c.opts.Step, c.opts.Window, c.buckets, r.MinTime, r.MaxTime))
	}
	c.marks.Flush(counts)
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

// AddGroupInWindow is AddGroup for a series known to have a chunk in the
// window, without its chunks. With a step it counts nothing and returns false.
func (c *SeriesCounter) AddGroupInWindow(value string, hash uint64) bool {
	if c.opts.Step > 0 {
		return false
	}
	if counts := c.group(value, hash); counts != nil {
		counts[0]++
	}
	return true
}

// AddCount adds n series to value's group. Only a count with no step and no hashes can take it.
func (c *SeriesCounter) AddCount(value string, n int64) error {
	if c.opts.Step > 0 || c.opts.Hashes {
		return errors.New("a count with a step or hashes adds series one by one")
	}
	if n <= 0 {
		return nil
	}
	c.counted.Add(n - 1)
	c.group(value, 0)[0] += n
	return nil
}

// group returns value's bucket counts for the caller to increment, or nil when
// the counter keeps hashes and the hash was added.
func (c *SeriesCounter) group(value string, hash uint64) []int64 {
	c.counted.Add(1)
	if c.opts.Hashes {
		set := c.sets[value]
		if set == nil {
			set = map[uint64]struct{}{}
			c.sets[strings.Clone(value)] = set
		}
		if _, ok := set[hash]; !ok {
			set[hash] = struct{}{}
			c.hashes++
		}
		return nil
	}
	counts := c.counts[value]
	if counts == nil {
		counts = make([]int64, c.buckets)
		c.counts[strings.Clone(value)] = counts
	}
	return counts
}

// SeriesBuckets is a bitset of the buckets one series touches, for a caller
// that gathers a series' chunks from several sources before counting it once.
// nil touches none.
type SeriesBuckets []uint64

// MarkBuckets returns touched with the buckets the chunks overlap also set.
// It allocates a nil touched only when a chunk is in the window.
func (c *SeriesCounter) MarkBuckets(touched SeriesBuckets, chunks []ChunkRange) SeriesBuckets {
	for _, r := range chunks {
		if r.MaxTime < c.opts.MinT || r.MinTime >= c.opts.MaxT {
			continue
		}
		if touched == nil {
			touched = make(SeriesBuckets, (c.buckets+63)/64)
		}
		if c.opts.Step == 0 {
			touched[0] |= 1
			continue
		}
		first, last := BucketSpan(c.opts.MinT, c.opts.Step, c.opts.Window, c.buckets, r.MinTime, r.MaxTime)
		for i := first; i <= last; i++ {
			touched[i/64] |= 1 << (i % 64)
		}
	}
	return touched
}

// AddGroupBuckets is AddGroup for buckets gathered with MarkBuckets. It
// reports whether the series touched a bucket and was counted.
func (c *SeriesCounter) AddGroupBuckets(value string, hash uint64, touched SeriesBuckets) bool {
	any := false
	for _, w := range touched {
		any = any || w != 0
	}
	if !any {
		return false
	}
	counts := c.group(value, hash)
	if counts == nil {
		return true
	}
	for i := range c.buckets {
		if touched[i/64]&(1<<(i%64)) != 0 {
			counts[i]++
		}
	}
	return true
}

// GroupBy is the label the counter groups by.
func (c *SeriesCounter) GroupBy() string { return c.opts.GroupBy }

// Buckets is the number of buckets each group has: 1 without a step.
func (c *SeriesCounter) Buckets() int { return c.buckets }

// Counted is the number of series counted so far, including repeated hashes.
// It may be read while another goroutine adds.
func (c *SeriesCounter) Counted() int64 { return c.counted.Load() }

// HashesHeld is the distinct hashes the counter holds, with Hashes.
func (c *SeriesCounter) HashesHeld() int64 { return c.hashes }

// GroupCount is the number of distinct group-by values so far.
func (c *SeriesCounter) GroupCount() int { return len(c.counts) + len(c.sets) }

// Groups returns every group sorted by value. Exactly one of Counts, Hashes
// and Sketch is set on each.
func (c *SeriesCounter) Groups() ([]*cardinalitypb.SeriesCountGroup, error) {
	out := make([]*cardinalitypb.SeriesCountGroup, 0, len(c.counts)+len(c.sets))
	for v, counts := range c.counts {
		out = append(out, &cardinalitypb.SeriesCountGroup{Value: v, Counts: counts})
	}
	for v, set := range c.sets {
		g := &cardinalitypb.SeriesCountGroup{Value: v}
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
	slices.SortFunc(out, func(a, b *cardinalitypb.SeriesCountGroup) int { return strings.Compare(a.Value, b.Value) })
	return out, nil
}
