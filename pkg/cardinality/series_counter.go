// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/axiomhq/hyperloglog"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality/cardinalitypb"
)

// SketchPrecision is the HyperLogLog precision of the sketches a series
// counter sends for large groups. Every source of a cardinality estimate
// must use the same precision, so the querier can merge their sketches.
const SketchPrecision = 11

// ErrHashesNeedSketchAbove refuses a count that asks for hashes with no
// sketch_above: a group's hashes are sent in one message, so a group with
// millions of series would make a message over the gRPC size limit.
var ErrHashesNeedSketchAbove = errors.New("hashes need a positive sketch_above, since a group over it is sent as a sketch and a group's hashes go in one message")

// MaxCounterBuckets caps the buckets one count can ask for, so a request
// can't ask for a huge count slice per group.
const MaxCounterBuckets = 1440

// CounterOptions says what a SeriesCounter counts.
type CounterOptions struct {
	// MinT and MaxT are the window in milliseconds, [MinT, MaxT).
	MinT, MaxT int64
	// Step, when positive, starts a bucket every Step, and each group gets
	// one count per bucket.
	Step int64
	// Window is each bucket's width, as Buckets describes; 0 means Step.
	// Only with Step.
	Window int64
	// GroupBy is the label to group by, __name__ when empty.
	GroupBy string
	// Hashes keeps each group's label-set hashes instead of a count, so a
	// caller can union them with another source's. Not allowed with Step.
	Hashes bool
	// SketchAbove, with Hashes, sends a group with more series than this as
	// an HLL sketch instead of its hashes. It must be positive with Hashes:
	// a group's hashes go in one message, so a group over it is a sketch.
	SketchAbove int64
}

// ChunkRange is the time range of one chunk, both ends included, as in
// chunk metas.
type ChunkRange struct {
	MinTime, MaxTime int64
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
	marks   *BucketMarks // scratch for one series' buckets
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

// Add counts one series with the given chunks. A series with no chunk that
// overlaps the window is not counted, and a series counts once per bucket
// however many of its chunks touch that bucket. It reports whether the
// series was counted.
func (c *SeriesCounter) Add(lset labels.Labels, chunks []ChunkRange) bool {
	var hash uint64
	if c.opts.Hashes {
		hash = labels.StableHash(lset)
	}
	return c.AddGroup(lset.Get(c.opts.GroupBy), hash, chunks)
}

// AddGroup is Add for a caller that already has the series' value of the
// group-by label and, when counting hashes, its labels.StableHash. A new
// group's key is a clone of value, which may be a slice of the series' label
// data, so the group keeps only the value alive, not the whole label set.
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

// group counts one series into value's group and returns the group's
// bucket counts for the caller to add it to, or nil when the counter keeps
// hashes and the series' hash has been added.
func (c *SeriesCounter) group(value string, hash uint64) []int64 {
	c.counted++
	if c.opts.Hashes {
		set := c.sets[value]
		if set == nil {
			set = map[uint64]struct{}{}
			c.sets[strings.Clone(value)] = set
		}
		set[hash] = struct{}{}
		return nil
	}
	counts := c.counts[value]
	if counts == nil {
		counts = make([]int64, c.buckets)
		c.counts[strings.Clone(value)] = counts
	}
	return counts
}

// SeriesBuckets is which of a count's buckets one series touches, one bit
// per bucket, for a caller that gathers a series' chunks from several
// sources before counting it once: it holds the buckets touched instead of
// the chunks, so its size is bounded by the buckets, one word for a count
// with no step. nil touches none.
type SeriesBuckets []uint64

// MarkBuckets returns touched with the buckets the chunks overlap within
// the window set as well. A nil touched is allocated when a chunk is in the
// window, so it stays nil for a series with none.
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

// AddGroupBuckets is AddGroup for a series whose buckets were gathered with
// MarkBuckets. It reports whether the series was counted, which it is when
// it touches a bucket.
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

// Counted is the number of series counted so far.
func (c *SeriesCounter) Counted() int64 { return c.counted }

// GroupCount is the number of groups so far: the distinct values of the
// group-by label among the series counted.
func (c *SeriesCounter) GroupCount() int { return len(c.counts) + len(c.sets) }

// Groups returns every group's result, sorted by value, as the wire message
// every source streams: exactly one of Counts, Hashes and Sketch is set, and
// a Sketch is at SketchPrecision.
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
