// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// Reads an estimate can use, named as in Mimir's cardinality/estimate route.
const (
	ReadIndexHeader = "index_header"
	ReadFullIndex   = "full_index"
)

// CardinalityEstimateRequest is the library's version of Mimir's cardinality/estimate
// route: series matching Matchers with a chunk in [MinT, MaxT), grouped by
// GroupBy (metric name when empty), optionally per Step-wide bucket.
type CardinalityEstimateRequest struct {
	Matchers   []*labels.Matcher
	MinT, MaxT int64
	Step       int64
	GroupBy    string
	// MaxSeries stops after that many series and marks a lower bound.
	MaxSeries int
	// Snap widens a window inside one block range to that range.
	Snap bool
}

// Estimate is one answer, with the read that produced it.
type Estimate struct {
	Read       string
	Dedup      bool
	Snapped    bool
	MinT, MaxT int64
	// Counts holds one count per bucket for each group.
	Counts     map[string][]int
	LowerBound bool
	Blocks     int
	SeriesRead int
	Elapsed    time.Duration
}

// CardinalityEstimate answers req from the blocks in bucketDir the way the
// querier does: every metric name over exactly one block range of
// series-disjoint blocks, with no matchers and no step, from the
// index-headers; anything else from the full index, with dedup by label-set
// hash when the blocks may share series.
func CardinalityEstimate(bucketDir string, req CardinalityEstimateRequest) (Estimate, error) {
	if req.MaxT <= req.MinT {
		return Estimate{}, errors.New("MaxT must be after MinT")
	}
	if req.GroupBy == "" {
		req.GroupBy = labels.MetricName
	}
	metas, err := readMetas(bucketDir)
	if err != nil {
		return Estimate{}, err
	}
	out := Estimate{MinT: req.MinT, MaxT: req.MaxT}
	blocks := overlapping(metas, req.MinT, req.MaxT)
	if req.Snap && len(blocks) > 0 {
		lo, hi := blocks[0].MinTime, blocks[0].MaxTime
		for _, m := range blocks {
			if m.MinTime != lo || m.MaxTime != hi {
				return Estimate{}, fmt.Errorf("the window [%d, %d) touches more than one block range, so it can't be snapped to one", req.MinT, req.MaxT)
			}
		}
		out.Snapped = lo != req.MinT || hi != req.MaxT
		req.MinT, req.MaxT, out.MinT, out.MaxT = lo, hi, lo, hi
		blocks = overlapping(metas, lo, hi)
	}
	oneRange := len(blocks) > 0 && checkDisjointShards(blocks) == nil
	for _, m := range blocks {
		oneRange = oneRange && m.MinTime == blocks[0].MinTime && m.MaxTime == blocks[0].MaxTime
	}
	out.Blocks = len(blocks)
	start := time.Now()

	if oneRange && len(req.Matchers) == 0 && req.Step == 0 && req.GroupBy == labels.MetricName &&
		blocks[0].MinTime == req.MinT && blocks[0].MaxTime == req.MaxT {
		nc, err := NameCountsForRange(bucketDir, BlockRange{MinT: req.MinT, MaxT: req.MaxT})
		if err != nil {
			return Estimate{}, err
		}
		out.Read = ReadIndexHeader
		out.Counts = make(map[string][]int, len(nc.Counts))
		for name, n := range nc.Counts {
			out.Counts[name] = []int{n}
		}
		out.Elapsed = time.Since(start)
		return out, nil
	}

	out.Read, out.Dedup = ReadFullIndex, !oneRange && len(blocks) > 1
	if out.Dedup && req.Step > 0 {
		return Estimate{}, errors.New("per-bucket counts need a window inside one block range whose blocks share no series")
	}
	c, err := newCounter(req, out.Dedup)
	if err != nil {
		return Estimate{}, err
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].ULID.Compare(blocks[j].ULID) < 0 })
	for _, m := range blocks {
		finished, err := c.addBlock(filepath.Join(bucketDir, "anonymous", m.ULID.String()))
		if err != nil {
			return Estimate{}, fmt.Errorf("block %s: %w", m.ULID, err)
		}
		if !finished {
			out.LowerBound = true
			break
		}
	}
	out.Counts, out.SeriesRead = c.result(), c.read
	out.Elapsed = time.Since(start)
	return out, nil
}

func overlapping(metas []*block.Meta, minT, maxT int64) []*block.Meta {
	var out []*block.Meta
	for _, m := range metas {
		if m.MinTime < maxT && m.MaxTime > minT {
			out = append(out, m)
		}
	}
	return out
}

// counter reads series entries and chunk metas from full indexes.
type counter struct {
	req     CardinalityEstimateRequest
	dedup   bool
	buckets int
	read    int
	counts  map[string][]int
	sets    map[string]map[uint64]struct{}
	seen    []bool
}

func newCounter(req CardinalityEstimateRequest, dedup bool) (*counter, error) {
	c := &counter{req: req, dedup: dedup, buckets: 1, counts: map[string][]int{}, sets: map[string]map[uint64]struct{}{}}
	if req.Step > 0 {
		if (req.MaxT-req.MinT)%req.Step != 0 {
			return nil, fmt.Errorf("step %d doesn't divide the window [%d, %d)", req.Step, req.MinT, req.MaxT)
		}
		c.buckets = int((req.MaxT - req.MinT) / req.Step)
	}
	c.seen = make([]bool, c.buckets)
	return c, nil
}

// addBlock adds one block's series to c. It returns false if MaxSeries
// was reached before the block was finished.
func (c *counter) addBlock(dir string) (bool, error) {
	r, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return false, err
	}
	defer r.Close()

	ctx := context.Background()
	var p index.Postings
	if len(c.req.Matchers) == 0 {
		name, value := index.AllPostingsKey()
		p, err = r.Postings(ctx, name, value)
	} else {
		p, err = tsdb.PostingsForMatchers(ctx, r, c.req.Matchers...)
	}
	if err != nil {
		return false, err
	}
	var (
		b    labels.ScratchBuilder
		chks []chunks.Meta
	)
	for p.Next() {
		if err := r.Series(p.At(), &b, &chks); err != nil {
			return false, err
		}
		if !c.mark(chks) {
			continue
		}
		if c.req.MaxSeries > 0 && c.read >= c.req.MaxSeries {
			return false, nil
		}
		c.read++
		lset := b.Labels()
		v := lset.Get(c.req.GroupBy)
		if c.dedup {
			set := c.sets[v]
			if set == nil {
				set = map[uint64]struct{}{}
				c.sets[v] = set
			}
			set[labels.StableHash(lset)] = struct{}{}
			continue
		}
		counts := c.counts[v]
		if counts == nil {
			counts = make([]int, c.buckets)
			c.counts[v] = counts
		}
		for i, hit := range c.seen {
			if hit {
				counts[i]++
			}
		}
	}
	return true, p.Err()
}

// mark sets c.seen to the buckets the series' chunks touch and reports
// whether there is any. Chunk times are inclusive.
func (c *counter) mark(chks []chunks.Meta) bool {
	clear(c.seen)
	found := false
	for _, m := range chks {
		if m.MaxTime < c.req.MinT || m.MinTime >= c.req.MaxT {
			continue
		}
		found = true
		if c.req.Step == 0 {
			c.seen[0] = true
			continue
		}
		first := max(0, (m.MinTime-c.req.MinT)/c.req.Step)
		last := min(int64(c.buckets-1), (m.MaxTime-c.req.MinT)/c.req.Step)
		for i := first; i <= last; i++ {
			c.seen[i] = true
		}
	}
	return found
}

func (c *counter) result() map[string][]int {
	if !c.dedup {
		return c.counts
	}
	out := make(map[string][]int, len(c.sets))
	for v, set := range c.sets {
		out[v] = []int{len(set)}
	}
	return out
}
