// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

// E8: counts series per step-sized time bucket of a block range from chunk metas,
// so a short spike shows up, and checks every bucket against the truth.

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/tools/headcount/model"
)

// BucketCounts is a series count per time bucket inside one block range.
type BucketCounts struct {
	Range  BlockRange
	Step   time.Duration
	Metric string // empty for every series

	// Starts[i] is bucket i's start; it covers [Starts[i], Starts[i]+Step).
	Starts []int64
	Counts []int

	SeriesRead int // series whose chunk metas were read
	Elapsed    time.Duration
}

// CountBuckets counts, for every step-sized bucket of block range r, the
// series of metric (every series if metric is empty) with a chunk
// overlapping the bucket, from the chunk metas in the full index of r's
// blocks, which must share no series. A chunk's time span reaches from its
// first sample to its last, so a gap inside one chunk still counts the
// series as present.
func CountBuckets(bucketDir string, r BlockRange, step time.Duration, metric string) (BucketCounts, error) {
	stepMs := step.Milliseconds()
	if stepMs <= 0 || (r.MaxT-r.MinT)%stepMs != 0 {
		return BucketCounts{}, fmt.Errorf("step %s must divide the block range [%d, %d)", step, r.MinT, r.MaxT)
	}
	dirs, err := rangeBlockDirs(bucketDir, r)
	if err != nil {
		return BucketCounts{}, err
	}

	n := int((r.MaxT - r.MinT) / stepMs)
	bc := BucketCounts{Range: r, Step: step, Metric: metric, Starts: make([]int64, n), Counts: make([]int, n)}
	for i := range bc.Starts {
		bc.Starts[i] = r.MinT + int64(i)*stepMs
	}

	start := time.Now()
	for _, dir := range dirs {
		if err := addBlockBuckets(dir, &bc); err != nil {
			return BucketCounts{}, fmt.Errorf("block %s: %w", filepath.Base(dir), err)
		}
	}
	bc.Elapsed = time.Since(start)
	return bc, nil
}

func addBlockBuckets(dir string, bc *BucketCounts) error {
	r, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return err
	}
	defer r.Close()

	name, value := "", ""
	if bc.Metric != "" {
		name, value = "__name__", bc.Metric
	}
	p, err := r.Postings(context.Background(), name, value)
	if err != nil {
		return err
	}
	stepMs := bc.Step.Milliseconds()
	var b labels.ScratchBuilder
	var chks []chunks.Meta
	seen := make([]bool, len(bc.Counts))
	for p.Next() {
		if err := r.Series(p.At(), &b, &chks); err != nil {
			return err
		}
		bc.SeriesRead++
		clear(seen)
		for _, c := range chks {
			// Chunk times are inclusive; clamp to the block range.
			first := max(0, (c.MinTime-bc.Range.MinT)/stepMs)
			last := min(int64(len(seen)-1), (c.MaxTime-bc.Range.MinT)/stepMs)
			for i := first; i <= last; i++ {
				seen[i] = true
			}
		}
		for i, s := range seen {
			if s {
				bc.Counts[i]++
			}
		}
	}
	return p.Err()
}

// rangeBlockDirs returns the directories of the blocks whose range is
// exactly r, in block-ID order, after checking that they share no series.
func rangeBlockDirs(bucketDir string, r BlockRange) ([]string, error) {
	metas, err := readMetas(bucketDir)
	if err != nil {
		return nil, err
	}
	var chosen []*block.Meta
	for _, m := range metas {
		if m.MinTime == r.MinT && m.MaxTime == r.MaxT {
			chosen = append(chosen, m)
		}
	}
	if len(chosen) == 0 {
		return nil, fmt.Errorf("no block covers exactly [%d, %d)", r.MinT, r.MaxT)
	}
	if err := checkDisjointShards(chosen); err != nil {
		return nil, err
	}
	sort.Slice(chosen, func(i, j int) bool { return chosen[i].ULID.Compare(chosen[j].ULID) < 0 })
	dirs := make([]string, len(chosen))
	for i, m := range chosen {
		dirs[i] = filepath.Join(bucketDir, "anonymous", m.ULID.String())
	}
	return dirs, nil
}

// E8Result is one block range's bucket counts against the truth.
type E8Result struct {
	BucketCounts
	Truth []int
}

// Pass reports whether every bucket's count equals the truth.
func (r E8Result) Pass() bool {
	for i := range r.Counts {
		if r.Counts[i] != r.Truth[i] {
			return false
		}
	}
	return true
}

// RunE8 counts metric (every series if empty) per step-sized bucket of r
// and compares each bucket with the population's truth.
func RunE8(bucketDir string, pop *model.Model, r BlockRange, step time.Duration, metric string) (E8Result, error) {
	bc, err := CountBuckets(bucketDir, r, step, metric)
	if err != nil {
		return E8Result{}, err
	}
	var matchers []*labels.Matcher
	if metric != "" {
		matchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", metric)}
	}
	res := E8Result{BucketCounts: bc, Truth: make([]int, len(bc.Starts))}
	for i, s := range bc.Starts {
		res.Truth[i] = pop.Truth(matchers, s, s+step.Milliseconds())
	}
	return res, nil
}
