// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

// E7: counts series live in an arbitrary time window, once by snapping to the block range
// and once by checking chunk metas, and checks both against the truth.

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/tools/headcount/model"
)

// WindowCount is the tenant-wide series count for a window that need not
// line up with block boundaries, computed two ways.
type WindowCount struct {
	MinT, MaxT int64

	// Snapped is the block range holding the window, and SnappedCount the
	// count for that range from index-headers only. It answers for the
	// snapped range, which must be reported instead of the one asked for.
	Snapped      BlockRange
	SnappedCount int
	SnappedTime  time.Duration

	// CheckedCount is the number of series with a chunk overlapping
	// [MinT, MaxT), from each series' chunk metas in the full index.
	CheckedCount  int
	CheckedSeries int // series whose chunk metas were read
	CheckedTime   time.Duration
	// MaxChunkSpan is the longest MaxTime-MinTime of any chunk read. A
	// chunk overlapping the window can have its samples up to this far
	// outside it, which bounds CheckedCount's error.
	MaxChunkSpan int64
}

// CountWindow counts series live in [minT, maxT), which must lie inside
// one block range of bucketDir, by snapping to that range and by checking
// chunk metas.
func CountWindow(bucketDir string, minT, maxT int64) (WindowCount, error) {
	ranges, err := BlockRanges(bucketDir)
	if err != nil {
		return WindowCount{}, err
	}
	wc := WindowCount{MinT: minT, MaxT: maxT}
	found := false
	for _, r := range ranges {
		if r.MinT <= minT && maxT <= r.MaxT {
			wc.Snapped, found = r, true
			break
		}
	}
	if !found {
		return WindowCount{}, fmt.Errorf("window [%d, %d) is not inside one block range", minT, maxT)
	}

	nc, err := NameCountsForRange(bucketDir, wc.Snapped)
	if err != nil {
		return WindowCount{}, err
	}
	wc.SnappedCount, wc.SnappedTime = nc.Total(), nc.Elapsed

	start := time.Now()
	for _, id := range nc.Blocks {
		if err := addChunkCheckedCount(filepath.Join(bucketDir, "anonymous", id.String()), &wc); err != nil {
			return WindowCount{}, fmt.Errorf("block %s: %w", id, err)
		}
	}
	wc.CheckedTime = time.Since(start)
	return wc, nil
}

func addChunkCheckedCount(dir string, wc *WindowCount) error {
	r, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return err
	}
	defer r.Close()

	p, err := r.Postings(context.Background(), "", "")
	if err != nil {
		return err
	}
	var b labels.ScratchBuilder
	var chks []chunks.Meta
	for p.Next() {
		if err := r.Series(p.At(), &b, &chks); err != nil {
			return err
		}
		wc.CheckedSeries++
		live := false
		for _, c := range chks {
			wc.MaxChunkSpan = max(wc.MaxChunkSpan, c.MaxTime-c.MinTime)
			// Chunk times are inclusive; the window's end is exclusive.
			if c.MinTime < wc.MaxT && wc.MinT <= c.MaxTime {
				live = true
			}
		}
		if live {
			wc.CheckedCount++
		}
	}
	return p.Err()
}

// E7Result is one window's counts against the population's truth.
type E7Result struct {
	WindowCount
	// Truth is the count for the window asked for, TruthSnapped for the
	// snapped range, and TruthWidened for the window widened by
	// MaxChunkSpan on both sides.
	Truth, TruthSnapped, TruthWidened int
}

// Pass reports whether the snapped count is exact for the snapped range
// and the chunk-checked count lies between the window's truth and the
// widened window's truth.
func (r E7Result) Pass() bool {
	return r.SnappedCount == r.TruthSnapped && r.Truth <= r.CheckedCount && r.CheckedCount <= r.TruthWidened
}

// RunE7 runs CountWindow for each window and compares it with the truth.
func RunE7(bucketDir string, pop *model.Model, windows [][2]int64) ([]E7Result, error) {
	var out []E7Result
	for _, w := range windows {
		wc, err := CountWindow(bucketDir, w[0], w[1])
		if err != nil {
			return nil, err
		}
		out = append(out, E7Result{
			WindowCount:  wc,
			Truth:        pop.Truth(nil, w[0], w[1]),
			TruthSnapped: pop.Truth(nil, wc.Snapped.MinT, wc.Snapped.MaxT),
			TruthWidened: pop.Truth(nil, w[0]-wc.MaxChunkSpan, w[1]+wc.MaxChunkSpan),
		})
	}
	return out, nil
}
