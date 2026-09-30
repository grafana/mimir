// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// WindowNameCounts is every metric name's series count over a window made
// of whole block ranges, computed two ways.
type WindowNameCounts struct {
	MinT, MaxT int64
	Ranges     []BlockRange

	// Summed adds each range's index-header counts, which counts a series
	// once for every range it lives in.
	Summed        map[string]int
	SummedTime    time.Duration
	SummedHeaders int64 // index-header file bytes read

	// Exact unions each name's series hashes across the blocks, from the
	// full index: every series once.
	Exact      map[string]int
	ExactTime  time.Duration
	SeriesRead int   // series whose labels were read and hashed
	IndexBytes int64 // index file bytes of the blocks read, object storage in a store-gateway
}

// NameCountsForWindow counts every metric name's series over [minT, maxT),
// which must be exactly a run of consecutive block ranges in bucketDir.
func NameCountsForWindow(bucketDir string, minT, maxT int64) (WindowNameCounts, error) {
	all, err := BlockRanges(bucketDir)
	if err != nil {
		return WindowNameCounts{}, err
	}
	w := WindowNameCounts{MinT: minT, MaxT: maxT, Summed: map[string]int{}, Exact: map[string]int{}}
	next := minT
	for _, r := range all {
		if r.MinT >= minT && r.MaxT <= maxT {
			if r.MinT != next {
				return WindowNameCounts{}, fmt.Errorf("block ranges do not cover [%d, %d) without a gap at %d", minT, maxT, next)
			}
			w.Ranges = append(w.Ranges, r)
			next = r.MaxT
		} else if r.MinT < maxT && minT < r.MaxT {
			return WindowNameCounts{}, fmt.Errorf("block range [%d, %d) cuts through the window [%d, %d)", r.MinT, r.MaxT, minT, maxT)
		}
	}
	if next != maxT {
		return WindowNameCounts{}, fmt.Errorf("block ranges end at %d, before the window's end %d", next, maxT)
	}

	start := time.Now()
	for _, r := range w.Ranges {
		nc, err := NameCountsForRange(bucketDir, r)
		if err != nil {
			return WindowNameCounts{}, err
		}
		for name, c := range nc.Counts {
			w.Summed[name] += c
		}
		w.SummedHeaders += nc.IndexHeaderBytes
	}
	w.SummedTime = time.Since(start)

	metas, err := readMetas(bucketDir)
	if err != nil {
		return WindowNameCounts{}, err
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].ULID.Compare(metas[j].ULID) < 0 })
	start = time.Now()
	sets := map[string]map[uint64]struct{}{}
	for _, m := range metas {
		if m.MinTime < minT || m.MaxTime > maxT {
			continue
		}
		dir := filepath.Join(bucketDir, "anonymous", m.ULID.String())
		b, err := loadBlock(dir, m)
		if err != nil {
			return WindowNameCounts{}, fmt.Errorf("block %s: %w", m.ULID, err)
		}
		if st, err := os.Stat(filepath.Join(dir, "index")); err == nil {
			w.IndexBytes += st.Size()
		}
		for name, hashes := range b.SeriesByName {
			set := sets[name]
			if set == nil {
				set = map[uint64]struct{}{}
				sets[name] = set
			}
			for _, h := range hashes {
				set[h] = struct{}{}
			}
			w.SeriesRead += len(hashes)
		}
	}
	for name, set := range sets {
		w.Exact[name] = len(set)
	}
	w.ExactTime = time.Since(start)
	return w, nil
}
