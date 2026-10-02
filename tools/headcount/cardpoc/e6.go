// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

// E6: per-metric-name series counts are exact below a series threshold and, above it,
// a p=11 HLL sketch keeps the p95 relative error within 5%.

import (
	"sort"

	"github.com/axiomhq/hyperloglog"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/tools/headcount/model"
)

// e6Precision is the HLL precision used for values above the
// exact-hash threshold: 2^11 registers, trading some accuracy for a
// small, fixed sketch size regardless of the group's true cardinality.
const e6Precision = 11

// NameGroupResult is one metric name's outcome from RunE6.
type NameGroupResult struct {
	Name  string
	Truth int
	// Exact is a real distinct-series count for this name, always
	// computed (RunE6 needs it to measure HLL's error), regardless of
	// which representation RunE6 would actually choose to report.
	Exact int
	// Estimate is what RunE6 would report: Exact itself, if Exact is
	// below the threshold, otherwise the HLL sketch's estimate.
	Estimate     int
	UsedHLL      bool
	PayloadBytes int
}

// RelativeError returns (Estimate-Truth)/Truth, signed.
func (g NameGroupResult) RelativeError() float64 {
	if g.Truth == 0 {
		return 0
	}
	return float64(g.Estimate-g.Truth) / float64(g.Truth)
}

// E6Result is RunE6's outcome across every metric name in a snapshot.
type E6Result struct {
	Threshold int
	Groups    []NameGroupResult
}

// e6MaxP95Error is the accuracy goal for a sketch-based count: at most 5%
// absolute relative error per metric at the 95th percentile. HLL's
// standard error at p=11 is 1.04/sqrt(2^11), about 2.3%, which puts its
// p95 near 4.5%.
const e6MaxP95Error = 0.05

// Pass reports whether every group below Threshold matched its truth
// exactly, and the p95 absolute relative error among groups at or above
// it is at most e6MaxP95Error.
func (r E6Result) Pass() bool {
	var aboveErrs []float64
	for _, g := range r.Groups {
		if g.Truth < r.Threshold {
			if g.Estimate != g.Truth {
				return false
			}
			continue
		}
		e := g.RelativeError()
		if e < 0 {
			e = -e
		}
		aboveErrs = append(aboveErrs, e)
	}
	if len(aboveErrs) == 0 {
		return true
	}
	return p95(aboveErrs) <= e6MaxP95Error
}

// HLLGroups returns the groups that used HLL, largest truth first.
func (r E6Result) HLLGroups() []NameGroupResult {
	var out []NameGroupResult
	for _, g := range r.Groups {
		if g.UsedHLL {
			out = append(out, g)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Truth > out[j].Truth })
	return out
}

// HLLErrorStats returns the p95 and maximum absolute relative error over
// the groups that used HLL, or zeros if none did.
func (r E6Result) HLLErrorStats() (p95Err, maxErr float64) {
	var errs []float64
	for _, g := range r.HLLGroups() {
		e := g.RelativeError()
		if e < 0 {
			e = -e
		}
		errs = append(errs, e)
		maxErr = max(maxErr, e)
	}
	if len(errs) == 0 {
		return 0, 0
	}
	return p95(errs), maxErr
}

// p95 returns the 95th percentile of errs, using the nearest-rank method
// (no interpolation): the smallest value at or above 95% of the data.
func p95(errs []float64) float64 {
	sorted := append([]float64(nil), errs...)
	sort.Float64s(sorted)
	idx := int(0.95 * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// RunE6 checks, for every metric name in bucketDir, that a threshold-bytes
// count is exact below threshold series and stays close (via a p=11 HLL
// sketch) above it. bucketDir is expected to be a compacted snapshot,
// where a name's series can still be split across more than one block
// (one per shard, or one per day), so RunE6 unions hashes across every
// block that has the name before deciding which count to report.
func RunE6(bucketDir string, pop *model.Model, threshold int) (E6Result, error) {
	blocks, err := LoadBlocks(bucketDir)
	if err != nil {
		return E6Result{}, err
	}

	names := map[string]struct{}{}
	for _, b := range blocks {
		for name := range b.SeriesByName {
			names[name] = struct{}{}
		}
	}

	cfg := pop.Config()
	start, end := cfg.Start.UnixMilli(), cfg.End.UnixMilli()

	result := E6Result{Threshold: threshold}
	for name := range names {
		g, err := nameGroupResult(blocks, name, threshold, pop, start, end)
		if err != nil {
			return E6Result{}, err
		}
		result.Groups = append(result.Groups, g)
	}
	sort.Slice(result.Groups, func(i, j int) bool { return result.Groups[i].Name < result.Groups[j].Name })
	return result, nil
}

func nameGroupResult(blocks []Block, name string, threshold int, pop *model.Model, start, end int64) (NameGroupResult, error) {
	seen := hashesForName(blocks, name)
	exact := len(seen)

	sketch, err := hyperloglog.NewSketch(e6Precision, true)
	if err != nil {
		return NameGroupResult{}, err
	}
	for h := range seen {
		sketch.InsertHash(h)
	}

	g := NameGroupResult{Name: name, Exact: exact}
	if exact < threshold {
		g.Estimate = exact
		g.PayloadBytes = 8 * exact // one uint64 hash per series.
	} else {
		g.UsedHLL = true
		g.Estimate = int(sketch.Estimate())
		sketchBytes, err := sketch.MarshalBinary()
		if err != nil {
			return NameGroupResult{}, err
		}
		g.PayloadBytes = len(sketchBytes)
	}

	matcher := labels.MustNewMatcher(labels.MatchEqual, "__name__", name)
	g.Truth = pop.Truth([]*labels.Matcher{matcher}, start, end)
	return g, nil
}
