// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import "github.com/grafana/mimir/tools/headcount/model"

// MergeExperimentResult is every merge mode's whole-tenant series count
// against the same population's ground truth, for one snapshot.
type MergeExperimentResult struct {
	Truth          int
	SumAll         int
	SumSkipSources int
	SumMinLevel2   int
	HashUnion      int
}

// RelativeError returns (count-Truth)/Truth, signed: positive is an
// overcount, negative an undercount.
func (r MergeExperimentResult) RelativeError(count int) float64 {
	if r.Truth == 0 {
		return 0
	}
	return float64(count-r.Truth) / float64(r.Truth)
}

func runMergeExperiment(bucketDir string, pop *model.Model) (MergeExperimentResult, error) {
	blocks, err := LoadBlocks(bucketDir)
	if err != nil {
		return MergeExperimentResult{}, err
	}
	cfg := pop.Config()
	truth := pop.Truth(nil, cfg.Start.UnixMilli(), cfg.End.UnixMilli())

	return MergeExperimentResult{
		Truth:          truth,
		SumAll:         SumAll(blocks),
		SumSkipSources: SumSkipSources(blocks),
		SumMinLevel2:   SumMinLevel(blocks, 2),
		HashUnion:      HashUnion(blocks),
	}, nil
}

// E2Result is RunE2's outcome: on a compacted snapshot, a naive sum
// overcounts a long-lived series once per day-block it appears in, and
// only the exact hash union is expected to be correct.
type E2Result struct{ MergeExperimentResult }

// Pass reports whether hash union recovered the exact count.
func (r E2Result) Pass() bool { return r.HashUnion == r.Truth }

// RunE2 runs the merge experiment against a compacted snapshot.
func RunE2(compactedDir string, pop *model.Model) (E2Result, error) {
	res, err := runMergeExperiment(compactedDir, pop)
	return E2Result{res}, err
}

// E3Result is RunE3's outcome on the level-1 snapshot. Unlike E2 and E4,
// none of sources-skip or level-2-and-above is expected to fix the
// overcount here: level-1 blocks have no compaction history to skip and
// no level-2 blocks to fall back to, so only hash union is exact.
type E3Result struct{ MergeExperimentResult }

func (r E3Result) Pass() bool { return r.HashUnion == r.Truth }

// RunE3 runs the merge experiment against the level-1 snapshot.
func RunE3(l1Dir string, pop *model.Model) (E3Result, error) {
	res, err := runMergeExperiment(l1Dir, pop)
	return E3Result{res}, err
}

// E4Result is RunE4's outcome on a handover snapshot: the level-1
// sources a compaction has already superseded are still on disk, so a
// naive sum double-counts them, but skipping blocks listed in another
// visible block's Compaction.Sources is expected to recover the exact
// count here (unlike on the level-1 snapshot, where that link doesn't
// exist yet).
type E4Result struct{ MergeExperimentResult }

func (r E4Result) Pass() bool { return r.HashUnion == r.Truth && r.SumSkipSources == r.Truth }

// RunE4 runs the merge experiment against a handover snapshot.
func RunE4(handoverDir string, pop *model.Model) (E4Result, error) {
	res, err := runMergeExperiment(handoverDir, pop)
	return E4Result{res}, err
}
