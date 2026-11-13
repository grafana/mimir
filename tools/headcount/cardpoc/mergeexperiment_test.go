// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/tools/headcount/cardgen"
)

func TestRunE3_OnGeneratedL1Blocks(t *testing.T) {
	pop := testPopulation(t)
	bucket := t.TempDir()
	require.NoError(t, os.MkdirAll(bucket+"/anonymous", 0o755))
	_, err := cardgen.Generate(pop, bucket+"/anonymous", cardgen.Config{Partitions: 3, Seed: 1})
	require.NoError(t, err)

	res, err := RunE3(bucket, pop)
	require.NoError(t, err)

	require.True(t, res.Pass(), "hash union must be exact: got %d, truth %d", res.HashUnion, res.Truth)
	require.Greater(t, res.SumAll, res.Truth, "the naive sum must overcount on level-1 blocks")
}

// The following exercise E2Result/E4Result's pass logic directly against
// hand-built Block fixtures, without a live compactor: they check that
// the *rule* each experiment applies is the right one, independently of
// whether a real compaction run produces blocks that satisfy it.

func TestE2Result_Pass(t *testing.T) {
	require.True(t, E2Result{MergeExperimentResult{Truth: 10, HashUnion: 10, SumAll: 40}}.Pass(),
		"E2 must pass on hash union alone, regardless of how badly the naive sum overcounts")
	require.False(t, E2Result{MergeExperimentResult{Truth: 10, HashUnion: 9}}.Pass())
}

func TestE4Result_Pass(t *testing.T) {
	require.True(t, E4Result{MergeExperimentResult{Truth: 10, HashUnion: 10, SumSkipSources: 10}}.Pass())
	require.True(t, E4Result{MergeExperimentResult{Truth: 10, HashUnion: 10, SumSkipSources: 14}}.Pass(),
		"a source-dedup sum that still overcounts (cross-block-range duplication, not a duplicate-block problem) must not fail E4: only hash union is required to be exact")
	require.False(t, E4Result{MergeExperimentResult{Truth: 10, HashUnion: 9}}.Pass())
}

func TestMergeExperimentResult_RelativeError(t *testing.T) {
	r := MergeExperimentResult{Truth: 100}
	require.InDelta(t, 0.1, r.RelativeError(110), 1e-9)
	require.InDelta(t, -0.1, r.RelativeError(90), 1e-9)
}
