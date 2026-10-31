// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"fmt"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// blockRefSpan constructs a BlockRef with a deterministic ULID derived from
// seed and an explicit [minTime, maxTime) range, in raw milliseconds.
func blockRefSpan(t *testing.T, seed uint64, minTime, maxTime int64) BlockRef {
	t.Helper()
	id := ulid.MustNew(seed, nil)
	return BlockRef{
		Dir: fmt.Sprintf("/blocks/%s", id),
		Meta: block.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    id,
				MinTime: minTime,
				MaxTime: maxTime,
				Version: 1,
			},
		},
	}
}

// verifyOverlaps runs the verifier and returns the failures it recorded along
// with its summary verdict.
func verifyOverlaps(t *testing.T, refs []BlockRef) ([]Failure, error) {
	t.Helper()
	report := newReport(len(refs))
	err := NewOverlappingBlockVerifier(log.NewNopLogger()).Verify(t.Context(), refs, report)
	return report.Failures(), err
}

// verifyOverlapsPass asserts the check passed and recorded nothing.
func verifyOverlapsPass(t *testing.T, refs []BlockRef) {
	t.Helper()
	failures, err := verifyOverlaps(t, refs)
	require.NoError(t, err)
	assert.Empty(t, failures)
}

// verifyOverlapsFail asserts the check reported a verdict, and returns the
// failures it recorded.
func verifyOverlapsFail(t *testing.T, refs []BlockRef) []Failure {
	t.Helper()
	failures, err := verifyOverlaps(t, refs)
	require.Error(t, err, "overlaps must be reported as a summary verdict, not only in the report")
	assert.NotEmpty(t, failures)
	return failures
}

func TestOverlappingBlockVerifier_Verify(t *testing.T) {
	t.Run("empty_batch_passes", func(t *testing.T) {
		verifyOverlapsPass(t, nil)
		verifyOverlapsPass(t, []BlockRef{})
	})

	t.Run("single_block_passes", func(t *testing.T) {
		verifyOverlapsPass(t, []BlockRef{blockRefSpan(t, 1, 0, 100)})
	})

	t.Run("disjoint_gapped_blocks_pass", func(t *testing.T) {
		verifyOverlapsPass(t, []BlockRef{blockRefSpan(t, 1, 0, 100), blockRefSpan(t, 2, 200, 300)})
	})

	t.Run("exactly_touching_blocks_pass", func(t *testing.T) {
		verifyOverlapsPass(t, []BlockRef{blockRefSpan(t, 1, 0, 100), blockRefSpan(t, 2, 100, 200)})
	})

	t.Run("summary_verdict_is_bounded_and_counts_regions", func(t *testing.T) {
		// Region A: 3 chained blocks. Region B: 2 blocks. Plus one loner.
		refs := []BlockRef{
			blockRefSpan(t, 1, 0, 10), blockRefSpan(t, 2, 5, 15), blockRefSpan(t, 3, 12, 20),
			blockRefSpan(t, 4, 1000, 1100), blockRefSpan(t, 5, 1050, 1150),
			blockRefSpan(t, 6, 500, 600),
		}
		_, err := verifyOverlaps(t, refs)
		require.EqualError(t, err, "5 block(s) overlap another block, across 2 region(s)")
		for _, r := range refs {
			assert.NotContains(t, err.Error(), r.Meta.ULID.String(),
				"the verdict must not enumerate blocks; it is logged as one line")
		}
	})

	t.Run("overlapping_pair_records_one_failure_per_block", func(t *testing.T) {
		r1 := blockRefSpan(t, 1, 0, 21600000)
		r2 := blockRefSpan(t, 2, 18000000, 39600000)
		failures := verifyOverlapsFail(t, []BlockRef{r2, r1})

		require.Len(t, failures, 2)
		// Reported in sorted order, so r1 (earlier MinTime) comes first.
		assert.Equal(t, []string{r1.Meta.ULID.String(), r2.Meta.ULID.String()}, failureULIDs(failures))
		for i, f := range failures {
			assert.Equal(t, "overlapping-block", f.Check)
			assert.Contains(t, f.Err.Error(), "overlaps 1 other block(s) in region [0, 39600000)",
				"failure %d should name the region bounds and peer count", i)
		}
		assert.Equal(t, r1.Dir, failures[0].BlockDir, "BlockDir should be populated for batch failures")
		assert.Contains(t, failures[0].Err.Error(), "block [minTime=0, maxTime=21600000)")
		assert.Contains(t, failures[1].Err.Error(), "block [minTime=18000000, maxTime=39600000)")
	})

	t.Run("failure_message_does_not_name_peers", func(t *testing.T) {
		r1 := blockRefSpan(t, 1, 0, 100)
		r2 := blockRefSpan(t, 2, 50, 150)

		// Naming every peer in every message would make total output
		// quadratic in the size of the overlap run.
		for _, msg := range failureMessages(verifyOverlapsFail(t, []BlockRef{r1, r2})) {
			assert.NotContains(t, msg, r1.Meta.ULID.String())
			assert.NotContains(t, msg, r2.Meta.ULID.String())
		}
	})

	t.Run("nested_range_reports_outer_bounds", func(t *testing.T) {
		outer := blockRefSpan(t, 1, 0, 86400000)
		inner := blockRefSpan(t, 2, 21600000, 43200000)
		failures := verifyOverlapsFail(t, []BlockRef{inner, outer})

		require.Len(t, failures, 2)
		for _, msg := range failureMessages(failures) {
			assert.Contains(t, msg, "region [0, 86400000)",
				"a nested block must not shrink the region's upper bound")
		}
	})

	t.Run("exact_duplicate_ranges_reported_in_ulid_order", func(t *testing.T) {
		r1 := blockRefSpan(t, 1, 0, 100)
		r2 := blockRefSpan(t, 2, 0, 100)
		failures := verifyOverlapsFail(t, []BlockRef{r2, r1})

		want := []string{r1.Meta.ULID.String(), r2.Meta.ULID.String()}
		slices.Sort(want)
		assert.Equal(t, want, failureULIDs(failures))
	})

	t.Run("transitive_chain_shares_one_region", func(t *testing.T) {
		a := blockRefSpan(t, 1, 0, 10)
		b := blockRefSpan(t, 2, 5, 15)
		c := blockRefSpan(t, 3, 12, 20)
		failures := verifyOverlapsFail(t, []BlockRef{c, a, b})

		// a and c do not overlap each other, but the chain makes one region.
		require.Len(t, failures, 3)
		for _, msg := range failureMessages(failures) {
			assert.Contains(t, msg, "region [0, 20)")
			assert.Contains(t, msg, "overlaps 2 other block(s)")
		}
	})

	t.Run("independent_regions_and_loner_excluded", func(t *testing.T) {
		regionA1 := blockRefSpan(t, 1, 0, 100)
		regionA2 := blockRefSpan(t, 2, 50, 150)
		regionB1 := blockRefSpan(t, 3, 1000, 1100)
		regionB2 := blockRefSpan(t, 4, 1050, 1150)
		loner := blockRefSpan(t, 5, 500, 600)
		failures := verifyOverlapsFail(t, []BlockRef{regionB2, loner, regionA1, regionB1, regionA2})

		// Regions are reported in ascending order of start time, and the
		// non-overlapping block is not reported at all.
		assert.Equal(t, []string{
			regionA1.Meta.ULID.String(), regionA2.Meta.ULID.String(),
			regionB1.Meta.ULID.String(), regionB2.Meta.ULID.String(),
		}, failureULIDs(failures))
		assert.NotContains(t, failureULIDs(failures), loner.Meta.ULID.String())

		msgs := failureMessages(failures)
		assert.Contains(t, msgs[0], "region [0, 150)")
		assert.Contains(t, msgs[2], "region [1000, 1150)")
	})

	t.Run("deterministic_across_input_order", func(t *testing.T) {
		refs1 := []BlockRef{
			blockRefSpan(t, 1, 0, 100),
			blockRefSpan(t, 2, 50, 150),
			blockRefSpan(t, 3, 1000, 1100),
			blockRefSpan(t, 4, 1050, 1150),
		}
		refs2 := []BlockRef{refs1[3], refs1[1], refs1[2], refs1[0]}

		assert.Equal(t, verifyOverlapsFail(t, refs1), verifyOverlapsFail(t, refs2),
			"recorded failures must be identical regardless of input order")
	})

	t.Run("same_shard_overlapping_blocks_flagged", func(t *testing.T) {
		r1 := withShard(blockRefSpan(t, 1, 0, 100), "1_of_4")
		r2 := withShard(blockRefSpan(t, 2, 50, 150), "1_of_4")
		failures, err := verifyOverlaps(t, []BlockRef{r2, r1})
		require.EqualError(t, err, "2 block(s) overlap another block, across 1 region(s)")

		require.Len(t, failures, 2)
		assert.Equal(t, []string{r1.Meta.ULID.String(), r2.Meta.ULID.String()}, failureULIDs(failures))
		for _, f := range failures {
			assert.Equal(t, "overlapping-block", f.Check)
			assert.Equal(t, "1_of_4", f.Shard)
			assert.Contains(t, f.Err.Error(), "block in shard 1_of_4")
			assert.Contains(t, f.Err.Error(), "overlaps 1 other block(s) in region [0, 150)")
		}
	})

	t.Run("different_shards_overlapping_blocks_pass", func(t *testing.T) {
		verifyOverlapsPass(t, []BlockRef{
			withShard(blockRefSpan(t, 1, 0, 100), "1_of_4"),
			withShard(blockRefSpan(t, 2, 50, 150), "2_of_4"),
		})
		verifyOverlapsPass(t, []BlockRef{
			withShard(blockRefSpan(t, 1, 0, 100), "1_of_4"),
			withShard(blockRefSpan(t, 2, 0, 100), "2_of_4"),
		})
	})

	t.Run("unsharded_and_sharded_overlapping_blocks_pass", func(t *testing.T) {
		verifyOverlapsPass(t, []BlockRef{
			blockRefSpan(t, 1, 0, 100),
			withShard(blockRefSpan(t, 2, 50, 150), "1_of_4"),
		})
	})

	t.Run("unsharded_overlap_uses_unsharded_message", func(t *testing.T) {
		failures := verifyOverlapsFail(t, []BlockRef{blockRefSpan(t, 1, 0, 100), blockRefSpan(t, 2, 50, 150)})
		require.Len(t, failures, 2)
		for _, f := range failures {
			assert.Empty(t, f.Shard)
			assert.Contains(t, f.Err.Error(), "unsharded block [minTime=")
		}
	})

	t.Run("regions_counted_per_shard", func(t *testing.T) {
		a1 := withShard(blockRefSpan(t, 1, 0, 100), "1_of_2")
		a2 := withShard(blockRefSpan(t, 2, 50, 150), "1_of_2")
		b1 := withShard(blockRefSpan(t, 3, 0, 100), "2_of_2")
		b2 := withShard(blockRefSpan(t, 4, 50, 150), "2_of_2")
		loner := blockRefSpan(t, 5, 0, 100)

		failures, err := verifyOverlaps(t, []BlockRef{b2, loner, a2, b1, a1})
		require.EqualError(t, err, "4 block(s) overlap another block, across 2 region(s)")
		assert.Equal(t, []string{
			a1.Meta.ULID.String(), a2.Meta.ULID.String(),
			b1.Meta.ULID.String(), b2.Meta.ULID.String(),
		}, failureULIDs(failures))
		assert.NotContains(t, failureULIDs(failures), loner.Meta.ULID.String())
	})

	t.Run("shards_reported_in_ascending_order_regardless_of_input_order", func(t *testing.T) {
		refs1 := []BlockRef{
			blockRefSpan(t, 1, 0, 100), blockRefSpan(t, 2, 50, 150),
			withShard(blockRefSpan(t, 3, 0, 100), "2_of_2"), withShard(blockRefSpan(t, 4, 50, 150), "2_of_2"),
			withShard(blockRefSpan(t, 5, 0, 100), "1_of_2"), withShard(blockRefSpan(t, 6, 50, 150), "1_of_2"),
		}
		refs2 := []BlockRef{refs1[5], refs1[2], refs1[1], refs1[4], refs1[3], refs1[0]}

		failures1 := verifyOverlapsFail(t, refs1)
		failures2 := verifyOverlapsFail(t, refs2)
		assert.Equal(t, failures1, failures2, "recorded failures must be identical regardless of input order")

		shards := make([]string, 0, len(failures1))
		for _, f := range failures1 {
			shards = append(shards, f.Shard)
		}
		assert.Equal(t, []string{"", "", "1_of_2", "1_of_2", "2_of_2", "2_of_2"}, shards)
	})

	t.Run("empty_or_malformed_shard_label_groups_with_unsharded", func(t *testing.T) {
		for _, label := range []string{"", "not-a-shard"} {
			failures := verifyOverlapsFail(t, []BlockRef{
				withShard(blockRefSpan(t, 1, 0, 100), label),
				blockRefSpan(t, 2, 50, 150),
			})
			require.Len(t, failures, 2, "label %q", label)
			for _, f := range failures {
				assert.Empty(t, f.Shard, "label %q", label)
			}
		}
	})

	t.Run("every_message_stays_bounded_for_a_fully_overlapping_batch", func(t *testing.T) {
		// The motivating case: a producer emitting systematically overlapping
		// blocks, so every block in the batch is at fault. Each failure must
		// stay small so the report is one tidy line per block rather than one
		// enormous string.
		const (
			n      = 1000
			stride = int64(6 * 60 * 60 * 1000)
		)
		refs := make([]BlockRef, 0, n)
		for i := range n {
			minT := int64(i) * stride
			refs = append(refs, blockRefSpan(t, uint64(i), minT, minT+2*stride))
		}

		failures, err := verifyOverlaps(t, refs)
		require.EqualError(t, err, "1000 block(s) overlap another block, across 1 region(s)",
			"the verdict must stay a fixed-size summary no matter how many blocks collide")
		require.Len(t, failures, n, "every block overlaps, so every block is reported")
		for _, msg := range failureMessages(failures) {
			assert.Less(t, len(msg), 150, "per-block message must not grow with batch size")
		}
	})
}
