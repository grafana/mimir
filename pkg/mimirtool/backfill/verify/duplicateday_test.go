// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
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

// blockRefAt constructs a BlockRef with a deterministic ULID derived from
// seed and a MinTime/MaxTime confined to the given UTC day index.
// day=0 -> [0, msPerDay); day=1 -> [msPerDay, 2*msPerDay); etc.
func blockRefAt(t *testing.T, seed uint64, day int64) BlockRef {
	t.Helper()
	id := ulid.MustNew(seed, nil)
	return BlockRef{
		Dir: fmt.Sprintf("/blocks/%s", id),
		Meta: block.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:    id,
				MinTime: day * msPerDay,
				MaxTime: (day + 1) * msPerDay,
				Version: 1,
			},
		},
	}
}

// verifyDuplicateDays runs the verifier and returns the failures it recorded
// along with its summary verdict.
func verifyDuplicateDays(t *testing.T, refs []BlockRef) ([]Failure, error) {
	t.Helper()
	report := newReport(len(refs))
	err := NewDuplicateDayVerifier(log.NewNopLogger()).Verify(context.Background(), refs, report)
	return report.Failures(), err
}

// verifyDuplicateDaysPass asserts the check passed and recorded nothing.
func verifyDuplicateDaysPass(t *testing.T, refs []BlockRef) {
	t.Helper()
	failures, err := verifyDuplicateDays(t, refs)
	require.NoError(t, err)
	assert.Empty(t, failures)
}

// verifyDuplicateDaysFail asserts the check reported a verdict, and returns
// the failures it recorded.
func verifyDuplicateDaysFail(t *testing.T, refs []BlockRef) []Failure {
	t.Helper()
	failures, err := verifyDuplicateDays(t, refs)
	require.Error(t, err, "collisions must be reported as a summary verdict, not only in the report")
	assert.NotEmpty(t, failures)
	return failures
}

func TestDuplicateDayVerifier_Verify(t *testing.T) {
	t.Run("empty_batch_passes", func(t *testing.T) {
		verifyDuplicateDaysPass(t, nil)
		verifyDuplicateDaysPass(t, []BlockRef{})
	})

	t.Run("single_block_passes", func(t *testing.T) {
		verifyDuplicateDaysPass(t, []BlockRef{blockRefAt(t, 1, 0)})
	})

	t.Run("distinct_days_pass", func(t *testing.T) {
		verifyDuplicateDaysPass(t, []BlockRef{blockRefAt(t, 1, 0), blockRefAt(t, 2, 1)})
	})

	t.Run("summary_verdict_is_bounded_and_counts_collisions", func(t *testing.T) {
		// Day 0: 3 blocks. Day 4: 2 blocks.
		refs := []BlockRef{
			blockRefAt(t, 1, 0), blockRefAt(t, 2, 0), blockRefAt(t, 3, 0),
			blockRefAt(t, 4, 4), blockRefAt(t, 5, 4),
		}
		_, err := verifyDuplicateDays(t, refs)
		require.EqualError(t, err, "5 block(s) share a UTC day with another block, across 2 day(s)")
		for _, r := range refs {
			assert.NotContains(t, err.Error(), r.Meta.ULID.String(),
				"the verdict must not enumerate blocks; it is logged as one line")
		}
	})

	t.Run("same_day_records_one_failure_per_block", func(t *testing.T) {
		r1 := blockRefAt(t, 1, 5)
		r2 := blockRefAt(t, 2, 5)
		failures := verifyDuplicateDaysFail(t, []BlockRef{r1, r2})

		require.Len(t, failures, 2)
		assert.Equal(t, []string{r1.Meta.ULID.String(), r2.Meta.ULID.String()}, failureULIDs(failures))
		for _, f := range failures {
			assert.Equal(t, "duplicate-day", f.Check)
			assert.Contains(t, f.Err.Error(), "covers UTC day 5")
			assert.Contains(t, f.Err.Error(), "covered by 1 other block(s)")
		}
		assert.Equal(t, r1.Dir, failures[0].BlockDir, "BlockDir should be populated for batch failures")
	})

	t.Run("failure_message_does_not_name_peers", func(t *testing.T) {
		r1 := blockRefAt(t, 1, 0)
		r2 := blockRefAt(t, 2, 0)
		for _, msg := range failureMessages(verifyDuplicateDaysFail(t, []BlockRef{r1, r2})) {
			assert.NotContains(t, msg, r1.Meta.ULID.String())
			assert.NotContains(t, msg, r2.Meta.ULID.String())
		}
	})

	t.Run("blocks_within_a_day_reported_in_ulid_order", func(t *testing.T) {
		r1 := blockRefAt(t, 10, 0)
		r2 := blockRefAt(t, 20, 0)
		r3 := blockRefAt(t, 30, 0)
		failures := verifyDuplicateDaysFail(t, []BlockRef{r3, r1, r2})

		want := []string{r1.Meta.ULID.String(), r2.Meta.ULID.String(), r3.Meta.ULID.String()}
		slices.Sort(want)
		assert.Equal(t, want, failureULIDs(failures))
		for _, msg := range failureMessages(failures) {
			assert.Contains(t, msg, "covered by 2 other block(s)")
		}
	})

	t.Run("multiple_collision_groups_day_ascending", func(t *testing.T) {
		// Day 2: 2 collisions. Day 7: 3 collisions. Day 5: 1 block (loner).
		refs := []BlockRef{
			blockRefAt(t, 100, 2), blockRefAt(t, 101, 2),
			blockRefAt(t, 200, 7), blockRefAt(t, 201, 7), blockRefAt(t, 202, 7),
			blockRefAt(t, 300, 5),
		}
		failures := verifyDuplicateDaysFail(t, refs)

		require.Len(t, failures, 5, "the single-block day must not be reported")
		assert.NotContains(t, failureULIDs(failures), ulid.MustNew(300, nil).String())

		msgs := failureMessages(failures)
		for _, msg := range msgs[:2] {
			assert.Contains(t, msg, "covers UTC day 2")
		}
		for _, msg := range msgs[2:] {
			assert.Contains(t, msg, "covers UTC day 7")
		}
	})

	t.Run("deterministic_across_input_order", func(t *testing.T) {
		refs1 := []BlockRef{
			blockRefAt(t, 1, 0), blockRefAt(t, 2, 0),
			blockRefAt(t, 3, 3), blockRefAt(t, 4, 3),
		}
		refs2 := []BlockRef{refs1[3], refs1[1], refs1[2], refs1[0]}

		assert.Equal(t, verifyDuplicateDaysFail(t, refs1), verifyDuplicateDaysFail(t, refs2),
			"recorded failures must be identical regardless of input order")
	})
}
