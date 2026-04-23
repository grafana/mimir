// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"slices"
	"strings"
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
		Dir: "",
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

func TestDuplicateDayVerifier_Verify(t *testing.T) {
	t.Run("empty_batch_returns_nil", func(t *testing.T) {
		v := NewDuplicateDayVerifier(log.NewNopLogger())
		require.NoError(t, v.Verify(context.Background(), nil))
		require.NoError(t, v.Verify(context.Background(), []BlockRef{}))
	})

	t.Run("single_block_returns_nil", func(t *testing.T) {
		v := NewDuplicateDayVerifier(log.NewNopLogger())
		require.NoError(t, v.Verify(context.Background(), []BlockRef{blockRefAt(t, 1, 0)}))
	})

	t.Run("two_distinct_days_returns_nil", func(t *testing.T) {
		v := NewDuplicateDayVerifier(log.NewNopLogger())
		refs := []BlockRef{blockRefAt(t, 1, 0), blockRefAt(t, 2, 1)}
		require.NoError(t, v.Verify(context.Background(), refs))
	})

	t.Run("two_blocks_same_day_returns_error", func(t *testing.T) {
		v := NewDuplicateDayVerifier(log.NewNopLogger())
		r1 := blockRefAt(t, 1, 5)
		r2 := blockRefAt(t, 2, 5)
		err := v.Verify(context.Background(), []BlockRef{r1, r2})
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "same UTC day")
		assert.Contains(t, msg, r1.Meta.ULID.String())
		assert.Contains(t, msg, r2.Meta.ULID.String())
		assert.Contains(t, msg, "day 5")
	})

	t.Run("three_blocks_same_day_returns_error_lex_order", func(t *testing.T) {
		v := NewDuplicateDayVerifier(log.NewNopLogger())
		r1 := blockRefAt(t, 10, 0)
		r2 := blockRefAt(t, 20, 0)
		r3 := blockRefAt(t, 30, 0)
		err := v.Verify(context.Background(), []BlockRef{r3, r1, r2})
		require.Error(t, err)
		msg := err.Error()
		sorted := []string{r1.Meta.ULID.String(), r2.Meta.ULID.String(), r3.Meta.ULID.String()}
		slices.Sort(sorted)
		for i := 0; i < len(sorted)-1; i++ {
			assert.Less(t, strings.Index(msg, sorted[i]), strings.Index(msg, sorted[i+1]),
				"ULIDs must appear in lexicographic order in the error message")
		}
	})

	t.Run("multiple_collision_groups_day_ascending", func(t *testing.T) {
		v := NewDuplicateDayVerifier(log.NewNopLogger())
		// Day 2: 2 collisions. Day 7: 3 collisions. Day 5: 1 block (loner).
		refs := []BlockRef{
			blockRefAt(t, 100, 2), blockRefAt(t, 101, 2),
			blockRefAt(t, 200, 7), blockRefAt(t, 201, 7), blockRefAt(t, 202, 7),
			blockRefAt(t, 300, 5),
		}
		err := v.Verify(context.Background(), refs)
		require.Error(t, err)
		msg := err.Error()
		assert.Contains(t, msg, "day 2")
		assert.Contains(t, msg, "day 7")
		assert.NotContains(t, msg, "day 5", "single-block day must not appear in collision report")
		assert.Less(t, strings.Index(msg, "day 2"), strings.Index(msg, "day 7"),
			"colliding days must be enumerated in ascending order")
		assert.Contains(t, msg, ulid.MustNew(100, nil).String())
		assert.Contains(t, msg, ulid.MustNew(101, nil).String())
		assert.Contains(t, msg, ulid.MustNew(200, nil).String())
		assert.Contains(t, msg, ulid.MustNew(201, nil).String())
		assert.Contains(t, msg, ulid.MustNew(202, nil).String())
		assert.NotContains(t, msg, ulid.MustNew(300, nil).String())
	})

	t.Run("deterministic_across_input_order", func(t *testing.T) {
		v := NewDuplicateDayVerifier(log.NewNopLogger())
		refs1 := []BlockRef{
			blockRefAt(t, 1, 0), blockRefAt(t, 2, 0),
			blockRefAt(t, 3, 3), blockRefAt(t, 4, 3),
		}
		refs2 := []BlockRef{refs1[3], refs1[1], refs1[2], refs1[0]}
		err1 := v.Verify(context.Background(), refs1)
		err2 := v.Verify(context.Background(), refs2)
		require.Error(t, err1)
		require.Error(t, err2)
		assert.Equal(t, err1.Error(), err2.Error(),
			"error string must be identical regardless of input order")
	})
}
