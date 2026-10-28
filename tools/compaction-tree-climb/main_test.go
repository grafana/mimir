// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestTreeClimb(t *testing.T) {
	const shardCount = 4
	ranges := []int64{
		(2 * time.Hour).Milliseconds(),
		(12 * time.Hour).Milliseconds(),
		(24 * time.Hour).Milliseconds(),
	}
	day := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	grouper := compactor.NewSplitAndMergeGrouper("user", ranges, newOverrides(shardCount, 0, 1), log.NewNopLogger())

	newMeta := func(shardID string, start time.Duration, length time.Duration, level int) *block.Meta {
		return &block.Meta{
			BlockMeta: tsdb.BlockMeta{
				ULID:       ulid.Make(),
				MinTime:    day + start.Milliseconds(),
				MaxTime:    day + (start + length).Milliseconds(),
				Compaction: tsdb.BlockMetaCompaction{Level: level},
			},
			Thanos: block.ThanosMeta{Labels: map[string]string{block.CompactorShardIDExternalLabel: shardID}},
		}
	}

	metas := map[ulid.ULID]*block.Meta{}
	noCompact := map[ulid.ULID]struct{}{}
	groups := map[string][]ulid.ULID{}

	// Two shard trees, each with failed 2h merge jobs split into halves of L2 blocks
	shardIDs := []string{"1_of_4", "3_of_4"}
	for _, shardID := range shardIDs {
		for _, failed := range []struct {
			start  time.Duration
			blocks int
			// firstHalf gets the extra block of an odd-sized job, as in the incident's groups file
			firstHalf int
		}{
			{start: 4 * time.Hour, blocks: 6, firstHalf: 3},
			{start: 6 * time.Hour, blocks: 7, firstHalf: 4},
			{start: 14 * time.Hour, blocks: 6, firstHalf: 3},
		} {
			var job []*block.Meta
			for range failed.blocks {
				m := newMeta(shardID, failed.start, 2*time.Hour, 2)
				metas[m.ULID] = m
				noCompact[m.ULID] = struct{}{}
				job = append(job, m)
			}

			// Derive the job key from the planner, as the compactor would have logged it
			jobs, err := grouper.Groups(map[ulid.ULID]*block.Meta{job[0].ULID: job[0], job[1].ULID: job[1]})
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			for i, half := range [][]*block.Meta{job[:failed.firstHalf], job[failed.firstHalf:]} {
				key := fmt.Sprintf("%s-%d", jobs[0].Key(), i+1)
				for _, m := range half {
					groups[key] = append(groups[key], m.ULID)
				}
			}
		}
	}

	// Blocks already further up the trees, which the compacted groups would merge with
	higher := []*block.Meta{
		newMeta("1_of_4", 0, 2*time.Hour, 3),
		newMeta("1_of_4", 12*time.Hour, 12*time.Hour, 4),
		newMeta("3_of_4", 18*time.Hour, 2*time.Hour, 2),
	}
	// A block in another tree, which must be left alone
	unrelated := newMeta("2_of_4", 0, 2*time.Hour, 3)
	for _, m := range append(higher, unrelated) {
		metas[m.ULID] = m
	}

	previouslyChosen := map[string]struct{}{}
	var compacted []ulid.ULID
	for iteration := 0; ; iteration++ {
		require.Less(t, iteration, 10, "climb did not finish")

		c, err := chooseGroups(metas, noCompact, groups, previouslyChosen, ranges[len(ranges)-1])
		require.NoError(t, err)
		if len(c.chosen) == 0 {
			break
		}
		require.NoError(t, verify(grouper, metas, noCompact, c))
		require.Len(t, c.chosen, len(shardIDs), "expected one group per tree")

		if iteration == 0 {
			require.Equal(t, len(higher), len(c.chosen[0].toMark)+len(c.chosen[1].toMark))

			// Without the marks, the compacted groups would merge with the blocks further up the trees
			unmarkedOnly := &groupChoice{}
			for _, g := range c.chosen {
				g := *g
				g.toMark = nil
				unmarkedOnly.chosen = append(unmarkedOnly.chosen, &g)
			}
			require.ErrorContains(t, verify(grouper, metas, noCompact, unmarkedOnly), "would be compacted again")
		}

		// Apply the mark and unmark files
		for _, g := range c.chosen {
			for _, id := range g.toMark {
				noCompact[id] = struct{}{}
			}
			for _, id := range g.blocks {
				delete(noCompact, id)
			}
			previouslyChosen[g.key] = struct{}{}
		}

		// Before compaction runs, trees with unmarked groups are skipped
		c, err = chooseGroups(metas, noCompact, groups, previouslyChosen, ranges[len(ranges)-1])
		require.NoError(t, err)
		require.Empty(t, c.chosen)
		require.Equal(t, len(shardIDs), c.numAwaitingTrees)

		// Compact each unmarked group
		jobs, err := grouper.Groups(withoutMarked(metas, noCompact))
		require.NoError(t, err)
		require.Len(t, jobs, len(shardIDs))
		for _, job := range jobs {
			out := &block.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:       ulid.Make(),
					MinTime:    job.MinTime(),
					MaxTime:    job.MaxTime(),
					Compaction: tsdb.BlockMetaCompaction{Level: 3},
				},
				Thanos: block.ThanosMeta{Labels: job.Metas()[0].Thanos.Labels},
			}
			for _, id := range job.IDs() {
				delete(metas, id)
			}
			metas[out.ULID] = out
			compacted = append(compacted, out.ULID)
		}
	}

	require.Len(t, previouslyChosen, len(groups))
	require.NotContains(t, noCompact, unrelated.ULID)
	for _, m := range higher {
		require.Contains(t, noCompact, m.ULID)
	}
	// Each half was compacted on its own, and nothing is left to compact. Every compacted block but the last
	// of each tree was marked when the tree's next group was chosen.
	require.Len(t, compacted, len(groups))
	for _, id := range compacted[:len(compacted)-len(shardIDs)] {
		require.Contains(t, noCompact, id)
	}
	jobs, err := grouper.Groups(withoutMarked(metas, noCompact))
	require.NoError(t, err)
	require.Empty(t, jobs)
}

func TestBlocksFile(t *testing.T) {
	// The file lists a <tenant>/<block ID> per line, and is never overwritten
	path := filepath.Join(t.TempDir(), "unmark")
	ids := []ulid.ULID{ulid.Make(), ulid.Make()}
	require.NoError(t, createFile(path, blocksFile("user", ids)))
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "user/"+ids[0].String()+"\nuser/"+ids[1].String()+"\n", string(content))
	require.ErrorContains(t, checkNotExists(path), "already exists")
	require.ErrorIs(t, createFile(path, nil), os.ErrExist)
}

func withoutMarked(metas map[ulid.ULID]*block.Meta, noCompact map[ulid.ULID]struct{}) map[ulid.ULID]*block.Meta {
	out := map[ulid.ULID]*block.Meta{}
	for id, m := range metas {
		if _, marked := noCompact[id]; !marked {
			out[id] = m
		}
	}
	return out
}
