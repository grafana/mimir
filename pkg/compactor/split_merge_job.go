// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type compactionStage string

const (
	stageSplit compactionStage = "split"
	stageMerge compactionStage = "merge"
)

// job holds a compaction job planned by the split merge compactor.
type job struct {
	userID string

	// Source blocks that should be compacted together when running this job.
	blocksGroup

	// The compaction stage of this job.
	stage compactionStage

	// The shard blocks in this job belong to. Its exact value depends on the stage:
	//
	// - split: identifier of the group of blocks that are going to be merged together
	// when splitting their series into multiple output blocks.
	//
	// - merge: value of the ShardIDLabelName of all blocks in this job (all blocks in
	// the job share the same label value).
	shardID string
}

func (j *job) shardingKey() string {
	return fmt.Sprintf("%s-%s-%d-%d-%s", j.userID, j.stage, j.rangeStart, j.rangeEnd, j.shardID)
}

// conflicts returns true if the two jobs cannot be planned at the same time.
func (j *job) conflicts(other *job) bool {
	// Never conflict if related to different users or if time ranges don't overlap.
	if j.userID != other.userID || !j.overlaps(other.blocksGroup) {
		return false
	}

	// Blocks with different downsample resolution or external labels (excluding the shard ID)
	// are never merged together, so they can't conflict. Since all blocks within the same job are expected to have the same
	// downsample resolution and external labels, we just check the 1st block of each job.
	if len(j.blocks) > 0 && len(other.blocks) > 0 {
		myLabels := labelsWithoutShard(j.blocksGroup.blocks[0].Thanos.Labels)
		otherLabels := labelsWithoutShard(other.blocksGroup.blocks[0].Thanos.Labels)
		if !labels.Equal(myLabels, otherLabels) {
			return false
		}
		if j.blocksGroup.blocks[0].Thanos.Downsample != other.blocksGroup.blocks[0].Thanos.Downsample {
			return false
		}
	}

	// We should merge after all splitting has been done, so two overlapping jobs
	// for different stages shouldn't coexist.
	if j.stage != other.stage {
		return true
	}

	// At this point we have two overlapping jobs for the same stage. They conflict if
	// belonging to the same shard.
	return j.shardID == other.shardID
}

func (j *job) String() string {
	blocks := make([]string, 0, len(j.blocks))
	for _, block := range j.blocks {
		minT := time.Unix(0, block.MinTime*int64(time.Millisecond)).UTC()
		maxT := time.Unix(0, block.MaxTime*int64(time.Millisecond)).UTC()
		blocks = append(blocks, fmt.Sprintf("%s (min time: %s, max time: %s)", block.ULID.String(), minT.String(), maxT.String()))
	}

	// Keep the output stable for tests.
	slices.Sort(blocks)

	return fmt.Sprintf("stage: %s, range start: %d, range end: %d, shard: %s, blocks: %s",
		j.stage, j.rangeStart, j.rangeEnd, j.shardID, strings.Join(blocks, ","))
}

// blocksGroup holds a group of blocks within the same time range.
type blocksGroup struct {
	rangeStart int64         // Included.
	rangeEnd   int64         // Excluded.
	blocks     []*block.Meta // Sorted by MinTime.
}

// overlaps returns whether the group range overlaps with the input group.
func (g blocksGroup) overlaps(other blocksGroup) bool {
	if g.rangeStart >= other.rangeEnd || other.rangeStart >= g.rangeEnd {
		return false
	}

	return true
}

func (g blocksGroup) rangeLength() int64 {
	return g.rangeEnd - g.rangeStart
}

// minTime returns the lowest MinTime across all blocks in the group.
func (g blocksGroup) minTime() int64 {
	// Blocks are expected to be sorted by MinTime.
	return g.blocks[0].MinTime
}

// maxTime returns the highest MaxTime across all blocks in the group.
func (g blocksGroup) maxTime() int64 {
	max := g.blocks[0].MaxTime

	for _, b := range g.blocks[1:] {
		if b.MaxTime > max {
			max = b.MaxTime
		}
	}

	return max
}

// maxCompactionLevel returns the highest Compaction.Level across all blocks in the group.
func (g blocksGroup) maxCompactionLevel() int {
	maxLevel := g.blocks[0].Compaction.Level

	for _, b := range g.blocks[1:] {
		if b.Compaction.Level > maxLevel {
			maxLevel = b.Compaction.Level
		}
	}

	return maxLevel
}

// getNonShardedBlocks returns the list of non-sharded blocks.
func (g blocksGroup) getNonShardedBlocks() []*block.Meta {
	var out []*block.Meta

	for _, b := range g.blocks {
		if value, ok := b.Thanos.Labels[tsdb.CompactorShardIDExternalLabel]; !ok || value == "" {
			out = append(out, b)
		}
	}

	return out
}
