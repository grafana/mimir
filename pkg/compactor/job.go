// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

// Job holds a compaction job, which consists of a group of blocks that should be compacted together.
// Not goroutine safe.
type Job struct {
	userID         string
	key            string
	labels         labels.Labels
	resolution     int64
	metasByMinTime []*metadata.Meta
	hashFunc       metadata.HashFunc
	useSplitting   bool
	shardingKey    string

	// The number of shards to split compacted block into. Not used if splitting is disabled.
	splitNumShards uint32
}

// NewJob returns a new compaction Job.
func NewJob(userID string, key string, lset labels.Labels, resolution int64, hashFunc metadata.HashFunc, useSplitting bool, splitNumShards uint32, shardingKey string) *Job {
	return &Job{
		userID:         userID,
		key:            key,
		labels:         lset,
		resolution:     resolution,
		hashFunc:       hashFunc,
		useSplitting:   useSplitting,
		splitNumShards: splitNumShards,
		shardingKey:    shardingKey,
	}
}

// UserID returns the user/tenant to which this job belongs to.
func (job *Job) UserID() string {
	return job.userID
}

// Key returns an identifier for the job.
func (job *Job) Key() string {
	return job.key
}

// AppendMeta the block with the given meta to the job.
func (job *Job) AppendMeta(meta *metadata.Meta) error {
	if !labels.Equal(job.labels, labels.FromMap(meta.Thanos.Labels)) {
		return errors.New("block and group labels do not match")
	}
	if job.resolution != meta.Thanos.Downsample.Resolution {
		return errors.New("block and group resolution do not match")
	}

	job.metasByMinTime = append(job.metasByMinTime, meta)
	sort.Slice(job.metasByMinTime, func(i, j int) bool {
		return job.metasByMinTime[i].MinTime < job.metasByMinTime[j].MinTime
	})
	return nil
}

// IDs returns all sorted IDs of blocks in the job.
func (job *Job) IDs() (ids []ulid.ULID) {
	for _, m := range job.metasByMinTime {
		ids = append(ids, m.ULID)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i].Compare(ids[j]) < 0
	})
	return ids
}

// MinTime returns the min time across all job's blocks.
func (job *Job) MinTime() int64 {
	if len(job.metasByMinTime) > 0 {
		return job.metasByMinTime[0].MinTime
	}
	return math.MaxInt64
}

// MaxTime returns the max time across all job's blocks.
func (job *Job) MaxTime() int64 {
	max := int64(math.MinInt64)
	for _, m := range job.metasByMinTime {
		if m.MaxTime > max {
			max = m.MaxTime
		}
	}
	return max
}

// Metas returns the metadata for each block that is part of this job, ordered by the block's MinTime
func (job *Job) Metas() []*metadata.Meta {
	out := make([]*metadata.Meta, len(job.metasByMinTime))
	copy(out, job.metasByMinTime)
	return out
}

// Labels returns the external labels for the output block(s) of this job.
func (job *Job) Labels() labels.Labels {
	return job.labels
}

// Resolution returns the common downsampling resolution of blocks in the job.
func (job *Job) Resolution() int64 {
	return job.resolution
}

// UseSplitting returns whether blocks should be splitted into multiple shards when compacted.
func (job *Job) UseSplitting() bool {
	return job.useSplitting
}

// SplittingShards returns the number of output shards to build if splitting is enabled.
func (job *Job) SplittingShards() uint32 {
	return job.splitNumShards
}

// ShardingKey returns the key used to shard this job across multiple instances.
func (job *Job) ShardingKey() string {
	return job.shardingKey
}

func (job *Job) String() string {
	return fmt.Sprintf("%s (minTime: %d maxTime: %d)", job.Key(), job.MinTime(), job.MaxTime())
}
