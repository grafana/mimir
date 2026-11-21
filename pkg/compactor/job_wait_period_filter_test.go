// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"errors"
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestInterleaveJobBlocks(t *testing.T) {
	newTestJob := func(level int, numBlocks int) *Job {
		job := newJob("user-1", "group-1", labels.EmptyLabels(), 0, true, 2, "shard-1")
		for i := range numBlocks {
			meta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(uint64(i), nil), MinTime: int64(i), Compaction: tsdb.BlockMetaCompaction{Level: level}}}
			require.NoError(t, job.AppendMeta(meta))
		}
		return job
	}

	jobs := []*Job{
		newTestJob(1, 5),
		newTestJob(1, 1),
		newTestJob(2, 3), // not a first level compaction job, so these blocks are not interleaved
		newTestJob(1, 2),
		newTestJob(1, 1),
	}

	blockOf := func(jobIdx, blockIdx int) interleavedBlock {
		return interleavedBlock{jobIdx: jobIdx, meta: jobs[jobIdx].Metas()[blockIdx]}
	}

	// First block from each job, then second blocks, and so on
	expected := []interleavedBlock{
		blockOf(0, 0), blockOf(1, 0), blockOf(3, 0), blockOf(4, 0),
		blockOf(0, 1), blockOf(3, 1),
		blockOf(0, 2),
		blockOf(0, 3),
		blockOf(0, 4),
	}
	assert.Equal(t, expected, interleaveJobBlocks(jobs))
}

func TestJobWaitPeriodElapsed(t *testing.T) {
	minutesAgo := func(n int) time.Time {
		return time.Now().Add(-time.Duration(n) * time.Minute)
	}

	type jobBlock struct {
		meta     *block.Meta
		attrs    objstore.ObjectAttributes
		attrsErr error
	}

	mockedErr := errors.New("mocked error")

	// Blocks with compaction level 1.
	meta1 := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(1, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}
	meta2 := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(2, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}

	// Blocks with compaction level 2.
	meta3 := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(3, nil), Compaction: tsdb.BlockMetaCompaction{Level: 2}}}
	meta4 := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(4, nil), Compaction: tsdb.BlockMetaCompaction{Level: 2}}}

	// OOO blocks
	oooMeta1 := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(5, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}
	oooMeta2 := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(6, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}
	oooMeta1.Compaction.SetOutOfOrder()
	oooMeta2.Compaction.SetOutOfOrder()

	// Blocks with min-/max-time
	now := time.Now()
	meta7 := &block.Meta{BlockMeta: tsdb.BlockMeta{
		ULID:       ulid.MustNew(7, nil),
		MinTime:    now.Add(-time.Hour).UnixMilli(),
		MaxTime:    now.Add(-30 * time.Minute).UnixMilli(),
		Compaction: tsdb.BlockMetaCompaction{Level: 1}},
	}
	meta8 := &block.Meta{BlockMeta: tsdb.BlockMeta{
		ULID:       ulid.MustNew(7, nil),
		MinTime:    now.Add(-time.Hour).UnixMilli(),
		MaxTime:    now.UnixMilli(),
		Compaction: tsdb.BlockMetaCompaction{Level: 1}},
	}

	// Blocks with min-/max-time and compaction level 2
	meta9 := &block.Meta{BlockMeta: tsdb.BlockMeta{
		ULID:       ulid.MustNew(7, nil),
		MinTime:    now.Add(-time.Hour).UnixMilli(),
		MaxTime:    now.Add(-30 * time.Minute).UnixMilli(),
		Compaction: tsdb.BlockMetaCompaction{Level: 2}},
	}
	meta10 := &block.Meta{BlockMeta: tsdb.BlockMeta{
		ULID:       ulid.MustNew(7, nil),
		MinTime:    now.Add(-time.Hour).UnixMilli(),
		MaxTime:    now.Add(time.Hour).UnixMilli(),
		Compaction: tsdb.BlockMetaCompaction{Level: 2}},
	}

	tests := map[string]struct {
		waitPeriod        time.Duration
		oooWaitPeriod     time.Duration
		skipFutureMaxTime bool
		jobBlocks         []jobBlock
		expectedElapsed   bool
		expectedMeta      *block.Meta
		expectedErr       error
	}{
		"wait period disabled": {
			waitPeriod:        0,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: meta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"blocks uploaded since more than the wait period": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: meta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(25)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"blocks uploaded since less than the wait period": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: meta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: false,
			expectedMeta:    meta2,
		},
		"blocks uploaded since less than the wait period but their compaction level is > 1": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta3, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(4)}},
				{meta: meta4, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"out of order block": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: oooMeta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: oooMeta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"block with max-time since more than the wait period": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: true,
			jobBlocks: []jobBlock{
				{meta: meta7, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: meta8, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: false,
			expectedMeta:    meta8,
		},
		"block with MaxTime more than wait period but LastModified old enough": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: true,
			jobBlocks: []jobBlock{
				{meta: meta8, attrs: objstore.ObjectAttributes{LastModified: time.Now().Add(-20 * time.Minute)}},
			},
			expectedElapsed: false,
			expectedMeta:    meta8,
		},
		"block with max-time since more than the wait period but skip-future-max-time disabled": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta7, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: meta8, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"block with max-time since more than the wait period but their compaction level is > 1": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: true,
			jobBlocks: []jobBlock{
				{meta: meta9, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: meta10, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"an error occurred while checking the blocks upload timestamp": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				// This block has been uploaded since more than the wait period.
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},

				// This block has been uploaded since less than the wait period, but we failed getting its attributes.
				{meta: meta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}, attrsErr: mockedErr},
			},
			expectedElapsed: false,
			expectedMeta:    meta2,
			expectedErr:     mockedErr,
		},
		"a block's meta.json was not found": {
			waitPeriod:        10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: meta2, attrsErr: bucket.ErrObjectDoesNotExist},
			},
			expectedElapsed: false,
			expectedMeta:    meta2,
			expectedErr:     bucket.ErrObjectDoesNotExist,
		},
		"ooo wait period disabled - ooo blocks not considered, period elapsed": {
			waitPeriod:        10 * time.Minute,
			oooWaitPeriod:     0,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: oooMeta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: oooMeta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"ooo block uploaded within ooo wait period, period not elapsed": {
			waitPeriod:        10 * time.Minute,
			oooWaitPeriod:     10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: oooMeta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: oooMeta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: false,
			expectedMeta:    oooMeta2,
		},
		"ooo block uploaded beyond ooo wait period, period elapsed": {
			waitPeriod:        10 * time.Minute,
			oooWaitPeriod:     10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: oooMeta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(20)}},
				{meta: oooMeta2, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(15)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
		"mixed job with ooo block not clearing threshold, period elapsed": {
			waitPeriod:        10 * time.Minute,
			oooWaitPeriod:     5 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(7)}},
				{meta: oooMeta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(7)}},
			},
			expectedElapsed: false,
			expectedMeta:    meta1,
		},
		"mixed job with inorder block not clearing threshold, period elapsed": {
			waitPeriod:        5 * time.Minute,
			oooWaitPeriod:     10 * time.Minute,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(7)}},
				{meta: oooMeta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(7)}},
			},
			expectedElapsed: false,
			expectedMeta:    oooMeta1,
		},
		"both wait periods disabled, period elapsed": {
			waitPeriod:        0,
			oooWaitPeriod:     0,
			skipFutureMaxTime: false,
			jobBlocks: []jobBlock{
				{meta: meta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
				{meta: oooMeta1, attrs: objstore.ObjectAttributes{LastModified: minutesAgo(5)}},
			},
			expectedElapsed: true,
			expectedMeta:    nil,
		},
	}

	for testName, testData := range tests {
		for _, maxConcurrency := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s, concurrency=%d", testName, maxConcurrency), func(t *testing.T) {
				job := newJob("user-1", "group-1", labels.EmptyLabels(), 0, true, 2, "shard-1")
				for _, b := range testData.jobBlocks {
					require.NoError(t, job.AppendMeta(b.meta))
				}

				userBucket := &bucket.ClientMock{}
				for _, b := range testData.jobBlocks {
					userBucket.MockAttributes(path.Join(b.meta.ULID.String(), block.MetaFilename), b.attrs, b.attrsErr)
				}

				results, err := jobsWaitPeriodElapsed(t.Context(), []*Job{job}, testData.waitPeriod, testData.oooWaitPeriod, testData.skipFutureMaxTime, userBucket, maxConcurrency, log.NewNopLogger())
				require.NoError(t, err)
				require.Len(t, results, 1)
				assert.Equal(t, testData.expectedElapsed, results[0].block == nil)
				assert.Equal(t, testData.expectedMeta, results[0].block)
				if testData.expectedErr != nil {
					assert.ErrorIs(t, results[0].err, testData.expectedErr)
				} else {
					assert.NoError(t, results[0].err)
				}
			})
		}
	}
}

func TestJobWaitPeriodFilter_Filter(t *testing.T) {
	waitPeriod := 10 * time.Minute

	elapsedMeta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(1, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}
	recentMeta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(2, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}
	failedMeta := &block.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(3, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}

	userBucket := &bucket.ClientMock{}
	userBucket.MockAttributes(path.Join(elapsedMeta.ULID.String(), block.MetaFilename), objstore.ObjectAttributes{LastModified: time.Now().Add(-2 * waitPeriod)}, nil)
	userBucket.MockAttributes(path.Join(recentMeta.ULID.String(), block.MetaFilename), objstore.ObjectAttributes{LastModified: time.Now().Add(-waitPeriod / 2)}, nil)
	userBucket.MockAttributes(path.Join(failedMeta.ULID.String(), block.MetaFilename), objstore.ObjectAttributes{}, errors.New("mocked error"))

	elapsedJob := newJob("user-1", "group-1", labels.EmptyLabels(), 0, true, 2, "shard-1")
	require.NoError(t, elapsedJob.AppendMeta(elapsedMeta))
	recentJob := newJob("user-1", "group-2", labels.EmptyLabels(), 0, true, 2, "shard-1")
	require.NoError(t, recentJob.AppendMeta(recentMeta))
	failedJob := newJob("user-1", "group-3", labels.EmptyLabels(), 0, true, 2, "shard-1")
	require.NoError(t, failedJob.AppendMeta(failedMeta))

	for _, maxConcurrency := range []int{1, 4} {
		t.Run(fmt.Sprintf("concurrency=%d", maxConcurrency), func(t *testing.T) {
			filter := newJobWaitPeriodFilter(waitPeriod, 0, false, maxConcurrency, userBucket, log.NewNopLogger())
			kept, err := filter.Filter(t.Context(), []*Job{elapsedJob, recentJob, failedJob})
			require.NoError(t, err)
			assert.Equal(t, []*Job{elapsedJob}, kept)
		})
	}
}
