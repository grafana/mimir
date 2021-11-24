// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/2be2db77/pkg/compact/compact_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package compactor

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/extprom"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/errutil"
)

func TestHaltError(t *testing.T) {
	err := errors.New("test")
	assert.True(t, !IsHaltError(err), "halt error")

	err = halt(errors.New("test"))
	assert.True(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(halt(errors.New("test")), "something")
	assert.True(t, IsHaltError(err), "not a halt error")

	err = errors.Wrap(errors.Wrap(halt(errors.New("test")), "something"), "something2")
	assert.True(t, IsHaltError(err), "not a halt error")
}

func TestHaltMultiError(t *testing.T) {
	haltErr := halt(errors.New("halt error"))
	nonHaltErr := errors.New("not a halt error")

	errs := errutil.MultiError{nonHaltErr}
	assert.True(t, !IsHaltError(errs.Err()), "should not be a halt error")

	errs.Add(haltErr)
	assert.True(t, IsHaltError(errs.Err()), "if any halt errors are present this should return true")
	assert.True(t, IsHaltError(errors.Wrap(errs.Err(), "wrap")), "halt error with wrap")

}

func TestRetryMultiError(t *testing.T) {
	retryErr := retry(errors.New("retry error"))
	nonRetryErr := errors.New("not a retry error")

	errs := errutil.MultiError{nonRetryErr}
	assert.True(t, !IsRetryError(errs.Err()), "should not be a retry error")

	errs = errutil.MultiError{retryErr}
	assert.True(t, IsRetryError(errs.Err()), "if all errors are retriable this should return true")

	assert.True(t, IsRetryError(errors.Wrap(errs.Err(), "wrap")), "retry error with wrap")

	errs = errutil.MultiError{nonRetryErr, retryErr}
	assert.True(t, !IsRetryError(errs.Err()), "mixed errors should return false")
}

func TestRetryError(t *testing.T) {
	err := errors.New("test")
	assert.True(t, !IsRetryError(err), "retry error")

	err = retry(errors.New("test"))
	assert.True(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.New("test")), "something")
	assert.True(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(errors.Wrap(retry(errors.New("test")), "something"), "something2")
	assert.True(t, IsRetryError(err), "not a retry error")

	err = errors.Wrap(retry(errors.Wrap(halt(errors.New("test")), "something")), "something2")
	assert.True(t, IsHaltError(err), "not a halt error. Retry should not hide halt error")
}

func TestGroupKey(t *testing.T) {
	for _, tcase := range []struct {
		input    metadata.Thanos
		expected string
	}{
		{
			input:    metadata.Thanos{},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@17241709254077376921",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{"foo": "bar", "foo1": "bar2"},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@2124638872457683483",
		},
		{
			input: metadata.Thanos{
				Labels:     map[string]string{`foo/some..thing/some.thing/../`: `a_b_c/bar-something-a\metric/a\x`},
				Downsample: metadata.ThanosDownsample{Resolution: 0},
			},
			expected: "0@16590761456214576373",
		},
	} {
		if ok := t.Run("", func(t *testing.T) {
			assert.Equal(t, tcase.expected, DefaultGroupKey(tcase.input))
		}); !ok {
			return
		}
	}
}

func TestGroupMaxMinTime(t *testing.T) {
	g := &Job{
		metasByMinTime: []*metadata.Meta{
			{BlockMeta: tsdb.BlockMeta{MinTime: 0, MaxTime: 10}},
			{BlockMeta: tsdb.BlockMeta{MinTime: 1, MaxTime: 20}},
			{BlockMeta: tsdb.BlockMeta{MinTime: 2, MaxTime: 30}},
		},
	}

	assert.Equal(t, int64(0), g.MinTime())
	assert.Equal(t, int64(30), g.MaxTime())
}

func TestFilterOwnJobs(t *testing.T) {
	jobsFn := func() []*Job {
		return []*Job{
			NewJob("user", "key1", nil, 0, metadata.NoneFunc, false, 0, ""),
			NewJob("user", "key2", nil, 0, metadata.NoneFunc, false, 0, ""),
			NewJob("user", "key3", nil, 0, metadata.NoneFunc, false, 0, ""),
			NewJob("user", "key4", nil, 0, metadata.NoneFunc, false, 0, ""),
		}
	}

	tests := map[string]struct {
		ownJob       ownCompactionJobFunc
		expectedJobs int
	}{
		"should return all planned jobs if the compactor instance owns all of them": {
			ownJob: func(job *Job) (bool, error) {
				return true, nil
			},
			expectedJobs: 4,
		},
		"should return no jobs if the compactor instance owns none of them": {
			ownJob: func(job *Job) (bool, error) {
				return false, nil
			},
			expectedJobs: 0,
		},
		"should return some jobs if the compactor instance owns some of them": {
			ownJob: func() ownCompactionJobFunc {
				count := 0
				return func(job *Job) (bool, error) {
					count++
					return count%2 == 0, nil
				}
			}(),
			expectedJobs: 2,
		},
	}

	m := NewBucketCompactorMetrics(prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewCounter(prometheus.CounterOpts{}), nil)
	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			bc, err := NewBucketCompactor(log.NewNopLogger(), nil, nil, nil, nil, "", nil, 2, false, testCase.ownJob, nil, m)
			require.NoError(t, err)

			res, err := bc.filterOwnJobs(jobsFn())

			require.NoError(t, err)
			assert.Len(t, res, testCase.expectedJobs)
		})
	}
}

func TestNoCompactionMarkFilter(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()

	block1 := ulid.MustParse("01DTVP434PA9VFXSW2JK000001") // No mark file.
	block2 := ulid.MustParse("01DTVP434PA9VFXSW2JK000002") // Marked for no-compaction
	block3 := ulid.MustParse("01DTVP434PA9VFXSW2JK000003") // Has wrong version of marker file.
	block4 := ulid.MustParse("01DTVP434PA9VFXSW2JK000004") // Has invalid marker file.
	block5 := ulid.MustParse("01DTVP434PA9VFXSW2JK000005") // No mark file.

	for name, testFn := range map[string]func(t *testing.T, synced *extprom.TxGaugeVec){
		"filter with no deletion of blocks marked for no-compaction": func(t *testing.T, synced *extprom.TxGaugeVec) {
			metas := map[ulid.ULID]*metadata.Meta{
				block1: blockMeta(block1.String(), 100, 200, nil),
				block2: blockMeta(block2.String(), 200, 300, nil), // Has no-compaction marker.
				block4: blockMeta(block4.String(), 400, 500, nil), // Marker with invalid syntax is ignored.
				block5: blockMeta(block5.String(), 500, 600, nil),
			}

			f := NewNoCompactionMarkFilter(log.NewNopLogger(), objstore.BucketWithMetrics("test", bkt, nil), 4, false)
			require.NoError(t, f.Filter(ctx, metas, synced))

			require.Contains(t, metas, block1)
			require.Contains(t, metas, block2)
			require.Contains(t, metas, block4)
			require.Contains(t, metas, block5)

			require.Len(t, f.NoCompactMarkedBlocks(), 1)
			require.Contains(t, f.NoCompactMarkedBlocks(), block2)

			synced.Submit()
			assert.Equal(t, 1.0, testutil.ToFloat64(synced.WithLabelValues(block.MarkedForNoCompactionMeta)))
		},
		"filter with deletion enabled": func(t *testing.T, synced *extprom.TxGaugeVec) {
			metas := map[ulid.ULID]*metadata.Meta{
				block1: blockMeta(block1.String(), 100, 200, nil),
				block2: blockMeta(block2.String(), 300, 300, nil), // Has no-compaction marker.
				block4: blockMeta(block4.String(), 400, 500, nil), // Marker with invalid syntax is ignored.
				block5: blockMeta(block5.String(), 500, 600, nil),
			}

			f := NewNoCompactionMarkFilter(log.NewNopLogger(), objstore.BucketWithMetrics("test", bkt, nil), 4, false)
			require.NoError(t, f.Filter(ctx, metas, synced))

			require.Contains(t, metas, block1)
			// block2 was removed from metas.
			require.Contains(t, metas, block4)
			require.Contains(t, metas, block5)

			require.Len(t, f.NoCompactMarkedBlocks(), 1)
			require.Contains(t, f.NoCompactMarkedBlocks(), block2)

			synced.Submit()
			assert.Equal(t, 1.0, testutil.ToFloat64(synced.WithLabelValues(block.MarkedForNoCompactionMeta)))
		},
		"filter with deletion enabled, but canceled context": func(t *testing.T, synced *extprom.TxGaugeVec) {
			metas := map[ulid.ULID]*metadata.Meta{
				block1: blockMeta(block1.String(), 100, 200, nil),
				block2: blockMeta(block2.String(), 200, 300, nil),
				block3: blockMeta(block3.String(), 300, 400, nil),
				block4: blockMeta(block4.String(), 400, 500, nil),
				block5: blockMeta(block5.String(), 500, 600, nil),
			}

			canceledCtx, cancel := context.WithCancel(context.Background())
			cancel()

			f := NewNoCompactionMarkFilter(log.NewNopLogger(), objstore.BucketWithMetrics("test", bkt, nil), 4, false)
			require.Error(t, f.Filter(canceledCtx, metas, synced))

			require.Contains(t, metas, block1)
			require.Contains(t, metas, block2)
			require.Contains(t, metas, block3)
			require.Contains(t, metas, block4)
			require.Contains(t, metas, block5)

			require.Empty(t, f.NoCompactMarkedBlocks())
			synced.Submit()
			assert.Equal(t, 0.0, testutil.ToFloat64(synced.WithLabelValues(block.MarkedForNoCompactionMeta)))
		},
		"filtering block with wrong marker version": func(t *testing.T, synced *extprom.TxGaugeVec) {
			metas := map[ulid.ULID]*metadata.Meta{
				block3: blockMeta(block3.String(), 300, 300, nil), // Has no-compaction marker.
			}

			f := NewNoCompactionMarkFilter(log.NewNopLogger(), objstore.BucketWithMetrics("test", bkt, nil), 4, true)
			err := f.Filter(ctx, metas, synced)

			require.Error(t, err)
			require.Contains(t, err.Error(), "unexpected no-compact-mark file version 100, expected 1")

			require.Len(t, metas, 1)
			require.Contains(t, metas, block3)

			synced.Submit()
			assert.Equal(t, 0.0, testutil.ToFloat64(synced.WithLabelValues(block.MarkedForNoCompactionMeta)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Block 2 is marked for no-compaction.
			require.NoError(t, block.MarkForNoCompact(ctx, log.NewNopLogger(), bkt, block2, metadata.OutOfOrderChunksNoCompactReason, "details...", prometheus.NewCounter(prometheus.CounterOpts{})))
			// Block 3 has marker with invalid version
			require.NoError(t, bkt.Upload(ctx, block3.String()+"/no-compact-mark.json", strings.NewReader(`{"id":"`+block3.String()+`","version":100,"details":"details","no_compact_time":1637757932,"reason":"reason"}`)))
			// Block 4 has marker with invalid JSON syntax
			require.NoError(t, bkt.Upload(ctx, block4.String()+"/no-compact-mark.json", strings.NewReader(`invalid json`)))

			synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced", Help: "Number of block metadata synced"},
				[]string{"state"}, []string{block.MarkedForNoCompactionMeta},
			)

			testFn(t, synced)
		})
	}
}
