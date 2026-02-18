// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/index_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
)

func TestHealthStats_AnyErr(t *testing.T) {
	testCases := []struct {
		name          string
		stats         HealthStats
		expectSuccess bool
	}{
		{
			name: "critical error",
			stats: HealthStats{
				IndexFormat:           index.FormatV2,
				OutsideChunks:         1,
				CompleteOutsideChunks: 1,
			},
			expectSuccess: false,
		},
		{
			name: "invalid index version",
			stats: HealthStats{
				IndexFormat: index.FormatV1,
			},
			expectSuccess: false,
		},
		{
			name: "issue 347 outside chunks",
			stats: HealthStats{
				IndexFormat:           index.FormatV2,
				OutsideChunks:         1,
				Issue347OutsideChunks: 1,
			},
			expectSuccess: false,
		},
		{
			name: "out of order labels",
			stats: HealthStats{
				IndexFormat:      index.FormatV2,
				OutOfOrderLabels: 1,
			},
			expectSuccess: false,
		},

		{
			name: "out of order chunks",
			stats: HealthStats{
				IndexFormat:      index.FormatV2,
				TotalSeries:      100,
				OutOfOrderSeries: 50,
				OutOfOrderChunks: 10,
			},
			expectSuccess: false,
		},
		{
			name: "success",
			stats: HealthStats{
				IndexFormat: index.FormatV2,
			},
			expectSuccess: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.stats.AnyErr()
			if tc.expectSuccess {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestRewrite(t *testing.T) {
	const excludeTime int64 = 600

	ctx := context.Background()

	tmpDir := t.TempDir()

	meta, err := CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "1", "b", "1"),
	}, 150, 0, 1000, labels.EmptyLabels())
	require.NoError(t, err)

	ir, err := index.NewFileReader(filepath.Join(tmpDir, meta.ULID.String(), IndexFilename), index.DecodePostingsRaw)
	require.NoError(t, err)

	defer func() { require.NoError(t, ir.Close()) }()

	cr, err := chunks.NewDirReader(filepath.Join(tmpDir, meta.ULID.String(), "chunks"), nil)
	require.NoError(t, err)

	defer func() { require.NoError(t, cr.Close()) }()

	newMeta := &Meta{
		BlockMeta: tsdb.BlockMeta{ULID: ULID(1)},
		Thanos:    ThanosMeta{},
	}

	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, newMeta.ULID.String()), os.ModePerm))
	iw, err := index.NewWriter(ctx, filepath.Join(tmpDir, newMeta.ULID.String(), IndexFilename))
	require.NoError(t, err)
	defer iw.Close()

	cw, err := chunks.NewWriter(filepath.Join(tmpDir, newMeta.ULID.String()))
	require.NoError(t, err)

	defer cw.Close()

	totalChunks := 0
	ignoredChunks := 0
	require.NoError(t, rewrite(ctx, log.NewNopLogger(), ir, cr, iw, cw, newMeta, false, []ignoreFnType{func(_, _ int64, _ *chunks.Meta, curr *chunks.Meta) (bool, error) {
		totalChunks++
		if curr.OverlapsClosedInterval(excludeTime, excludeTime) {
			// Ignores all chunks that overlap with the excludeTime. excludeTime was randomly selected inside the block.
			ignoredChunks++
			return true, nil
		}
		return false, nil
	}}))
	require.Equal(t, uint64(5), newMeta.Stats.NumSeries)
	require.Equal(t, uint64(269), newMeta.Stats.NumSamples)
	require.Equal(t, uint64(66), newMeta.Stats.NumFloatSamples)
	require.Equal(t, uint64(203), newMeta.Stats.NumHistogramSamples)
	require.Greater(t, ignoredChunks, 0)           // Sanity check.
	require.Greater(t, totalChunks, ignoredChunks) // Sanity check.

	require.NoError(t, iw.Close())
	require.NoError(t, cw.Close())

	ir2, err := index.NewFileReader(filepath.Join(tmpDir, newMeta.ULID.String(), IndexFilename), index.DecodePostingsRaw)
	require.NoError(t, err)

	defer func() { require.NoError(t, ir2.Close()) }()

	n, v := index.AllPostingsKey()
	all, err := ir2.Postings(ctx, n, v)
	require.NoError(t, err)

	resultChunks := 0
	for p := ir2.SortedPostings(all); p.Next(); {
		var builder labels.ScratchBuilder
		var chks []chunks.Meta

		require.NoError(t, ir2.Series(p.At(), &builder, &chks))
		for _, chkMeta := range chks {
			require.NoError(t, err)
			require.True(t, chkMeta.MinTime > excludeTime || chkMeta.MaxTime < excludeTime)
		}
		resultChunks += len(chks)
	}
	require.Equal(t, totalChunks-ignoredChunks, resultChunks)
}

func ULID(i int) ulid.ULID { return ulid.MustNew(uint64(i), nil) }
