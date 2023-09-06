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
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
)

func TestRewrite(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	b, err := CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "1", "b", "1"),
	}, 150, 0, 1000, labels.EmptyLabels())
	require.NoError(t, err)

	ir, err := index.NewFileReader(filepath.Join(tmpDir, b.String(), IndexFilename))
	require.NoError(t, err)

	defer func() { require.NoError(t, ir.Close()) }()

	cr, err := chunks.NewDirReader(filepath.Join(tmpDir, b.String(), "chunks"), nil)
	require.NoError(t, err)

	defer func() { require.NoError(t, cr.Close()) }()

	m := &Meta{
		BlockMeta: tsdb.BlockMeta{ULID: ULID(1)},
		Thanos:    ThanosMeta{},
	}

	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, m.ULID.String()), os.ModePerm))
	iw, err := index.NewWriter(ctx, filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	require.NoError(t, err)
	defer iw.Close()

	cw, err := chunks.NewWriter(filepath.Join(tmpDir, m.ULID.String()))
	require.NoError(t, err)

	defer cw.Close()

	tocalChunks := 0
	ignoredChunks := 0
	require.NoError(t, rewrite(log.NewNopLogger(), ir, cr, iw, cw, m, []ignoreFnType{func(mint, maxt int64, prev *chunks.Meta, curr *chunks.Meta) (bool, error) {
		tocalChunks++
		if curr.MinTime <= 600 && 600 <= curr.MaxTime {
			ignoredChunks++
			return true, nil
		}
		return false, nil
	}}))
	require.Greater(t, ignoredChunks, 0)           // Sanity check.
	require.Greater(t, tocalChunks, ignoredChunks) // Sanity check.

	require.NoError(t, iw.Close())
	require.NoError(t, cw.Close())

	ir2, err := index.NewFileReader(filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	require.NoError(t, err)

	defer func() { require.NoError(t, ir2.Close()) }()

	all, err := ir2.Postings(index.AllPostingsKey())
	require.NoError(t, err)

	rewrittenChunks := 0
	for p := ir2.SortedPostings(all); p.Next(); {
		var builder labels.ScratchBuilder
		var chks []chunks.Meta

		require.NoError(t, ir2.Series(p.At(), &builder, &chks))
		for _, chkMeta := range chks {
			require.NoError(t, err)
			require.True(t, chkMeta.MinTime > 600 || chkMeta.MaxTime < 600)
		}
		rewrittenChunks += len(chks)
	}
	require.Equal(t, tocalChunks-ignoredChunks, rewrittenChunks)
}

func ULID(i int) ulid.ULID { return ulid.MustNew(uint64(i), nil) }
