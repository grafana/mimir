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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	e2eutil "github.com/grafana/mimir/pkg/storegateway/testhelper"
)

func TestRewrite(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	b, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
		labels.FromStrings("a", "4"),
		labels.FromStrings("a", "1", "b", "1"),
	}, 150, 0, 1000, labels.EmptyLabels(), 124, metadata.NoneFunc)
	require.NoError(t, err)

	ir, err := index.NewFileReader(filepath.Join(tmpDir, b.String(), IndexFilename))
	require.NoError(t, err)

	defer func() { require.NoError(t, ir.Close()) }()

	cr, err := chunks.NewDirReader(filepath.Join(tmpDir, b.String(), "chunks"), nil)
	require.NoError(t, err)

	defer func() { require.NoError(t, cr.Close()) }()

	m := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: ULID(1)},
		Thanos:    metadata.Thanos{},
	}

	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, m.ULID.String()), os.ModePerm))
	iw, err := index.NewWriter(ctx, filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	require.NoError(t, err)
	defer iw.Close()

	cw, err := chunks.NewWriter(filepath.Join(tmpDir, m.ULID.String()))
	require.NoError(t, err)

	defer cw.Close()

	require.NoError(t, rewrite(log.NewNopLogger(), ir, cr, iw, cw, m, []ignoreFnType{func(mint, maxt int64, prev *chunks.Meta, curr *chunks.Meta) (bool, error) {
		return curr.MaxTime == 696, nil
	}}))

	require.NoError(t, iw.Close())
	require.NoError(t, cw.Close())

	ir2, err := index.NewFileReader(filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	require.NoError(t, err)

	defer func() { require.NoError(t, ir2.Close()) }()

	all, err := ir2.Postings(index.AllPostingsKey())
	require.NoError(t, err)

	for p := ir2.SortedPostings(all); p.Next(); {
		var lset labels.Labels
		var chks []chunks.Meta

		require.NoError(t, ir2.Series(p.At(), &lset, &chks))
		require.Equal(t, 1, len(chks))
	}
}
