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

	"github.com/efficientgo/tools/core/pkg/testutil"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	e2eutil "github.com/grafana/mimir/pkg/storegateway/testhelper"
)

func TestRewrite(t *testing.T) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	b, err := e2eutil.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
		{{Name: "a", Value: "3"}},
		{{Name: "a", Value: "4"}},
		{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
	}, 150, 0, 1000, nil, 124, metadata.NoneFunc)
	testutil.Ok(t, err)

	ir, err := index.NewFileReader(filepath.Join(tmpDir, b.String(), IndexFilename))
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, ir.Close()) }()

	cr, err := chunks.NewDirReader(filepath.Join(tmpDir, b.String(), "chunks"), nil)
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, cr.Close()) }()

	m := &metadata.Meta{
		BlockMeta: tsdb.BlockMeta{ULID: ULID(1)},
		Thanos:    metadata.Thanos{},
	}

	testutil.Ok(t, os.MkdirAll(filepath.Join(tmpDir, m.ULID.String()), os.ModePerm))
	iw, err := index.NewWriter(ctx, filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	testutil.Ok(t, err)
	defer iw.Close()

	cw, err := chunks.NewWriter(filepath.Join(tmpDir, m.ULID.String()))
	testutil.Ok(t, err)

	defer cw.Close()

	testutil.Ok(t, rewrite(log.NewNopLogger(), ir, cr, iw, cw, m, []ignoreFnType{func(mint, maxt int64, prev *chunks.Meta, curr *chunks.Meta) (bool, error) {
		return curr.MaxTime == 696, nil
	}}))

	testutil.Ok(t, iw.Close())
	testutil.Ok(t, cw.Close())

	ir2, err := index.NewFileReader(filepath.Join(tmpDir, m.ULID.String(), IndexFilename))
	testutil.Ok(t, err)

	defer func() { testutil.Ok(t, ir2.Close()) }()

	all, err := ir2.Postings(index.AllPostingsKey())
	testutil.Ok(t, err)

	for p := ir2.SortedPostings(all); p.Next(); {
		var lset labels.Labels
		var chks []chunks.Meta

		testutil.Ok(t, ir2.Series(p.At(), &lset, &chks))
		testutil.Equals(t, 1, len(chks))
	}
}
