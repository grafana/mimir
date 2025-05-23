// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus-community/parquet-common/blob/382b6ec8ae40fb5dcdcabd8019f69a4be1cd8869/convert/writer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package convert

import (
	"context"
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/cancellation"
	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/parquet/schema"
	"github.com/grafana/mimir/pkg/parquet/storage"
)

func TestParquetWriter(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(ctx)

	totalNumberOfSeries := 1_150
	for i := 0; i != totalNumberOfSeries; i++ {
		for j := 0; j < 10; j++ {
			lbls := labels.FromStrings(labels.MetricName, fmt.Sprintf("foo_%d", i%200), "bar", fmt.Sprintf("%d", 2*i))
			_, err := app.Append(0, lbls, (time.Minute * time.Duration(j)).Milliseconds(), float64(i))
			require.NoError(t, err)
		}
	}

	require.NoError(t, app.Commit())
	h := st.Head()

	convertsOpts := DefaultConvertOpts

	// 2 row groups of size 100 per file
	convertsOpts.numRowGroups = 3
	convertsOpts.rowGroupSize = 100
	convertsOpts.writeBufferSize = 10
	convertsOpts.sortedLabels = []string{labels.MetricName, "bar"}

	rr, err := NewTsdbRowReader(ctx, h.MinTime(), h.MaxTime(), (time.Minute * 10).Milliseconds(), []Convertible{h}, convertsOpts)
	require.NoError(t, err)
	defer func() { _ = rr.Close() }()

	sw := NewShardedWrite(rr, rr.tsdbSchema, bkt, &convertsOpts)
	err = sw.Write(ctx)
	require.NoError(t, err)

	totalShards := int(math.Ceil(float64(totalNumberOfSeries) / float64(convertsOpts.numRowGroups*convertsOpts.rowGroupSize)))
	remainingRows := totalNumberOfSeries
	chunksDecoder := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	buf := make([]parquet.Row, totalNumberOfSeries)
	fSeries := make([]labels.Labels, 0, totalNumberOfSeries)
	fChunks := make([][]chunks.Meta, 0, totalNumberOfSeries)

	for i := 0; i < totalShards; i++ {
		labelsFileName := schema.LabelsPfileNameForShard(convertsOpts.name, i)
		labelsAttr, err := bkt.Attributes(ctx, labelsFileName)
		require.NoError(t, err)

		labelsFile, err := parquet.OpenFile(storage.NewBucketReadAt(ctx, labelsFileName, bkt), labelsAttr.Size)
		require.NoError(t, err)

		// Inspect row groups
		for _, group := range labelsFile.RowGroups() {
			require.LessOrEqual(t, group.NumRows(), int64(convertsOpts.rowGroupSize))
			for i, sortingCol := range convertsOpts.buildSortingColumns() {
				require.Equal(t, sortingCol.Path(), group.SortingColumns()[i].Path())
				require.Equal(t, sortingCol.Descending(), group.SortingColumns()[i].Descending())
				require.Equal(t, sortingCol.NullsFirst(), group.SortingColumns()[i].NullsFirst())
			}
		}

		lr := parquet.NewGenericReader[any](labelsFile)
		n, err := lr.ReadRows(buf)
		// Read the whole file
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, math.Min(float64(remainingRows), float64(convertsOpts.numRowGroups*convertsOpts.rowGroupSize)), float64(n))

		series, chunks, err := rowToSeries(t, labelsFile.Schema(), chunksDecoder, buf[:n])
		require.NoError(t, err)
		require.Len(t, series, n)
		require.Len(t, chunks, n)
		fSeries = append(fSeries, series...)
		// Should not have any chunk data on the labels file
		for _, chunk := range chunks {
			require.Len(t, chunk, 0)
		}

		chunksFileName := schema.ChunksPfileNameForShard(convertsOpts.name, i)
		chunksAttr, err := bkt.Attributes(ctx, chunksFileName)
		require.NoError(t, err)

		chunksFile, err := parquet.OpenFile(storage.NewBucketReadAt(ctx, chunksFileName, bkt), chunksAttr.Size)
		require.NoError(t, err)

		// should have the same number of row groups
		require.Equal(t, len(chunksFile.RowGroups()), len(chunksFile.RowGroups()))

		cr := parquet.NewGenericReader[any](chunksFile)
		n, err = cr.ReadRows(buf)
		// Read the whole file
		require.ErrorIs(t, err, io.EOF)
		require.Equal(t, math.Min(float64(remainingRows), float64(convertsOpts.numRowGroups*convertsOpts.rowGroupSize)), float64(n))

		series, chunks, err = rowToSeries(t, chunksFile.Schema(), chunksDecoder, buf[:n])
		require.NoError(t, err)
		require.Len(t, series, n)
		require.Len(t, chunks, n)
		fChunks = append(fChunks, chunks...)

		// Should not have any label
		for _, l := range series {
			require.Zero(t, l.Len())
		}

		remainingRows -= n
	}
	require.Len(t, fSeries, totalNumberOfSeries)
	require.Len(t, fChunks, totalNumberOfSeries)

	// make sure the series are sorted
	for i := 0; i < len(fSeries)-1; i++ {
		require.LessOrEqual(t, fSeries[i].Get(labels.MetricName), fSeries[i+1].Get(labels.MetricName))
		if fSeries[i].Get(labels.MetricName) == fSeries[i+1].Get(labels.MetricName) {
			require.LessOrEqual(t, fSeries[i].Get("bar"), fSeries[i+1].Get("bar"))
		}
	}
}

func Test_ShouldRespectContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cancellation.NewErrorf("test cancellation"))
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })
	s := &schema.TSDBProjection{
		Schema: parquet.NewSchema("testRow", parquet.Group{
			"testField": parquet.Leaf(parquet.FixedLenByteArrayType(32)),
		}),
	}

	sw, err := newSplitFileWriter(ctx, bkt, s.Schema, map[string]*schema.TSDBProjection{"test": s})
	require.NoError(t, err)
	require.ErrorIs(t, sw.Close(), context.Canceled)
}
