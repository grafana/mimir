// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdbcodec

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/parquet"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func TestReader(t *testing.T) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	tmpDir := t.TempDir()

	var (
		// parquet reader parameters
		// all copied from pkg/parquetconverter/parquet_converter.go for now
		maxParquetIndexSizeLimit = 100 // TODO what & why is this restriction?
		rowsPerBatch             = 50000
		batchesInBuffer          = 10
	)

	var (
		// test data parameters
		mint               = 0
		sampleCount        = 100
		labelCardinalities = []int{1, 2}
	)

	// create labelsets and series input data
	labelSets := GenerateTestLabelSets(labelCardinalities, 100)
	storageSeries := GenerateTestStorageSeriesFromLabelSets(labelSets, labelCardinalities, mint, sampleCount)

	// write block to file in test temp dir
	blockFilePath, err := tsdb.CreateBlock(storageSeries, tmpDir, 0, util_log.SlogFromGoKit(logger))
	require.NoError(t, err)

	// read block file and convert to parquet rows
	parquetRowsStream, _, numRows, err := BlockToParquetRowsStream(
		ctx, blockFilePath, maxParquetIndexSizeLimit, rowsPerBatch, batchesInBuffer, logger,
	)
	require.NoError(t, err)

	// collect rows; not bothering with batching over channel to avoid reading all into memory for now
	rows := make([]parquet.ParquetRow, 0)
	for rowBatch := range parquetRowsStream {
		rows = append(rows, rowBatch...)
	}

	// assert all series counts match & all rows were read
	require.Equal(t, len(labelSets), len(rows))
	require.Equal(t, len(storageSeries), len(rows))
	require.Equal(t, numRows, len(rows))

	parquetChunksDecoder := parquet.NewPrometheusParquetChunksDecoder()
	for _, row := range rows {
		fmt.Println(row.Columns)
		chunksMeta, err := parquetChunksDecoder.Decode(row.Data, int64(mint), int64(mint+sampleCount))
		require.NoError(t, err)
		fmt.Println(chunksMeta)
	}
}
