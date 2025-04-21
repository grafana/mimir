// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdbcodec

import (
	"context"
	"testing"

	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/parquet"
)

func TestReader(t *testing.T) {
	ctx := context.Background()
	logger := promslog.NewNopLogger()
	tmpDir := t.TempDir()

	var (
		// all copied from pkg/parquetconverter/parquet_converter.go for now
		maxParquetIndexSizeLimit = 100 // TODO what & why is this restriction?
		batchSize                = 50000
		batchStreamBufferSize    = 10
	)

	//labelSets := GenerateTestLabelSets([]int{1}, DefaultHistogramBuckets)
	//storageSeries := GenerateTestStorageSeriesFromLabelSets(labelSets)

	storageSeries := genSeries([]int{1, 2}, 0, 100)
	blockFilePath, err := tsdb.CreateBlock(storageSeries, tmpDir, 0, logger)
	require.NoError(t, err)

	parquetRowsStream, _, numRows, err := BlockToParquetRowsStream(
		ctx, blockFilePath, maxParquetIndexSizeLimit, batchSize, batchStreamBufferSize, logger,
	)

	rows := make([]parquet.ParquetRow, 0)
	for rowBatch := range parquetRowsStream {
		rows = append(rows, rowBatch...)
	}
	require.Equal(t, numRows, len(rows))
	//require.Equal(t, len(labelSets), len(labelNames))
}
