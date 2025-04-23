// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdbcodec

import (
	"bytes"
	"context"
	"slices"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/parquet"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func BlockToParquetRows(
	ctx context.Context,
	path string,
	maxParquetIndexSizeLimit int,
	rowsPerBatch int,
	logger log.Logger,
) ([][]parquet.ParquetRow, []string, int, error) {
	b, err := tsdb.OpenBlock(util_log.SlogFromGoKit(logger), path, nil, tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return nil, nil, 0, err
	}
	defer b.Close()

	idx, err := b.Index()
	defer idx.Close()
	if err != nil {
		return nil, nil, 0, err
	}

	cReader, err := b.Chunks()
	if err != nil {
		return nil, nil, 0, err
	}
	defer cReader.Close()

	metricNames, err := idx.LabelValues(ctx, labels.MetricName)
	if err != nil {
		return nil, nil, 0, err
	}

	k, v := index.AllPostingsKey()
	all, err := idx.Postings(ctx, k, v)
	if err != nil {
		return nil, nil, 0, err
	}

	total := 0
	for all.Next() {
		total++
	}

	labelNames, err := idx.LabelNames(ctx)
	if err != nil {
		return nil, nil, 0, err
	}

	slices.SortFunc(metricNames, func(a, b string) int {
		return bytes.Compare(
			truncateByteArray([]byte(a), maxParquetIndexSizeLimit),
			truncateByteArray([]byte(b), maxParquetIndexSizeLimit),
		)
	})

	chunksEncoder := parquet.NewPrometheusParquetChunksEncoder()
	var allRows [][]parquet.ParquetRow
	currentBatch := make([]parquet.ParquetRow, 0, rowsPerBatch)

	for _, metricName := range metricNames {
		if ctx.Err() != nil {
			return nil, nil, 0, ctx.Err()
		}

		p, err := idx.Postings(ctx, labels.MetricName, metricName)
		if err != nil {
			return nil, nil, 0, err
		}

		for p.Next() {
			var chks []chunks.Meta
			builder := labels.ScratchBuilder{}

			at := p.At()
			if err := idx.Series(at, &builder, &chks); err != nil {
				return nil, nil, 0, err
			}

			for i := range chks {
				chks[i].Chunk, _, err = cReader.ChunkOrIterable(chks[i])
				if err != nil {
					return nil, nil, 0, err
				}
			}

			data, err := chunksEncoder.Encode(chks)
			if err != nil {
				return nil, nil, 0, err
			}

			promLbls := builder.Labels()
			lbsls := make(map[string]string)
			promLbls.Range(func(l labels.Label) {
				lbsls[l.Name] = l.Value
			})

			row := parquet.ParquetRow{
				Hash:    promLbls.Hash(),
				Columns: lbsls,
				Data:    data,
			}

			currentBatch = append(currentBatch, row)
			if len(currentBatch) >= rowsPerBatch {
				allRows = append(allRows, currentBatch)
				currentBatch = make([]parquet.ParquetRow, 0, rowsPerBatch)
			}
		}
	}
	if len(currentBatch) > 0 {
		allRows = append(allRows, currentBatch)
	}

	return allRows, labelNames, total, nil
}

func truncateByteArray(value []byte, sizeLimit int) []byte {
	if len(value) > sizeLimit {
		value = value[:sizeLimit]
	}
	return value
}
