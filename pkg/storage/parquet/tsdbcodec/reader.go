// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/26344c3ec7409713fcf52a9c41cd0dce537b3100/pkg/compactor/parquet_compactor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdbcodec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/parquet"
)

type TSDBBlockToParquetReader struct {
	ctx context.Context

	closers       []io.Closer
	chunksEncoder *PrometheusParquetChunksEncoder

	seriesCount int
	metricNames []string
	labelNames  []string
}

func NewTSDBBlockToParquetReader(
	ctx context.Context,
	blockReader tsdb.BlockReader,
) (*TSDBBlockToParquetReader, error) {
	closers := make([]io.Closer, 0, 2) // index and chunks readers

	indexReader, err := blockReader.Index()
	if err != nil {
		return nil, fmt.Errorf("unable to get index reader from block: %s", err)
	}
	closers = append(closers, indexReader)

	chunkReader, err := blockReader.Chunks()
	if err != nil {
		return nil, fmt.Errorf("unable to get chunk reader from block: %s", err)
	}
	closers = append(closers, chunkReader)

	metricNames, err := indexReader.LabelValues(ctx, labels.MetricName)
	if err != nil {
		return nil, fmt.Errorf("unable to get metric names from block: %s", err)
	}
	k, v := index.AllPostingsKey()
	all, err := indexReader.Postings(ctx, k, v)
	if err != nil {
		return nil, fmt.Errorf("unable to get all postings from block: %s", err)
	}

	seriesCount := 0
	for all.Next() {
		seriesCount++
	}

	labelNames, err := indexReader.LabelNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get label names from block: %s", err)
	}

	return &TSDBBlockToParquetReader{
		ctx:           ctx,
		closers:       closers,
		chunksEncoder: NewPrometheusParquetChunksEncoder(),
		seriesCount:   seriesCount,
		metricNames:   metricNames,
		labelNames:    labelNames,
	}, nil
}

//func (br *TSDBBlockToParquetReader) Close() error {
//	err := &multierror.Error{}
//	for i := range br.closers {
//		err = multierror.Append(err, br.closers[i].Close())
//	}
//	return err.ErrorOrNil()
//}
//
//func (br *TSDBBlockToParquetReader) RowsStream(
//	ctx context.Context,
//	rowsPerBatch int,
//	maxBatchesInBuffer int,
//) (rowsBatchChan chan []parquet.ParquetRow, errChan chan error) {
//
//	rowsBatchChan = make(chan []parquet.ParquetRow, maxBatchesInBuffer)
//	defer close(rowsBatchChan)
//	errChan = make(chan error, 1)
//	defer close(errChan)
//
//	indexReaderMu := &sync.Mutex{}
//
//	for _, metricName := range br.metricNames {
//		if ctxErr := ctx.Err(); ctxErr != nil {
//			errChan <- ctxErr
//			return
//		}
//		indexReaderMu.Lock()
//		postings, err := br.indexReader.Postings(ctx, labels.MetricName, metricName)
//		if err != nil {
//			return
//		}
//		errGrp := &errgroup.Group{}
//	}
//
//	return rowsBatchChan, errChan
//}

func BlockToParquetRowsStream(
	ctx context.Context,
	path string,
	maxParquetIndexSizeLimit int, // TODO what does this mean and how to set it?
	rowsPerBatch int,
	batchStreamBufferSize int,
	logger log.Logger,
) (chan []parquet.ParquetRow, []string, int, error) {
	// TODO
	b, err := tsdb.OpenBlock( /*logutil.GoKitLogToSlog(logger)*/ slog.New(slog.DiscardHandler), path, nil, tsdb.DefaultPostingsDecoderFactory)
	if err != nil {
		return nil, nil, 0, err
	}
	idx, err := b.Index()
	if err != nil {
		return nil, nil, 0, err
	}

	cReader, err := b.Chunks()
	if err != nil {
		return nil, nil, 0, err
	}

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
	rc := make(chan []parquet.ParquetRow, batchStreamBufferSize)
	chunksEncoder := NewPrometheusParquetChunksEncoder()

	go func() {
		defer b.Close()
		defer cReader.Close()
		defer idx.Close()
		defer close(rc)

		batch := make([]parquet.ParquetRow, 0, rowsPerBatch)
		batchMutex := &sync.Mutex{}
		postingsMutex := &sync.Mutex{}

		for _, metricName := range metricNames {
			if ctx.Err() != nil {
				return
			}
			p, err := idx.Postings(ctx, labels.MetricName, metricName)
			if err != nil {
				return
			}
			eg := &errgroup.Group{}
			eg.SetLimit(runtime.GOMAXPROCS(0))

			for {
				postingsMutex.Lock()
				done := !p.Next()
				postingsMutex.Unlock()
				if done {
					break
				}

				eg.Go(func() error {
					chks := []chunks.Meta{}
					builder := labels.ScratchBuilder{}

					postingsMutex.Lock()
					at := p.At()
					postingsMutex.Unlock()

					err := idx.Series(at, &builder, &chks)
					if err != nil {
						return err
					}

					for i := range chks {
						chks[i].Chunk, _, err = cReader.ChunkOrIterable(chks[i])
						if err != nil {
							return err
						}
					}

					data, err := chunksEncoder.Encode(chks)
					if err != nil {
						return err
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

					batchMutex.Lock()
					batch = append(batch, row)
					if len(batch) >= rowsPerBatch {
						rc <- batch
						batch = make([]parquet.ParquetRow, 0, rowsPerBatch)
					}
					batchMutex.Unlock()
					return nil
				})
			}

			err = eg.Wait()
			if err != nil {
				level.Error(logger).Log("msg", "failed to process chunk", "err", err)
				return
			}
		}
		if len(batch) > 0 {
			rc <- batch
		}
	}()

	return rc, labelNames, total, nil
}

func truncateByteArray(value []byte, sizeLimit int) []byte {
	if len(value) > sizeLimit {
		value = value[:sizeLimit]
	}
	return value
}
