// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/backfill/backfill.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

// this is adapted from  https://github.com/prometheus/prometheus/blob/2f54aa060484a9a221eb227e1fb917ae66051c76/cmd/promtool/backfill.go#L68-L171
func CreateBlock(input storage.SeriesSet, outputDir string, blockDuration time.Duration) (blockID ulid.ULID, returnErr error) {
	blockWriter, err := tsdb.NewBlockWriter(promslog.NewNopLogger(), outputDir, blockDuration.Milliseconds()*2) // Multiply by 2 so that we can append samples anywhere in the original time window.
	if err != nil {
		return ulid.Zero, errors.Wrap(err, "create block writer")
	}

	defer func() {
		mErr := tsdb_errors.NewMulti()
		mErr.Add(returnErr)
		mErr.Add(blockWriter.Close())
		returnErr = mErr.Err()
	}()

	ctx := context.Background()
	var it chunkenc.Iterator

	for input.Next() {
		if err := input.Err(); err != nil {
			return ulid.Zero, errors.Wrap(err, "read next series")
		}

		series := input.At()
		it = series.Iterator(it)
		app := blockWriter.Appender(ctx)

		var seriesRef storage.SeriesRef
		wroteAny := false

		for {
			valueType := it.Next()
			if valueType == chunkenc.ValNone {
				break
			}

			var err error

			switch valueType {
			case chunkenc.ValFloat:
				t, v := it.At()
				seriesRef, err = app.Append(seriesRef, series.Labels(), t, v)
			case chunkenc.ValFloatHistogram:
				t, h := it.AtFloatHistogram(nil)
				seriesRef, err = app.AppendHistogram(seriesRef, series.Labels(), t, nil, h)
			case chunkenc.ValHistogram:
				t, h := it.AtHistogram(nil)
				seriesRef, err = app.AppendHistogram(seriesRef, series.Labels(), t, h, nil)
			default:
				return ulid.Zero, fmt.Errorf("unexpected value type: %v", valueType)
			}

			if err != nil {
				return ulid.Zero, errors.Wrap(err, "append sample")
			}

			wroteAny = true
		}

		if err := it.Err(); err != nil {
			return ulid.Zero, errors.Wrap(err, "read series data")
		}

		if !wroteAny {
			continue
		}

		if err := app.Commit(); err != nil {
			return ulid.Zero, errors.Wrap(err, "commit")
		}
	}

	blockID, err = blockWriter.Flush(ctx)
	if err != nil {
		return ulid.Zero, errors.Wrap(err, "flush")
	}

	return blockID, nil
}
