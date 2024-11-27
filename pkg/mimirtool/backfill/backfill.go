// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/backfill/backfill.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package backfill

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/units"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

// copied from https://github.com/prometheus/prometheus/blob/2f54aa060484a9a221eb227e1fb917ae66051c76/cmd/promtool/tsdb.go#L361-L398

func printBlocks(blocks []tsdb.BlockReader, writeHeader, humanReadable bool, output io.Writer) {
	tw := tabwriter.NewWriter(output, 13, 0, 2, ' ', 0)
	defer tw.Flush()

	if writeHeader {
		fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tDURATION\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES\tSIZE")
	}

	for _, b := range blocks {
		meta := b.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			getFormattedTime(meta.MinTime, humanReadable),
			getFormattedTime(meta.MaxTime, humanReadable),
			time.Duration(meta.MaxTime-meta.MinTime)*time.Millisecond,
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
			getFormattedBytes(b.Size(), humanReadable),
		)
	}
}

func getFormattedTime(timestamp int64, humanReadable bool) string {
	if humanReadable {
		return time.Unix(timestamp/1000, 0).UTC().String()
	}
	return strconv.FormatInt(timestamp, 10)
}

func getFormattedBytes(bytes int64, humanReadable bool) string {
	if humanReadable {
		return units.Base2Bytes(bytes).String()
	}
	return strconv.FormatInt(bytes, 10)
}

type Iterator interface {
	Next() error
	Sample() (ts int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram)
	Labels() (l labels.Labels)
}

type IteratorCreator func() Iterator

// this is adapted from  https://github.com/prometheus/prometheus/blob/2f54aa060484a9a221eb227e1fb917ae66051c76/cmd/promtool/backfill.go#L68-L171
func CreateBlocks(input IteratorCreator, mint, maxt int64, maxSamplesInAppender int, outputDir string, humanReadable bool, output io.Writer) (returnErr error) {
	blockDuration := tsdb.DefaultBlockDuration
	mint = blockDuration * (mint / blockDuration)

	db, err := tsdb.OpenDBReadOnly(outputDir, "", nil)
	if err != nil {
		return err
	}
	defer func() {
		mErr := tsdb_errors.NewMulti()
		mErr.Add(returnErr)
		mErr.Add(db.Close())
		returnErr = mErr.Err()
	}()

	var wroteHeader bool

	for t := mint; t <= maxt; t = t + blockDuration/2 {
		err := func() error {
			w, err := tsdb.NewBlockWriter(promslog.NewNopLogger(), outputDir, blockDuration)
			if err != nil {
				return errors.Wrap(err, "block writer")
			}
			defer func() {
				mErr := tsdb_errors.NewMulti()
				mErr.Add(err)
				mErr.Add(w.Close())
				err = mErr.Err()
			}()

			ctx := context.Background()
			app := w.Appender(ctx)
			i := input()
			tsUpper := t + blockDuration/2
			samplesCount := 0
			for {
				err := i.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					return errors.Wrap(err, "input")
				}

				ts, v, h, fh := i.Sample()
				if ts < t || ts >= tsUpper {
					continue
				}

				l := i.Labels()
				if h != nil || fh != nil {
					_, err = app.AppendHistogram(0, i.Labels(), ts, h, fh)
				} else {
					_, err = app.Append(0, i.Labels(), ts, v)
				}
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("add sample for metric=%s ts=%s value=%f", l, model.Time(ts).Time().Format(time.RFC3339Nano), v))
				}

				samplesCount++
				if samplesCount < maxSamplesInAppender {
					continue
				}

				// If we arrive here, the samples count is greater than the maxSamplesInAppender.
				// Therefore the old appender is committed and a new one is created.
				// This prevents keeping too many samples lined up in an appender and thus in RAM.
				if err := app.Commit(); err != nil {
					return errors.Wrap(err, "commit")
				}

				app = w.Appender(ctx)
				samplesCount = 0
			}

			if err := app.Commit(); err != nil {
				return errors.Wrap(err, "commit")
			}

			block, err := w.Flush(ctx)
			if err != nil && !strings.Contains(err.Error(), "no series appended, aborting") {
				return errors.Wrap(err, "flush")
			}

			blocks, err := db.Blocks()
			if err != nil {
				return errors.Wrap(err, "get blocks")
			}
			for _, b := range blocks {
				if b.Meta().ULID == block {
					printBlocks([]tsdb.BlockReader{b}, !wroteHeader, humanReadable, output)
					wroteHeader = true
					break
				}
			}

			return nil
		}()

		if err != nil {
			return errors.Wrap(err, "process blocks")
		}
	}
	return nil

}
