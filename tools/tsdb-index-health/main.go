// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/runutil"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	verifyChunks := flag.Bool("check-chunks", false, "Verify chunks in segment files.")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options...] <block-dir> [<block-dir> ...]:\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output())
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	for _, b := range flag.Args() {
		meta, err := metadata.ReadFromDir(b)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to read meta from block dir", b, "error:", err)
			continue
		}

		stats, err := GatherIndexHealthStats(logger, b, meta.MinTime, meta.MaxTime, *verifyChunks)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Failed to gather health stats from block dir", b, "error:", err)
			continue
		}

		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("  ", "  ")
		_ = enc.Encode(stats)
	}
}

type HealthStats struct {
	// TotalSeries represents total number of series in block.
	TotalSeries int64
	// OutOfOrderSeries represents number of series that have out of order chunks.
	OutOfOrderSeries int

	// OutOfOrderChunks represents number of chunks that are out of order (older time range is after younger one).
	OutOfOrderChunks int
	// DuplicatedChunks represents number of chunks with same time ranges within same series, potential duplicates.
	DuplicatedChunks int
	// OutsideChunks represents number of all chunks that are before or after time range specified in block meta.
	OutsideChunks int
	// CompleteOutsideChunks is subset of OutsideChunks that will be never accessed. They are completely out of time range specified in block meta.
	CompleteOutsideChunks int
	// Issue347OutsideChunks represents subset of OutsideChunks that are outsiders caused by https://github.com/prometheus/tsdb/issues/347
	// and is something that Thanos handle.
	//
	// Specifically we mean here chunks with minTime == block.maxTime and maxTime > block.MaxTime. These are
	// are segregated into separate counters. These chunks are safe to be deleted, since they are duplicated across 2 blocks.
	Issue347OutsideChunks int
	// OutOfOrderLabels represents the number of postings that contained out
	// of order labels, a bug present in Prometheus 2.8.0 and below.
	OutOfOrderLabels int

	// Debug Statistics.
	SeriesMinLifeDuration model.Duration
	SeriesAvgLifeDuration model.Duration
	SeriesMaxLifeDuration model.Duration

	SeriesMinLifeDurationWithoutSingleSampleSeries model.Duration
	SeriesAvgLifeDurationWithoutSingleSampleSeries model.Duration
	SeriesMaxLifeDurationWithoutSingleSampleSeries model.Duration

	SeriesMinChunks int64
	SeriesAvgChunks int64
	SeriesMaxChunks int64

	TotalChunks int64

	ChunkMinDuration model.Duration
	ChunkAvgDuration model.Duration
	ChunkMaxDuration model.Duration

	ChunkMinSize int64
	ChunkAvgSize int64
	ChunkMaxSize int64

	SingleSampleSeries int64
	SingleSampleChunks int64

	LabelNamesCount        int64
	MetricLabelValuesCount int64
}

type minMaxSumInt64 struct {
	sum int64
	min int64
	max int64

	cnt int64
}

func newMinMaxSumInt64() minMaxSumInt64 {
	return minMaxSumInt64{
		min: math.MaxInt64,
		max: math.MinInt64,
	}
}

func (n *minMaxSumInt64) Add(v int64) {
	n.cnt++
	n.sum += v
	if n.min > v {
		n.min = v
	}
	if n.max < v {
		n.max = v
	}
}

func (n *minMaxSumInt64) Avg() int64 {
	if n.cnt == 0 {
		return 0
	}
	return n.sum / n.cnt
}

func GatherIndexHealthStats(logger log.Logger, blockDir string, minTime, maxTime int64, checkChunks bool) (stats HealthStats, err error) {
	var cr *chunks.Reader
	if checkChunks {
		cr, err = chunks.NewDirReader(filepath.Join(blockDir, block.ChunksDirname), nil)
		if err != nil {
			return stats, errors.Wrap(err, "open chunks dir")
		}
		defer runutil.CloseWithErrCapture(&err, cr, "closing chunks reader")
	}

	r, err := index.NewFileReader(filepath.Join(blockDir, block.IndexFilename))
	if err != nil {
		return stats, errors.Wrap(err, "open index file")
	}
	defer runutil.CloseWithErrCapture(&err, r, "gather index issue file reader")

	p, err := r.Postings(index.AllPostingsKey())
	if err != nil {
		return stats, errors.Wrap(err, "get all postings")
	}
	var (
		lastLset labels.Labels
		lset     labels.Labels
		chks     []chunks.Meta

		seriesLifeDuration                          = newMinMaxSumInt64()
		seriesLifeDurationWithoutSingleSampleSeries = newMinMaxSumInt64()
		seriesChunks                                = newMinMaxSumInt64()
		chunkDuration                               = newMinMaxSumInt64()
		chunkSize                                   = newMinMaxSumInt64()
	)

	lnames, err := r.LabelNames()
	if err != nil {
		return stats, errors.Wrap(err, "label names")
	}
	stats.LabelNamesCount = int64(len(lnames))

	lvals, err := r.LabelValues("__name__")
	if err != nil {
		return stats, errors.Wrap(err, "metric label values")
	}
	stats.MetricLabelValuesCount = int64(len(lvals))

	// Per series.
	for p.Next() {
		lastLset = append(lastLset[:0], lset...)

		id := p.At()
		stats.TotalSeries++

		if err := r.Series(id, &lset, &chks); err != nil {
			return stats, errors.Wrap(err, "read series")
		}
		if len(lset) == 0 {
			return stats, errors.Errorf("empty label set detected for series %d", id)
		}
		if lastLset != nil && labels.Compare(lastLset, lset) >= 0 {
			return stats, errors.Errorf("series %v out of order; previous %v", lset, lastLset)
		}
		l0 := lset[0]
		for _, l := range lset[1:] {
			if l.Name < l0.Name {
				stats.OutOfOrderLabels++
				level.Warn(logger).Log("msg",
					"out-of-order label set: known bug in Prometheus 2.8.0 and below",
					"labelset", lset.String(),
					"series", fmt.Sprintf("%d", id),
				)
			}
			l0 = l
		}
		if len(chks) == 0 {
			return stats, errors.Errorf("empty chunks for series %d", id)
		}

		ooo := 0
		seriesLifeTimeMs := int64(0)
		// Per chunk in series.
		for i, c := range chks {
			stats.TotalChunks++

			chkDur := c.MaxTime - c.MinTime
			seriesLifeTimeMs += chkDur
			chunkDuration.Add(chkDur)
			if chkDur == 0 {
				stats.SingleSampleChunks++
			}

			// Approximate size.
			if i < len(chks)-2 {
				chunkSize.Add(int64(chks[i+1].Ref - c.Ref))
			}

			// Chunk vs the block ranges.
			if c.MinTime < minTime || c.MaxTime > maxTime {
				stats.OutsideChunks++
				if c.MinTime > maxTime || c.MaxTime < minTime {
					stats.CompleteOutsideChunks++
				} else if c.MinTime == maxTime {
					stats.Issue347OutsideChunks++
				}
			}

			if i == 0 {
				continue
			}

			prev := chks[i-1]

			// Chunk order within block.
			if c.MinTime > prev.MaxTime {
				continue
			}

			if c.MinTime == prev.MinTime && c.MaxTime == prev.MaxTime {
				// TODO(bplotka): Calc and check checksum from chunks itself.
				// The chunks can overlap 1:1 in time, but does not have same data.
				// We assume same data for simplicity, but it can be a symptom of error.
				stats.DuplicatedChunks++
				continue
			}

			// Chunks partly overlaps or out of order.
			level.Debug(logger).Log("msg", "found out of order chunks",
				"prev_ref", prev.Ref, "next_ref", c.Ref,
				"prev_minTime", timestamp.Time(prev.MinTime).UTC().Format(time.RFC3339Nano), "prev_maxTime", timestamp.Time(prev.MaxTime).UTC().Format(time.RFC3339Nano),
				"next_minTime", timestamp.Time(c.MinTime).UTC().Format(time.RFC3339Nano), "next_maxTime", timestamp.Time(c.MaxTime).UTC().Format(time.RFC3339Nano),
				"labels", lset, "prev_chunk_index", i-1, "next_chunk_index", i, "chunksForSeries", len(chks))

			ooo++
		}

		if ooo > 0 {
			stats.OutOfOrderSeries++
			stats.OutOfOrderChunks += ooo
		}

		seriesChunks.Add(int64(len(chks)))
		seriesLifeDuration.Add(seriesLifeTimeMs)

		if seriesLifeTimeMs == 0 {
			stats.SingleSampleSeries++
		} else {
			seriesLifeDurationWithoutSingleSampleSeries.Add(seriesLifeTimeMs)
		}

		if checkChunks {
			verifyChunks(logger, cr, lset, chks)
		}
	}
	if p.Err() != nil {
		return stats, errors.Wrap(err, "walk postings")
	}

	stats.SeriesMaxLifeDuration = model.Duration(time.Duration(seriesLifeDuration.max) * time.Millisecond)
	stats.SeriesAvgLifeDuration = model.Duration(time.Duration(seriesLifeDuration.Avg()) * time.Millisecond)
	stats.SeriesMinLifeDuration = model.Duration(time.Duration(seriesLifeDuration.min) * time.Millisecond)

	stats.SeriesMaxLifeDurationWithoutSingleSampleSeries = model.Duration(time.Duration(seriesLifeDurationWithoutSingleSampleSeries.max) * time.Millisecond)
	stats.SeriesAvgLifeDurationWithoutSingleSampleSeries = model.Duration(time.Duration(seriesLifeDurationWithoutSingleSampleSeries.Avg()) * time.Millisecond)
	stats.SeriesMinLifeDurationWithoutSingleSampleSeries = model.Duration(time.Duration(seriesLifeDurationWithoutSingleSampleSeries.min) * time.Millisecond)

	stats.SeriesMaxChunks = seriesChunks.max
	stats.SeriesAvgChunks = seriesChunks.Avg()
	stats.SeriesMinChunks = seriesChunks.min

	stats.ChunkMaxSize = chunkSize.max
	stats.ChunkAvgSize = chunkSize.Avg()
	stats.ChunkMinSize = chunkSize.min

	stats.ChunkMaxDuration = model.Duration(time.Duration(chunkDuration.max) * time.Millisecond)
	stats.ChunkAvgDuration = model.Duration(time.Duration(chunkDuration.Avg()) * time.Millisecond)
	stats.ChunkMinDuration = model.Duration(time.Duration(chunkDuration.min) * time.Millisecond)
	return stats, nil
}

func verifyChunks(l log.Logger, cr *chunks.Reader, lset labels.Labels, chks []chunks.Meta) {
	for _, cm := range chks {
		ch, err := cr.Chunk(cm)
		if err != nil {
			level.Error(l).Log("msg", "failed to read chunk", "ref", cm.Ref, "err", err)
			continue
		}

		samples := 0
		firstSample := true
		prevTs := int64(-1)

		it := ch.Iterator(nil)
		for it.Err() == nil && it.Next() {
			samples++
			ts, _ := it.At()

			if firstSample {
				firstSample = false
				if ts != cm.MinTime {
					level.Warn(l).Log("ref", cm.Ref, "msg", "timestamp of the first sample doesn't match chunk MinTime", "sampleTimestamp", formatTimestamp(ts), "chunkMinTime", formatTimestamp(cm.MinTime))
				}
			} else if ts <= prevTs {
				level.Warn(l).Log("ref", cm.Ref, "msg", "found sample with timestamp not strictly higher than previous timestamp", "previous", formatTimestamp(prevTs), "sampleTimestamp", formatTimestamp(ts))
			}

			prevTs = ts
		}

		if e := it.Err(); e != nil {
			level.Warn(l).Log("ref", cm.Ref, "msg", "failed to iterate over chunk samples", "err", err)
		} else if samples == 0 {
			level.Warn(l).Log("ref", cm.Ref, "msg", "no samples found in the chunk")
		} else if prevTs != cm.MaxTime {
			level.Warn(l).Log("ref", cm.Ref, "msg", "timestamp of the last sample doesn't match chunk MaxTime", "sampleTimestamp", formatTimestamp(prevTs), "chunkMaxTime", formatTimestamp(cm.MaxTime))
		}
	}
}

func formatTimestamp(ts int64) string {
	return fmt.Sprintf("%d (%s)", ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
}
