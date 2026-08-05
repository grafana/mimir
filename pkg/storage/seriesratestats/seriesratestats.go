// SPDX-License-Identifier: AGPL-3.0-only

// Package seriesratestats implements the per-block series sample rate statistics
// sidecar (block.SeriesRateStatsFilename). For every block it records a compact
// summary of the block's per-series sample rate distribution, plus one entry per
// series whose rate stands out from that block's own distribution: a series is
// stored individually only when its rate clears max(FloorRate, SignificanceFactor
// times the block's median rate). Blocks whose series all ingest at a similar rate
// therefore collapse to just the summary, while blocks with a skewed distribution
// keep their outlier tail.
//
// The statistics are collected through the tsdb.SeriesStatsObserver hook while the
// block is written, so per-series sample counts are exact and no block read-back
// is needed. Generation is best-effort: a failure to write the sidecar is logged
// and never fails the block write.
package seriesratestats

import (
	"container/heap"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// Version is the current version of the sidecar format.
const Version = 1

// Config holds the tunables of the adaptive significance selection.
type Config struct {
	// SignificanceFactor is the prominence factor k: a series is stored individually
	// when its rate is at least k times the block's median series rate.
	SignificanceFactor float64

	// FloorRate is the absolute floor F in samples/s: a series below it is never
	// stored individually, regardless of how it compares to the block's median.
	FloorRate float64

	// MaxSeries is the hard cap M on the number of per-series entries per block.
	MaxSeries int

	// MinSpan is the floor applied to a series' data span when computing its rate,
	// so that series with very few samples in a tiny span don't produce absurd rates.
	MinSpan time.Duration

	// ReservoirSize is the size of the rate sample reservoir used to estimate the
	// block's rate quantiles (including the median the significance bar derives from).
	ReservoirSize int
}

// DefaultConfig returns the default selection tunables.
func DefaultConfig() Config {
	return Config{
		SignificanceFactor: 2.0,
		FloorRate:          0.25,
		MaxSeries:          10000,
		MinSpan:            time.Minute,
		ReservoirSize:      4096,
	}
}

// Stats is the content of the series rate stats sidecar file.
type Stats struct {
	Version int    `json:"version"`
	BlockID string `json:"block_id"`
	MinTime int64  `json:"min_time"`
	MaxTime int64  `json:"max_time"`

	Summary Summary `json:"summary"`

	// Series holds the block's outlier series, sorted by decreasing rate.
	Series []SeriesEntry `json:"series"`
}

// Summary describes the block's per-series rate distribution. It is always
// present, also when no series cleared the significance bar.
type Summary struct {
	NumSeries  uint64 `json:"num_series"`
	NumSamples uint64 `json:"num_samples"`

	// Rate quantiles in samples/s, estimated from a reservoir sample.
	MedianRate float64 `json:"median_rate"`
	P90Rate    float64 `json:"p90_rate"`
	P99Rate    float64 `json:"p99_rate"`
	MaxRate    float64 `json:"max_rate"`

	// SignificanceBar is the rate a series had to clear to be stored individually:
	// max(FloorRate, SignificanceFactor * MedianRate).
	SignificanceBar float64 `json:"significance_bar"`

	// Truncated is true when more series cleared the bar than MaxSeries allows,
	// meaning Series is incomplete.
	Truncated bool `json:"truncated"`
}

// SeriesEntry holds the exact sample statistics of one outlier series. Samples,
// MinTime and MaxTime are raw values so that consumers can recompute rates over
// windows of their choice; Rate is samples/s over max(MaxTime-MinTime, MinSpan).
type SeriesEntry struct {
	Labels  map[string]string `json:"labels"`
	Samples uint64            `json:"samples"`
	MinTime int64             `json:"min_time"`
	MaxTime int64             `json:"max_time"`
	Rate    float64           `json:"rate"`
}

// NewObserverFactory returns a tsdb.SeriesStatsObserverFactory that writes a series
// rate stats sidecar into every block built by the TSDB compactor it is installed
// into. The sidecar is written into the block directory before the block is
// finalized, so it is uploaded together with the block. Failures are logged and
// never fail the block write.
func NewObserverFactory(cfg Config, logger log.Logger) tsdb.SeriesStatsObserverFactory {
	return func(meta *tsdb.BlockMeta, blockDir string) tsdb.SeriesStatsObserver {
		return newCollector(cfg, logger, meta, blockDir)
	}
}

// collector accumulates per-series rate statistics for a single output block.
// Add is called from the block writer goroutine only, and Done happens after all
// Add calls have finished, so no synchronization is needed.
type collector struct {
	cfg      Config
	logger   log.Logger
	meta     *tsdb.BlockMeta
	blockDir string

	numSeries  uint64
	numSamples uint64
	maxRate    float64

	// Reservoir sample (algorithm R) of per-series rates, used to estimate quantiles.
	reservoir []float64
	seen      int64
	rnd       *rand.Rand

	// Min-heap (by rate) of the top MaxSeries candidate series with rate >= FloorRate.
	candidates candidateHeap
	// evictedMaxRate is the highest rate dropped from the full heap, used to detect truncation.
	evictedMaxRate float64
	evicted        bool
}

func newCollector(cfg Config, logger log.Logger, meta *tsdb.BlockMeta, blockDir string) *collector {
	return &collector{
		cfg:       cfg,
		logger:    logger,
		meta:      meta,
		blockDir:  blockDir,
		reservoir: make([]float64, 0, cfg.ReservoirSize),
		rnd:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Add implements tsdb.SeriesStatsObserver. Chunk contents are only valid during the
// call, so all needed values are extracted here; labels are copied on retention.
func (c *collector) Add(lbls labels.Labels, chks []chunks.Meta) {
	if len(chks) == 0 {
		return
	}

	var samples uint64
	minT, maxT := chks[0].MinTime, chks[0].MaxTime
	for _, chk := range chks {
		samples += uint64(chk.Chunk.NumSamples())
		minT = min(minT, chk.MinTime)
		maxT = max(maxT, chk.MaxTime)
	}

	span := time.Duration(maxT-minT) * time.Millisecond
	rate := float64(samples) / max(span, c.cfg.MinSpan).Seconds()

	c.numSeries++
	c.numSamples += samples
	c.maxRate = max(c.maxRate, rate)

	// Reservoir sampling (algorithm R) of all rates for the quantile estimates.
	c.seen++
	if len(c.reservoir) < c.cfg.ReservoirSize {
		c.reservoir = append(c.reservoir, rate)
	} else if j := c.rnd.Int63n(c.seen); j < int64(c.cfg.ReservoirSize) {
		c.reservoir[j] = rate
	}

	// Keep the top MaxSeries candidates above the absolute floor. The significance
	// bar is only known once the whole block has been seen, so the final filtering
	// happens in Done.
	if rate < c.cfg.FloorRate {
		return
	}
	if len(c.candidates) < c.cfg.MaxSeries {
		heap.Push(&c.candidates, candidate{lbls: lbls.Copy(), samples: samples, minTime: minT, maxTime: maxT, rate: rate})
		return
	}
	c.evicted = true
	if rate > c.candidates[0].rate {
		c.evictedMaxRate = max(c.evictedMaxRate, c.candidates[0].rate)
		c.candidates[0] = candidate{lbls: lbls.Copy(), samples: samples, minTime: minT, maxTime: maxT, rate: rate}
		heap.Fix(&c.candidates, 0)
	} else {
		c.evictedMaxRate = max(c.evictedMaxRate, rate)
	}
}

// Done implements tsdb.SeriesStatsObserver. It derives the significance bar from
// the block's own rate distribution, filters the candidates by it and writes the
// sidecar file into the block directory.
func (c *collector) Done() {
	stats := c.buildStats()

	data, err := json.Marshal(stats)
	if err == nil {
		err = os.WriteFile(filepath.Join(c.blockDir, block.SeriesRateStatsFilename), data, 0o644)
	}
	if err != nil {
		// Generation is best-effort: never fail the block write over the sidecar.
		level.Warn(c.logger).Log("msg", "failed to write series rate stats sidecar", "block", c.meta.ULID, "err", err)
		return
	}

	level.Debug(c.logger).Log("msg", "written series rate stats sidecar", "block", c.meta.ULID,
		"series", c.numSeries, "outliers", len(stats.Series), "bar", stats.Summary.SignificanceBar)
}

func (c *collector) buildStats() Stats {
	quantiles := slices.Clone(c.reservoir)
	slices.Sort(quantiles)

	summary := Summary{
		NumSeries:  c.numSeries,
		NumSamples: c.numSamples,
		MedianRate: quantileOf(quantiles, 0.5),
		P90Rate:    quantileOf(quantiles, 0.9),
		P99Rate:    quantileOf(quantiles, 0.99),
		MaxRate:    c.maxRate,
	}
	summary.SignificanceBar = max(c.cfg.FloorRate, c.cfg.SignificanceFactor*summary.MedianRate)

	series := make([]SeriesEntry, 0, len(c.candidates))
	for _, cand := range c.candidates {
		if cand.rate < summary.SignificanceBar {
			continue
		}
		series = append(series, SeriesEntry{
			Labels:  cand.lbls.Map(),
			Samples: cand.samples,
			MinTime: cand.minTime,
			MaxTime: cand.maxTime,
			Rate:    cand.rate,
		})
	}
	sort.Slice(series, func(i, j int) bool { return series[i].Rate > series[j].Rate })

	// Series above the bar were dropped only if the heap overflowed with a rate that
	// would have cleared the bar.
	summary.Truncated = c.evicted && c.evictedMaxRate >= summary.SignificanceBar

	return Stats{
		Version: Version,
		BlockID: c.meta.ULID.String(),
		MinTime: c.meta.MinTime,
		MaxTime: c.meta.MaxTime,
		Summary: summary,
		Series:  series,
	}
}

// quantileOf returns the q-quantile of the sorted values, or 0 when empty.
func quantileOf(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx]
}

// ReadFromDir reads the series rate stats sidecar from a local block directory.
func ReadFromDir(blockDir string) (*Stats, error) {
	data, err := os.ReadFile(filepath.Join(blockDir, block.SeriesRateStatsFilename))
	if err != nil {
		return nil, err
	}
	stats := &Stats{}
	if err := json.Unmarshal(data, stats); err != nil {
		return nil, err
	}
	return stats, nil
}

type candidate struct {
	lbls    labels.Labels
	samples uint64
	minTime int64
	maxTime int64
	rate    float64
}

// candidateHeap is a min-heap of candidates by rate.
type candidateHeap []candidate

func (h candidateHeap) Len() int           { return len(h) }
func (h candidateHeap) Less(i, j int) bool { return h[i].rate < h[j].rate }
func (h candidateHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *candidateHeap) Push(x any)        { *h = append(*h, x.(candidate)) }
func (h *candidateHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
