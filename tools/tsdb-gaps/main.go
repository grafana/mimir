// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	defaultMinTotalMissedSamples  = 1
	defaultMaxTotalMissedSamples  = 1000000
	defaultMinSingleMissedSamples = 1
	defaultMaxSingleMissedSamples = 10000
	defaultMinTotalGapTime        = 1
	defaultMaxTotalGapTime        = 86400
	defaultMinSingleGapTime       = 1
	defaultMaxSingleGapTime       = 86400
	defaultScrapeInterval         = 0
)

type config struct {
	minTime                flagext.Time
	maxTime                flagext.Time
	minTotalMissedSamples  int
	maxTotalMissedSamples  int
	minSingleMissedSamples int
	maxSingleMissedSamples int
	minTotalGapTime        int
	maxTotalGapTime        int
	minSingleGapTime       int
	maxSingleGapTime       int
	scrapeInterval         int
	selector               string
}

func (c *config) registerFlags(f *flag.FlagSet) {
	f.Var(&c.minTime, "mint", "Minimum timestamp to consider (default: block min time)")
	f.Var(&c.maxTime, "maxt", "Maximum timestamp to consider (default: block max time)")
	f.IntVar(&c.minTotalMissedSamples, "min-total-missed-samples", defaultMinTotalMissedSamples, "Minimum total missed samples in a series")
	f.IntVar(&c.maxTotalMissedSamples, "max-total-missed-samples", defaultMaxTotalMissedSamples, "Maximum total missed samples in a series")
	f.IntVar(&c.minSingleMissedSamples, "min-single-missed-samples", defaultMinSingleMissedSamples, "Minimum missed samples in a single gap")
	f.IntVar(&c.maxSingleMissedSamples, "max-single-missed-samples", defaultMaxSingleMissedSamples, "Maximum missed samples in a single gap")
	f.IntVar(&c.minTotalGapTime, "min-total-gap-time", defaultMinTotalGapTime, "Minimum total gap time in seconds for a series")
	f.IntVar(&c.maxTotalGapTime, "max-total-gap-time", defaultMaxTotalGapTime, "Maximum total gap time in seconds for a series")
	f.IntVar(&c.minSingleGapTime, "min-single-gap-time", defaultMinSingleGapTime, "Minimum gap time in seconds for a single gap")
	f.IntVar(&c.maxSingleGapTime, "max-single-gap-time", defaultMaxSingleGapTime, "Maximum gap time in seconds for a single gap")
	f.IntVar(&c.scrapeInterval, "scrape-interval", defaultScrapeInterval, "Threshold for gap detection in seconds, set to 0 for automatic interval detection")
	f.StringVar(&c.selector, "select", "", "PromQL metric selector (e.g. '{__name__=~\"some_metric_prefix_.*\"}'")
}

func (c *config) validate() error {
	maxTime := time.Time(c.maxTime)
	if !maxTime.IsZero() && time.Time(c.minTime).After(maxTime) {
		return fmt.Errorf("minimum timestamp is greater than maximum timestamp")
	}
	if c.minTotalMissedSamples < 0 {
		return fmt.Errorf("minimum total missed samples must be non-negative")
	}
	if c.maxTotalMissedSamples < 0 {
		return fmt.Errorf("maximum total missed samples must be non-negative")
	}
	if c.minTotalMissedSamples > c.maxTotalMissedSamples {
		return fmt.Errorf("minimum total missed samples is greater than maximum total missed samples")
	}
	if c.minSingleMissedSamples < 0 {
		return fmt.Errorf("minimum missed samples in a single series must be non-negative")
	}
	if c.maxSingleMissedSamples < 0 {
		return fmt.Errorf("maximum missed samples in a single series must be non-negative")
	}
	if c.minSingleMissedSamples > c.maxSingleMissedSamples {
		return fmt.Errorf("minimum missed samples in a single series is greater than maximum missed samples in a single series")
	}
	if c.minTotalGapTime < 0 {
		return fmt.Errorf("minimum total gap time must be non-negative")
	}
	if c.maxTotalGapTime < 0 {
		return fmt.Errorf("maximum total gap time must be non-negative")
	}
	if c.minTotalGapTime > c.maxTotalGapTime {
		return fmt.Errorf("minimum total gap time is greater than maximum total gap time")
	}
	if c.minSingleGapTime < 0 {
		return fmt.Errorf("minimum gap time in a single series must be non-negative")
	}
	if c.maxSingleGapTime < 0 {
		return fmt.Errorf("maximum gap time in a single series must be non-negative")
	}
	if c.minSingleGapTime > c.maxSingleGapTime {
		return fmt.Errorf("minimum gap time in a single series is greater than maximum gap time in a single series")
	}
	if c.scrapeInterval < 0 {
		return fmt.Errorf("gap threshold must be non-negative")
	}
	// force the use of a metrics selector, as analyzing all series can be slow
	if c.selector == "" {
		if _, err := parser.ParseMetricSelector(c.selector); err != nil {
			return fmt.Errorf("failed to parse metric selector: %w", err)
		}

	}
	return nil
}

type gap struct {
	Start     int64 `json:"start"`     // Start of the gap in milliseconds
	End       int64 `json:"end"`       // End of the gap in milliseconds
	Intervals int   `json:"intervals"` // Number of intervals missed
}

type seriesGapStats struct {
	SeriesLabels              string `json:"seriesLabels"`
	MinTime                   int64  `json:"minTime"`
	MaxTime                   int64  `json:"maxTime"`
	TotalSamples              uint64 `json:"totalSamples"`
	MissedSamples             uint64 `json:"missedSamples"`
	MinIntervalDiffMillis     int64  `json:"minIntervalDiffMillis"`
	MaxIntervalDiffMillis     int64  `json:"maxIntervalDiffMillis"`
	MostCommonIntervalSeconds int    `json:"mostCommonIntervalSeconds"`
	GapThreshold              int    `json:"gapThreshold"`
	Gaps                      []gap  `json:"gaps"`
}

type blockGapStats struct {
	BlockID             string           `json:"blockID"`
	MinTime             int64            `json:"minTime"`
	MaxTime             int64            `json:"maxTime"`
	TotalSeries         uint64           `json:"totalSeries"`
	TotalMatchedSeries  uint64           `json:"totalMatchedSeries"`
	TotalSeriesWithGaps uint64           `json:"totalSeriesWithGaps"`
	TotalSamples        uint64           `json:"totalSamples"`
	TotalMissedSamples  uint64           `json:"totalMissedSamples"`
	GapStats            []seriesGapStats `json:"gapStats"`
}

var logger = log.NewLogfmtLogger(os.Stderr)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := config{}
	cfg.registerFlags(flag.CommandLine)

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) == 0 {
		level.Error(logger).Log("msg", "no block directory specified")
		return
	}

	if err := cfg.validate(); err != nil {
		level.Error(logger).Log("msg", err.Error())
		os.Exit(1)
	}

	ctx := context.Background()

	var matchers []*labels.Matcher
	if cfg.selector != "" {
		var err error
		matchers, err = parser.ParseMetricSelector(cfg.selector)
		if err != nil {
			level.Error(logger).Log("msg", "failed to parse matcher selector", "err", err)
			os.Exit(1)
		}

		var matchersStr []interface{}
		matchersStr = append(matchersStr, "msg", "using matchers")
		for _, m := range matchers {
			matchersStr = append(matchersStr, "matcher", m.String())
		}

		level.Debug(logger).Log(matchersStr...)
	}

	gapReport := map[string]blockGapStats{}

	for _, blockDir := range args {
		stats, err := analyzeBlockForGaps(ctx, cfg, blockDir, matchers)
		if err != nil {
			level.Error(logger).Log("msg", "failed to analyze block", "dir", blockDir, "err", err)
			os.Exit(1)
		}
		gapReport[blockDir] = stats
	}
	// write gap report as JSON
	err = json.NewEncoder(os.Stdout).Encode(gapReport)
	if err != nil {
		level.Error(logger).Log("msg", "failed to write gap report", "err", err)
	}
}

func analyzeBlockForGaps(ctx context.Context, cfg config, blockDir string, matchers []*labels.Matcher) (blockGapStats, error) {
	var blockStats blockGapStats
	blockStats.BlockID = blockDir
	b, err := tsdb.OpenBlock(util_log.SlogFromGoKit(logger), blockDir, nil, nil)
	if err != nil {
		return blockStats, fmt.Errorf("failed to open block: %w", err)
	}
	defer b.Close()

	// Get the block's min and max time and compare to the configured range limits
	meta := b.Meta()
	rangeStart, rangeEnd, err := calcRange(time.Time(cfg.minTime), time.Time(cfg.maxTime), &meta)
	if err != nil {
		return blockStats, err
	}
	blockStats.MinTime = meta.MinTime
	blockStats.MaxTime = meta.MaxTime
	blockStats.TotalSeries = meta.Stats.NumSeries
	blockStats.TotalSamples = meta.Stats.NumSamples

	idx, err := b.Index()
	if err != nil {
		return blockStats, err
	}
	defer idx.Close()

	p, err := idx.PostingsForMatchers(ctx, true, matchers...)
	if err != nil {
		return blockStats, err
	}

	var builder labels.ScratchBuilder
	for p.Next() {
		chks := []chunks.Meta(nil)
		err := idx.Series(p.At(), &builder, &chks)
		if err != nil {
			level.Error(logger).Log("msg", "series skipped, error getting series", "block", meta.ULID.String(), "seriesID", p.At(), "err", err)
			continue
		}

		lbls := builder.Labels()
		matches := true
		for _, m := range matchers {
			val := lbls.Get(m.Name)
			if !m.Matches(val) {
				matches = false
				break
			}
		}

		if !matches {
			continue
		}
		blockStats.TotalMatchedSeries++

		cr, err := b.Chunks()
		if err != nil {
			return blockStats, err

		}
		defer cr.Close()

		minTime, maxTime, minDiff, maxDiff, intervalCounts, timestamps := chunkGaps(cr, chks, rangeStart, rangeEnd)

		minSingleGap := int64(cfg.minSingleGapTime) * 1000
		maxSingleGap := int64(cfg.maxSingleGapTime) * 1000
		minTotalGap := int64(cfg.minTotalGapTime) * 1000
		maxTotalGap := int64(cfg.maxTotalGapTime) * 1000
		minSingleMissedSamples := int64(cfg.minSingleMissedSamples)
		maxSingleMissedSamples := int64(cfg.maxSingleMissedSamples)
		minTotalMissedSamples := int64(cfg.minTotalMissedSamples)
		maxTotalMissedSamples := int64(cfg.maxTotalMissedSamples)
		var mostCommonInterval int64
		for interval, count := range intervalCounts {
			if count > mostCommonInterval {
				mostCommonInterval = interval
			}
		}
		var gapsPresent bool
		// an interval difference should be more than 1.5x the expected interval difference to be a gap
		gapThreshold := int64(3*cfg.scrapeInterval/2) * 1000
		if gapThreshold == 0 {
			if maxDiff >= 2*minDiff {
				gapsPresent = true
			}
			gapThreshold = 3 * mostCommonInterval * 1000 / 2
		} else {
			// this accounts for if the set scrape interval is lower than the observed interval
			if len(intervalCounts) == 1 {
				gapThreshold = mostCommonInterval * 1000
			}
			if maxDiff > gapThreshold {
				gapsPresent = true
			}
		}
		if !gapsPresent {
			continue
		}
		var seriesStats seriesGapStats
		seriesStats.SeriesLabels = lbls.String()
		seriesStats.GapThreshold = int(gapThreshold)
		var totalGapTime int64
		var totalMissedSamples int64
		for i := 0; i < len(timestamps)-1; i++ {
			diff := timestamps[i+1] - timestamps[i]
			if diff > gapThreshold {
				missedSamples := diff / gapThreshold
				totalGapTime += diff
				totalMissedSamples += missedSamples
				// evaluate single gap filters
				if diff > maxSingleGap || diff < minSingleGap || missedSamples > maxSingleMissedSamples || missedSamples < minSingleMissedSamples {
					continue
				}
				seriesStats.Gaps = append(seriesStats.Gaps, gap{
					Start:     timestamps[i],
					End:       timestamps[i+1],
					Intervals: int(missedSamples),
				})
				seriesStats.MissedSamples += uint64(missedSamples)
			}
		}
		// evaluate total gap filters
		if len(seriesStats.Gaps) > 0 && totalGapTime >= minTotalGap && totalGapTime <= maxTotalGap && totalMissedSamples >= minTotalMissedSamples && totalMissedSamples <= maxTotalMissedSamples {
			seriesStats.MinTime = minTime
			seriesStats.MaxTime = maxTime
			seriesStats.TotalSamples = uint64(len(timestamps))
			seriesStats.MinIntervalDiffMillis = minDiff
			seriesStats.MaxIntervalDiffMillis = maxDiff
			seriesStats.MostCommonIntervalSeconds = int(mostCommonInterval)
			blockStats.TotalMissedSamples += seriesStats.MissedSamples
			blockStats.GapStats = append(blockStats.GapStats, seriesStats)
			blockStats.TotalSeriesWithGaps++
		}
	}
	if p.Err() != nil {
		return blockStats, p.Err()
	}
	return blockStats, nil
}

// roundToNearestSecond rounds a unix timestamp in milliseconds to nearest second
// this is used to estimate the scrape interval, which is configured in seconds
func roundToNearestSecond(t int64) int64 {
	return (t + 500) / 1000
}

// calcRange calculates the start and end of the range to analyze based on the block metadata
func calcRange(cfgStart, cfgEnd time.Time, meta *tsdb.BlockMeta) (int64, int64, error) {
	var rangeStart, rangeEnd int64
	if cfgStart.IsZero() {
		rangeStart = meta.MinTime
	} else {
		if cfgStart.Unix() >= meta.MaxTime {
			return rangeStart, rangeEnd, fmt.Errorf("configured minimum timestamp is greater than block max time")
		}
		rangeStart = cfgStart.UnixMilli()
	}
	if cfgEnd.IsZero() {
		rangeEnd = meta.MaxTime
	} else {
		rangeEnd = cfgEnd.UnixMilli()
	}
	return rangeStart, rangeEnd, nil
}

// chunkGaps reads chunks from a series and calculates the gaps between timestamps
func chunkGaps(cr tsdb.ChunkReader, chks []chunks.Meta, rangeStart, rangeEnd int64) (int64, int64, int64, int64, map[int64]int64, []int64) {
	var it chunkenc.Iterator
	var timestamps []int64
	var minTime, maxTime int64
	intervalCounts := map[int64]int64{}
	var minDiff, maxDiff int64
	minDiff = 1<<63 - 1
	maxDiff = -1
	minTime = 1<<63 - 1
	var lastT int64
	for _, meta := range chks {
		ch, iter, err := cr.ChunkOrIterable(meta)
		if meta.MinTime < minTime {
			minTime = meta.MinTime
		}
		maxTime = meta.MaxTime
		if err != nil {
			level.Error(logger).Log("msg", "failed to open chunk", "ref", meta.Ref, "err", err)
			continue
		}
		if iter != nil {
			level.Error(logger).Log("msg", "got iterable from ChunkOrIterable", "ref", meta.Ref)
			continue
		}
		it = ch.Iterator(nil)
		for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
			t, _ := it.At()
			if t < rangeStart {
				lastT = t
				continue
			}
			if t > rangeEnd {
				break
			}
			if lastT != 0 {
				diff := t - lastT
				if diff < minDiff {
					minDiff = diff
				}
				if diff > maxDiff {
					maxDiff = diff
				}
				timestamps = append(timestamps, t)
				seconds := roundToNearestSecond(diff)
				intervalCounts[seconds]++
			}
			lastT = t
		}
	}
	return minTime, maxTime, minDiff, maxDiff, intervalCounts, timestamps
}
