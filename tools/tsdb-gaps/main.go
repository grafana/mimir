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
	"github.com/prometheus/prometheus/tsdb/index"
)

const (
	defaultScrapeInterval = 15
)

type config struct {
	minTime        flagext.Time
	maxTime        flagext.Time
	scrapeInterval int
	selector       string
}

func (c *config) registerFlags(f *flag.FlagSet) {
	f.Var(&c.minTime, "mint", "Minimum timestamp to consider (default: block min time)")
	f.Var(&c.maxTime, "maxt", "Maximum timestamp to consider (default: block max time)")
	f.IntVar(&c.scrapeInterval, "scrape-interval", defaultScrapeInterval, "Threshold for gap detection in seconds (default: 15, set to 0 to automatic interval detection)")
	f.StringVar(&c.selector, "select", "", "PromQL metric selector (default: all series)")
}

func (c *config) validate() error {
	if time.Time(c.minTime).After(time.Time(c.maxTime)) {
		return fmt.Errorf("minimum timestamp is greater than maximum timestamp")
	}
	if c.scrapeInterval < 0 {
		return fmt.Errorf("gap threshold must be non-negative")
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
	Gaps                      []gap  `json:"gaps"`
}

type blockGapStats struct {
	BlockID             string           `json:"blockID"`
	MinTime             int64            `json:"minTime"`
	MaxTime             int64            `json:"maxTime"`
	TotalSeries         uint64           `json:"totalSeries"`
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
		fmt.Println("No block directory specified.")
		return
	}

	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
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
	b, err := tsdb.OpenBlock(logger, blockDir, nil)
	if err != nil {
		return blockStats, fmt.Errorf("failed to open block: %w", err)
	}
	defer b.Close()

	// Get the block's min and max time and compare to the configured range limits
	meta := b.Meta()
	var rangeStart, rangeEnd int64
	cfgStart := time.Time(cfg.minTime)
	cfgEnd := time.Time(cfg.maxTime)
	if cfgStart.IsZero() {
		rangeStart = meta.MinTime
	} else {
		if cfgStart.Unix() >= meta.MaxTime {
			return blockStats, fmt.Errorf("configured minimum timestamp is greater than block max time")
		}
		rangeStart = cfgStart.UnixMilli()
	}
	if cfgEnd.IsZero() {
		rangeEnd = meta.MaxTime
	} else {
		rangeEnd = cfgEnd.UnixMilli()
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

	k, v := index.AllPostingsKey()

	// If there is any "equal" matcher, we can use it for getting postings instead,
	// it can massively speed up iteration over the index, especially for large blocks.
	for _, m := range matchers {
		if m.Type == labels.MatchEqual {
			k = m.Name
			v = m.Value
			break
		}
	}

	p, err := idx.Postings(ctx, k, v)
	if err != nil {
		return blockStats, err
	}

	var builder labels.ScratchBuilder
	for p.Next() {
		chks := []chunks.Meta(nil)
		err := idx.Series(p.At(), &builder, &chks)
		if err != nil {
			level.Error(logger).Log("msg", "error getting series", "seriesID", p.At(), "err", err)
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

		cr, err := b.Chunks()
		if err != nil {
			return blockStats, err

		}
		defer cr.Close()

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
				level.Error(logger).Log("msg", "got iterable from ChunkorIterable", "ref", meta.Ref)
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
		var mostCommonInterval int64
		for interval, count := range intervalCounts {
			if count > mostCommonInterval {
				mostCommonInterval = interval
			}
		}
		var gapsPresent bool
		gapThreshold := int64(3*cfg.scrapeInterval/2) * 1000
		if gapThreshold == 0 {
			if maxDiff > 2*minDiff {
				gapsPresent = true
			}
			gapThreshold = 3 * mostCommonInterval * 1000 / 2
		} else {
			if maxDiff > gapThreshold {
				gapsPresent = true
			}
		}
		if gapsPresent {
			var seriesStats seriesGapStats
			seriesStats.SeriesLabels = lbls.String()
			for i := 0; i < len(timestamps)-1; i++ {
				diff := timestamps[i+1] - timestamps[i]
				if diff > gapThreshold {
					missedIntervals := int(diff / gapThreshold)
					seriesStats.Gaps = append(seriesStats.Gaps, gap{
						Start:     timestamps[i],
						End:       timestamps[i+1],
						Intervals: missedIntervals,
					})
					seriesStats.MissedSamples += uint64(missedIntervals)
				}
			}
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
