// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"sort"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// seriesStats accumulates, for a single series of one tenant, how many samples
// were ingested and the distribution of deltas (in milliseconds) between
// consecutive sample timestamps as they arrive in the dump.
type seriesStats struct {
	labels  string
	samples int

	lastTS  int64
	hasLast bool

	minDelta   int64
	maxDelta   int64
	sumDelta   int64
	deltaCount int
}

// observe records a sample at timestamp ts (ms). Samples must be observed in
// arrival order so the delta against the previous sample is meaningful.
func (s *seriesStats) observe(ts int64) {
	if s.hasLast {
		delta := ts - s.lastTS
		if s.deltaCount == 0 || delta < s.minDelta {
			s.minDelta = delta
		}
		if s.deltaCount == 0 || delta > s.maxDelta {
			s.maxDelta = delta
		}
		s.sumDelta += delta
		s.deltaCount++
	}
	s.lastTS = ts
	s.hasLast = true
	s.samples++
}

// doAnalyseSeries scans every write request for the requested tenant and, for
// each series, reports the number of ingested samples and the min/max/avg delta
// between consecutive sample timestamps. Detection is per-tenant by definition
// (the command operates on a single tenant), keyed by series hash.
func (c *DumpCommand) doAnalyseSeries(*kingpin.ParseContext) error {
	series := make(map[uint64]*seriesStats)
	hashBuf := make([]byte, 0, 1024)

	err := c.parseWriteRequestsFromDumpFile(
		func(_ int, record *kgo.Record, req *mimirpb.WriteRequest) {
			if string(record.Key) != c.seriesTenant {
				return
			}

			for _, ts := range req.Timeseries {
				var h uint64
				hashBuf, h = ingest.LabelAdaptersHash(hashBuf, ts.Labels)

				st, ok := series[h]
				if !ok {
					// labels.String() builds a fresh string, so it's safe to retain
					// beyond the request buffer's lifetime (see yoloString caveat).
					st = &seriesStats{labels: mimirpb.FromLabelAdaptersToLabels(ts.Labels).String()}
					series[h] = st
				}

				for _, s := range ts.Samples {
					st.observe(s.TimestampMs)
				}
				for _, hp := range ts.Histograms {
					st.observe(hp.Timestamp)
				}
			}
		},
		func(recordIdx int, err error) {
			c.printer.PrintLine(fmt.Sprintf("corrupted record %d: %v", recordIdx, err))
		},
	)
	if err != nil {
		return err
	}

	c.printSeriesAnalysis(series)
	return nil
}

func (c *DumpCommand) printSeriesAnalysis(series map[uint64]*seriesStats) {
	stats := make([]*seriesStats, 0, len(series))
	var totalSamples int
	for _, s := range series {
		stats = append(stats, s)
		totalSamples += s.samples
	}

	// Most active series first; ties broken by labels for deterministic output.
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].samples != stats[j].samples {
			return stats[i].samples > stats[j].samples
		}
		return stats[i].labels < stats[j].labels
	})

	c.printer.PrintLine(fmt.Sprintf("=== SERIES ANALYSIS (tenant: %s) ===", c.seriesTenant))
	c.printer.PrintLine(fmt.Sprintf("%-15s %d", "Unique series:", len(stats)))
	c.printer.PrintLine(fmt.Sprintf("%-15s %d", "Total samples:", totalSamples))
	c.printer.PrintLine("")
	c.printer.PrintLine(fmt.Sprintf("%10s %15s %15s %15s  %s", "Samples", "MinDelta", "MaxDelta", "AvgDelta", "Labels"))
	for _, s := range stats {
		c.printer.PrintLine(fmt.Sprintf("%10d %15s %15s %15s  %s",
			s.samples,
			formatDelta(s.minDelta, s.deltaCount),
			formatDelta(s.maxDelta, s.deltaCount),
			formatAvgDelta(s.sumDelta, s.deltaCount),
			s.labels,
		))
	}
}

// formatDelta renders a delta in milliseconds as a duration, or "-" when the
// series has a single sample and therefore no delta.
func formatDelta(deltaMs int64, deltaCount int) string {
	if deltaCount == 0 {
		return "-"
	}
	return (time.Duration(deltaMs) * time.Millisecond).String()
}

func formatAvgDelta(sumMs int64, deltaCount int) string {
	if deltaCount == 0 {
		return "-"
	}
	avgMs := float64(sumMs) / float64(deltaCount)
	return time.Duration(avgMs * float64(time.Millisecond)).String()
}
