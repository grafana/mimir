// SPDX-License-Identifier: AGPL-3.0-only

// Package model defines a synthetic tenant population and computes exact
// ground-truth answers over it, so that generated TSDB blocks and the
// index-derived counts can both be checked against something known to be
// correct rather than against each other.
package model

import (
	"fmt"
	"time"
)

// Config parameterizes a synthetic population. All fields are required unless
// noted; Validate reports the first problem found.
type Config struct {
	// Seed makes generation deterministic: the same Config and Seed always
	// produce the same Model.
	Seed int64

	// Start and End bound the time range covered by the population. End is
	// exclusive, matching Prometheus's window convention.
	Start, End time.Time

	// MetricNames is the number of distinct __name__ values.
	MetricNames int
	// SeriesZipfS is the Zipf exponent (s > 1) governing series count per
	// metric name: a few names have most of the series, most names have few.
	SeriesZipfS float64
	// SeriesFloor and SeriesCap bound the series count for any one name.
	SeriesFloor, SeriesCap uint64

	// ChurnFraction is the fraction of each metric's series whose "pod" label
	// value is periodically replaced, splitting one logical workload into a
	// sequence of distinct label sets over time (matching Prometheus
	// semantics: a label value change is a new series, not a mutation).
	ChurnFraction float64
	// ChurnPeriod is how often a churned series is replaced by the next one
	// in its group.
	ChurnPeriod time.Duration

	// SpikeMetric is the index of the metric name (0 <= SpikeMetric <
	// MetricNames) whose label-value count jumps for one day. Set to -1 to
	// disable the spike.
	SpikeMetric int
	// SpikeDay is the 0-indexed day (from Start) on which the spike occurs.
	SpikeDay int
	// SpikeBaseValues and SpikePeakValues are the "pod" label's cardinality
	// outside and during the spike day.
	SpikeBaseValues, SpikePeakValues int

	// GapFraction is the fraction of series that have one interior interval
	// longer than an hour removed from their lifetime, simulating a scrape
	// outage rather than a clean start or end.
	GapFraction float64
	// GapDuration is the length of the removed interval.
	GapDuration time.Duration

	// StaleFraction is the fraction of series whose lifetime ends before
	// Config.End, simulating a staleness marker rather than continuing to
	// the end of the observed window.
	StaleFraction float64
}

// Validate reports the first invalid field, or nil if cfg can be used to
// generate a Model.
func (cfg Config) Validate() error {
	switch {
	case !cfg.End.After(cfg.Start):
		return fmt.Errorf("end %s must be after start %s", cfg.End, cfg.Start)
	case cfg.MetricNames <= 0:
		return fmt.Errorf("metric names must be positive, got %d", cfg.MetricNames)
	case cfg.SeriesZipfS <= 1:
		return fmt.Errorf("zipf exponent must be > 1, got %g", cfg.SeriesZipfS)
	case cfg.SeriesFloor == 0:
		return fmt.Errorf("series floor must be positive")
	case cfg.SeriesCap < cfg.SeriesFloor:
		return fmt.Errorf("series cap %d must be >= series floor %d", cfg.SeriesCap, cfg.SeriesFloor)
	case cfg.SpikeMetric >= cfg.MetricNames:
		return fmt.Errorf("spike metric index %d out of range [0, %d)", cfg.SpikeMetric, cfg.MetricNames)
	case cfg.SpikeMetric >= 0 && cfg.SpikePeakValues < cfg.SpikeBaseValues:
		return fmt.Errorf("spike peak values %d must be >= base values %d", cfg.SpikePeakValues, cfg.SpikeBaseValues)
	case cfg.GapFraction > 0 && cfg.GapDuration <= 0:
		return fmt.Errorf("gap duration must be positive when gap fraction > 0")
	case cfg.ChurnFraction > 0 && cfg.ChurnPeriod <= 0:
		return fmt.Errorf("churn period must be positive when churn fraction > 0")
	case cfg.GapFraction < 0 || cfg.GapFraction > 1:
		return fmt.Errorf("gap fraction must be in [0, 1], got %g", cfg.GapFraction)
	case cfg.ChurnFraction < 0 || cfg.ChurnFraction > 1:
		return fmt.Errorf("churn fraction must be in [0, 1], got %g", cfg.ChurnFraction)
	case cfg.StaleFraction < 0 || cfg.StaleFraction > 1:
		return fmt.Errorf("stale fraction must be in [0, 1], got %g", cfg.StaleFraction)
	}
	return nil
}

func (cfg Config) startMillis() int64 { return cfg.Start.UnixMilli() }
func (cfg Config) endMillis() int64   { return cfg.End.UnixMilli() }
