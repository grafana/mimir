// SPDX-License-Identifier: AGPL-3.0-only

package cardgen

import (
	"fmt"
	"time"

	"github.com/grafana/mimir/tools/headcount/model"
)

// epoch is the fixed start time every profile's population begins at, so two
// runs with the same seed produce byte-identical fixtures regardless of when
// they're run.
var epoch = time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)

// Profile bundles a population Config with the partition count the
// downstream compactor run must match, one per scale tier (small,
// medium, large), plus thresholds, which targets E6 and E9.
type Profile struct {
	Population model.Config
	Partitions int
}

// Profiles are approximate: SeriesCap is tuned so the generated population's
// total active series lands near the tier's target, not exactly on it. Exact
// series counts don't matter for any pass criterion; only order of
// magnitude does.
var profiles = map[string]Profile{
	"small": {
		Partitions: 2,
		Population: model.Config{
			Start: epoch, End: epoch.Add(2 * 24 * time.Hour),
			MetricNames: 5_000,
			SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 60, // ~48k concurrent series, tuned against the ~50k target.
			ChurnFraction: 0.1, ChurnPeriod: 24 * time.Hour,
			GapFraction: 0.01, GapDuration: 2 * time.Hour,
			StaleFraction: 0.01,
			SpikeMetric:   -1,
		},
	},
	"medium": {
		Partitions: 4,
		Population: model.Config{
			Start: epoch, End: epoch.Add(4 * 24 * time.Hour),
			MetricNames: 100_000,
			SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 65, // ~1M concurrent, ~1.3M total rows with churn.
			ChurnFraction: 0.1, ChurnPeriod: 24 * time.Hour,
			GapFraction: 0.01, GapDuration: 2 * time.Hour,
			StaleFraction: 0.01,
			SpikeMetric:   0, SpikeDay: 2, SpikeBaseValues: 10, SpikePeakValues: 100_000,
		},
	},
	// thresholds is small plus names sized to sit above E6's 10k
	// exact-hash threshold and close to E9's top-N threshold, so that both
	// are tested on several names rather than one.
	"thresholds": {
		Partitions: 2,
		Population: model.Config{
			Start: epoch, End: epoch.Add(2 * 24 * time.Hour),
			MetricNames: 5_000,
			SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 60,
			FixedSeries:   thresholdSeries(),
			ChurnFraction: 0.1, ChurnPeriod: 24 * time.Hour,
			GapFraction: 0.01, GapDuration: 2 * time.Hour,
			StaleFraction: 0.01,
			SpikeMetric:   -1,
		},
	},
	"large": {
		Partitions: 4,
		Population: model.Config{
			Start: epoch, End: epoch.Add(24 * time.Hour),
			MetricNames: 1_000_000,
			SeriesZipfS: 1.2, SeriesFloor: 1, SeriesCap: 300,
			SpikeMetric: -1,
		},
	},
}

// thresholdSeries returns the thresholds profile's fixed series counts:
// six names 100 series apart at the top (19,000 down to 18,500), which is
// where E9's threshold falls with a top-5, then sixteen names from 17,500
// down to 10,000 in steps of 500, all above E6's threshold. Churn adds
// about 10% distinct series to each.
func thresholdSeries() []uint64 {
	var out []uint64
	for n := uint64(19_000); n >= 18_500; n -= 100 {
		out = append(out, n)
	}
	for n := uint64(17_500); n >= 10_000; n -= 500 {
		out = append(out, n)
	}
	return out
}

// ProfileNames returns the known profile names, for flag usage messages.
func ProfileNames() []string {
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	return names
}

// LoadProfile returns the named profile with seed applied to its
// population. Unlike the profile's other fields, the seed is never baked
// in, so the same profile can be regenerated under different seeds without
// editing this file.
func LoadProfile(name string, seed int64) (Profile, error) {
	p, ok := profiles[name]
	if !ok {
		return Profile{}, fmt.Errorf("unknown profile %q, want one of %v", name, ProfileNames())
	}
	p.Population.Seed = seed
	return p, nil
}
