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
// medium, large).
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
