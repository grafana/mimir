// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/compartments"
)

// NewPartitionRingWatchers creates a ring.PartitionRingWatchers watching the ingester partition
// ring(s). When compartments are disabled it watches a single ring under the plain ringName/ringKey,
// behaving exactly as before compartments existed. When compartments are enabled it watches one ring per
// read compartment, each named and keyed for its read compartment via
// compartments.Config.ReadCompartmentName. Watchers are indexed by read compartment ID.
// tokenGenerator resolves the tokens of partitions that use derived tokens.
func NewPartitionRingWatchers(compartmentsCfg compartments.Config, ringName, ringKey string, kvClient kv.Client, tokenGenerator ring.PartitionTokenGenerator, logger log.Logger, reg prometheus.Registerer) (*ring.PartitionRingWatchers, error) {
	opts := ring.DefaultPartitionRingOptions()
	opts.TokenGenerator = tokenGenerator

	var watchers []*ring.PartitionRingWatcher
	if !compartmentsCfg.Enabled {
		watchers = []*ring.PartitionRingWatcher{
			ring.NewPartitionRingWatcherWithOptions(ringName, ringKey, kvClient, opts, logger, reg),
		}
	} else {
		if compartmentsCfg.Read.NumCompartments < 1 {
			return nil, errors.Errorf("the number of compartments must be at least 1, got %d", compartmentsCfg.Read.NumCompartments)
		}
		watchers = make([]*ring.PartitionRingWatcher, compartmentsCfg.Read.NumCompartments)
		for c := range watchers {
			name := compartmentsCfg.ReadCompartmentName(ringName, c)
			key := compartmentsCfg.ReadCompartmentName(ringKey, c)

			watchers[c] = ring.NewPartitionRingWatcherWithOptions(name, key, kvClient, opts, logger, reg)
		}
	}

	return ring.NewPartitionRingWatchers(watchers...)
}
