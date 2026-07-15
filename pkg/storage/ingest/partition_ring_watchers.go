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
// ring(s). When compartmentsEnabled is false it watches a single ring under the plain ringName/ringKey,
// behaving exactly as before compartments existed (numCompartments is ignored). When compartmentsEnabled
// is true it watches numCompartments rings, each keyed for its read compartment via
// compartments.WithReadCompartmentSuffix. Watchers are indexed by read compartment ID.
func NewPartitionRingWatchers(compartmentsEnabled bool, numCompartments int, ringName, ringKey string, kvClient kv.Client, logger log.Logger, reg prometheus.Registerer) (*ring.PartitionRingWatchers, error) {
	var watchers []*ring.PartitionRingWatcher
	if !compartmentsEnabled {
		watchers = []*ring.PartitionRingWatcher{
			ring.NewPartitionRingWatcher(ringName, ringKey, kvClient, logger, reg),
		}
	} else {
		if numCompartments < 1 {
			return nil, errors.Errorf("the number of compartments must be at least 1, got %d", numCompartments)
		}
		watchers = make([]*ring.PartitionRingWatcher, numCompartments)
		for c := range watchers {
			name := compartments.WithReadCompartmentSuffix(ringName, c)
			key := compartments.WithReadCompartmentSuffix(ringKey, c)

			watchers[c] = ring.NewPartitionRingWatcher(name, key, kvClient, logger, reg)
		}
	}

	return ring.NewPartitionRingWatchers(watchers...)
}
