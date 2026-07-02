// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/compartments"
)

// PartitionRingWatchers watches the ingester partition ring(s) and exposes them through a single
// reference. When compartments are disabled it holds one watcher for the legacy partition ring; when
// enabled it holds one watcher per read compartment, each keyed via compartments.WithReadCompartmentSuffix.
//
// The distributor holds the whole component because it writes across all read compartments. An
// ingester does not: it only needs the watcher for its own read compartment, obtained via Watcher.
type PartitionRingWatchers struct {
	services.Service

	// watchers is indexed by read compartment ID. It always has at least one element.
	watchers []*ring.PartitionRingWatcher

	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

// NewPartitionRingWatchers creates a PartitionRingWatchers. When compartmentsEnabled is false it
// watches a single ring under the plain ringName/ringKey, behaving exactly as before compartments
// existed (numCompartments is ignored). When compartmentsEnabled is true it watches numCompartments
// rings, each keyed for its read compartment.
func NewPartitionRingWatchers(compartmentsEnabled bool, numCompartments int, ringName, ringKey string, kvClient kv.Client, logger log.Logger, reg prometheus.Registerer) (*PartitionRingWatchers, error) {
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

	subservices := make([]services.Service, len(watchers))
	for i, w := range watchers {
		subservices[i] = w
	}
	manager, err := services.NewManager(subservices...)
	if err != nil {
		return nil, errors.Wrap(err, "creating ingester partition ring watchers service manager")
	}

	w := &PartitionRingWatchers{
		watchers:           watchers,
		subservices:        manager,
		subservicesWatcher: services.NewFailureWatcher(),
	}
	w.Service = services.NewBasicService(w.starting, w.running, w.stopping).WithName("partition-rings-watcher")
	return w, nil
}

// Count returns the number of read compartment partition rings.
func (w *PartitionRingWatchers) Count() int {
	return len(w.watchers)
}

// Watcher returns the partition ring watcher for the given read compartment ID.
func (w *PartitionRingWatchers) Watcher(readCompartmentID int) *ring.PartitionRingWatcher {
	return w.watchers[readCompartmentID]
}

// PartitionRing returns the partition ring of the given read compartment ID.
func (w *PartitionRingWatchers) PartitionRing(readCompartmentID int) *ring.PartitionRing {
	return w.watchers[readCompartmentID].PartitionRing()
}

// All returns the watchers indexed by read compartment ID. The returned slice must not be modified.
func (w *PartitionRingWatchers) All() []*ring.PartitionRingWatcher {
	return w.watchers
}

func (w *PartitionRingWatchers) starting(ctx context.Context) error {
	w.subservicesWatcher.WatchManager(w.subservices)
	return services.StartManagerAndAwaitHealthy(ctx, w.subservices)
}

func (w *PartitionRingWatchers) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-w.subservicesWatcher.Chan():
		return errors.Wrap(err, "ingester partition ring watcher subservice failed")
	}
}

func (w *PartitionRingWatchers) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), w.subservices)
}
