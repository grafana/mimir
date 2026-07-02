// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"time"

	"github.com/grafana/dskit/ring"
)

// PartitionInstanceRings holds one ring.PartitionInstanceRing per read compartment (a single one when
// compartments are disabled). Each pairs a read compartment's partition ring with the shared ingester
// instance ring: this is correct because the partition ring desc already scopes partition ownership to
// the compartment's ingesters, and the instance ring is only used to resolve those owners' descriptors.
type PartitionInstanceRings struct {
	// rings is indexed by read compartment ID. It always has at least one element.
	rings []*ring.PartitionInstanceRing
}

// NewPartitionInstanceRings builds one PartitionInstanceRing per read compartment, pairing each
// compartment's partition ring (from the watcher) with the shared ingester instance ring.
func NewPartitionInstanceRings(watcher *PartitionRingWatchers, instanceRing ring.InstanceRingReader, heartbeatTimeout time.Duration) *PartitionInstanceRings {
	rings := make([]*ring.PartitionInstanceRing, watcher.Count())
	for c := range rings {
		rings[c] = ring.NewPartitionInstanceRing(watcher.Watcher(c), instanceRing, heartbeatTimeout)
	}
	return &PartitionInstanceRings{rings: rings}
}

// Count returns the number of read compartment partition-instance rings.
func (r *PartitionInstanceRings) Count() int {
	return len(r.rings)
}

// Get returns the partition-instance ring of the given read compartment ID.
func (r *PartitionInstanceRings) Get(readCompartmentID int) *ring.PartitionInstanceRing {
	return r.rings[readCompartmentID]
}
