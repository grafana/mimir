// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
)

// stubReadcacheRing implements the minimal ring surface
// activeReadcacheInstances depends on.
type stubReadcacheRing struct {
	set ring.ReplicationSet
	err error
}

func (s stubReadcacheRing) GetAllHealthy(_ ring.Operation) (ring.ReplicationSet, error) {
	return s.set, s.err
}

// TestActiveReadcacheInstances_StaticListWins verifies the operator
// escape hatch: when ReadcacheSlicer.Instances is non-empty it
// overrides the ring entirely. This is the lever an operator uses to
// drain a pod without depending on ring-edit endpoints.
func TestActiveReadcacheInstances_StaticListWins(t *testing.T) {
	r := &Rebalancer{
		logger: log.NewNopLogger(),
		readcacheRing: stubReadcacheRing{set: ring.ReplicationSet{Instances: []ring.InstanceDesc{
			{Id: "ring-only-a"}, {Id: "ring-only-b"},
		}}},
	}
	r.cfg.ReadcacheSlicer.Instances = flagext.StringSliceCSV{"pinned-1", "pinned-2"}

	got := r.activeReadcacheInstances()
	assert.Equal(t, []string{"pinned-1", "pinned-2"}, got,
		"static Instances must override ring-based discovery")
}

// TestActiveReadcacheInstances_RingFallback exercises the production
// path: with no static list, the slicer enumerates the ring.
func TestActiveReadcacheInstances_RingFallback(t *testing.T) {
	r := &Rebalancer{
		logger: log.NewNopLogger(),
		readcacheRing: stubReadcacheRing{set: ring.ReplicationSet{Instances: []ring.InstanceDesc{
			{Id: "readcache-b"}, {Id: "readcache-a"}, // unsorted on purpose
		}}},
	}
	got := r.activeReadcacheInstances()
	// The helper sorts so downstream slicer behaviour is
	// deterministic regardless of ring traversal order.
	assert.Equal(t, []string{"readcache-a", "readcache-b"}, got)
}

// TestActiveReadcacheInstances_NoSourcesReturnsNil covers the
// degraded path: no ring wired and no static list. activeReadcacheInstances
// must return nil so the caller skips the slicer round entirely;
// running with an empty instance set would assign every partition to
// "" and break the readcache log invariant.
func TestActiveReadcacheInstances_NoSourcesReturnsNil(t *testing.T) {
	r := &Rebalancer{logger: log.NewNopLogger()}
	assert.Nil(t, r.activeReadcacheInstances())
}

// TestActiveReadcacheInstances_RingErrorReturnsNil checks that a
// transient ring lookup failure (KV outage) is logged but doesn't
// crash the rebalancer; the slicer simply skips this round and
// retries on the next one once the KV is healthy again.
func TestActiveReadcacheInstances_RingErrorReturnsNil(t *testing.T) {
	r := &Rebalancer{
		logger:        log.NewNopLogger(),
		readcacheRing: stubReadcacheRing{err: errors.New("kv unavailable")},
	}
	assert.Nil(t, r.activeReadcacheInstances())
}
