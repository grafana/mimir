// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
)

func TestReadcacheMembershipTracker_DebouncesRemovalAndRecovery(t *testing.T) {
	tracker := newReadcacheMembershipTracker()

	assert.Equal(t, []string{"a", "b"}, tracker.observe([]string{"b", "a"}), "cold-start fleet is admitted immediately")
	assert.Equal(t, []string{"a", "b"}, tracker.observe([]string{"a"}), "first absence keeps the current member")
	assert.Equal(t, []string{"a", "b"}, tracker.observe([]string{"a", "b"}), "a recovery resets the absence streak")

	assert.Equal(t, []string{"a", "b"}, tracker.observe([]string{"a"}), "first consecutive absence still keeps the member")
	assert.Equal(t, []string{"a"}, tracker.observe([]string{"a"}), "second consecutive absence removes the member")

	assert.Equal(t, []string{"a"}, tracker.observe([]string{"a", "b"}), "first healthy observation does not readmit a removed member")
	assert.Equal(t, []string{"a", "b"}, tracker.observe([]string{"a", "b"}), "second healthy observation readmits the member")
}

func TestStabilizedReadcacheInstances_RingErrorRetainsStableSet(t *testing.T) {
	r := &Rebalancer{
		logger:              log.NewNopLogger(),
		readcacheMembership: newReadcacheMembershipTracker(),
		readcacheRing: stubReadcacheRing{set: ring.ReplicationSet{Instances: []ring.InstanceDesc{
			{Id: "a"}, {Id: "b"},
		}}},
	}

	assert.Equal(t, []string{"a", "b"}, r.stabilizedReadcacheInstances())

	r.readcacheRing = stubReadcacheRing{err: errors.New("kv unavailable")}
	assert.Equal(t, []string{"a", "b"}, r.stabilizedReadcacheInstances(), "lookup failure must not count as every member missing")
}
