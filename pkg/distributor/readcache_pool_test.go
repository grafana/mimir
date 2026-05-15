// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubReadcacheRing struct {
	set ring.ReplicationSet
	err error
}

func (s stubReadcacheRing) GetAllHealthy(_ ring.Operation) (ring.ReplicationSet, error) {
	return s.set, s.err
}

// TestReadcachePool_ResolveAddr_StaticOverridesRing covers the
// operator escape hatch: an instance listed in
// -distributor.readcache.addresses is dialed at the configured
// host:port even if the ring reports something else. This lets
// operators pin specific dial targets (e.g. to bypass a misconfigured
// ring entry) without taking the whole ring out of the picture.
func TestReadcachePool_ResolveAddr_StaticOverridesRing(t *testing.T) {
	cfg := ReadcacheConfig{Addresses: "readcache-1=overridden:1234"}
	p, err := newReadcachePool(cfg, stubReadcacheRing{set: ring.ReplicationSet{Instances: []ring.InstanceDesc{
		{Id: "readcache-1", Addr: "10.0.0.7:9095"},
	}}}, log.NewNopLogger())
	require.NoError(t, err)

	addr, err := p.resolveAddr("readcache-1")
	require.NoError(t, err)
	assert.Equal(t, "overridden:1234", addr,
		"static address must win over the ring entry for the same instance ID")
}

// TestReadcachePool_ResolveAddr_RingLookupSucceeds is the
// production path: no static map, address comes from the ring.
func TestReadcachePool_ResolveAddr_RingLookupSucceeds(t *testing.T) {
	p, err := newReadcachePool(ReadcacheConfig{}, stubReadcacheRing{set: ring.ReplicationSet{Instances: []ring.InstanceDesc{
		{Id: "readcache-1", Addr: "10.0.0.7:9095"},
		{Id: "readcache-2", Addr: "10.0.0.8:9095"},
	}}}, log.NewNopLogger())
	require.NoError(t, err)

	addr, err := p.resolveAddr("readcache-2")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.8:9095", addr)
}

// TestReadcachePool_ResolveAddr_UnknownInstance ensures unknown IDs
// surface a structured error rather than silently dialing a wrong
// target. The slicer can issue leases that name an instance the
// distributor briefly hasn't seen yet via the ring; the
// caller-side fallback (resolveReadcacheClientForPartition ->
// ingester pool) handles that case by treating ok=false as "fall
// back to ingester".
func TestReadcachePool_ResolveAddr_UnknownInstance(t *testing.T) {
	p, err := newReadcachePool(ReadcacheConfig{}, stubReadcacheRing{set: ring.ReplicationSet{Instances: []ring.InstanceDesc{
		{Id: "readcache-1", Addr: "10.0.0.7:9095"},
	}}}, log.NewNopLogger())
	require.NoError(t, err)

	_, err = p.resolveAddr("readcache-missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in ring")
}

// TestReadcachePool_ResolveAddr_RingErrorBubbles checks that a KV
// outage surfaces a usable error. The caller logs it and falls back
// to the ingester pool, so a partial readcache-ring outage degrades
// to "served by ingester" rather than "no client at all".
func TestReadcachePool_ResolveAddr_RingErrorBubbles(t *testing.T) {
	p, err := newReadcachePool(ReadcacheConfig{}, stubReadcacheRing{err: errors.New("kv unavailable")}, log.NewNopLogger())
	require.NoError(t, err)

	_, err = p.resolveAddr("readcache-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kv unavailable")
}

// TestNewReadcachePool_RejectsEmptyConfig guards the invariant that
// a pool with neither a ring nor a static map has nowhere to look up
// instance addresses; the distributor must catch this at startup
// rather than producing "no configured address" at query time.
func TestNewReadcachePool_RejectsEmptyConfig(t *testing.T) {
	_, err := newReadcachePool(ReadcacheConfig{}, nil, log.NewNopLogger())
	require.Error(t, err)
}
