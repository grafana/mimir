// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFrozenDescCollector_StableIdentityAcrossDescChurn is the
// regression test for the partition-lifecycle metric-registry bug:
// when a partitionReg accumulates new descriptors between Register
// and Unregister, the raw *prometheus.Registry's identity in the
// outer Registry changes (collectorID is a XOR of desc IDs), so
// Unregister silently returns false and leaves the stale entry
// behind. The next Register for a *different* partitionReg whose
// initial desc set happens to match the original collectorID then
// fails with "duplicate metrics collector registration attempted".
//
// Wrapping the partitionReg in a frozenDescCollector at Register
// time pins the descriptor identity, so Unregister matches even
// after the inner registry has grown new descriptors.
func TestFrozenDescCollector_StableIdentityAcrossDescChurn(t *testing.T) {
	outer := prometheus.NewRegistry()

	// First lifecycle: simulate startKafkaReader registering an
	// initial set of reader metrics on partitionReg, wrapping it in
	// a frozenDescCollector, then registering the wrapper.
	partitionReg := prometheus.NewRegistry()
	initialMetric := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reader_records_total",
		Help: "Records consumed by the partition reader.",
		ConstLabels: prometheus.Labels{
			"reader_partition": "0",
		},
	})
	require.NoError(t, partitionReg.Register(initialMetric))

	wrapper := newFrozenDescCollector(partitionReg)
	require.NotNil(t, wrapper)
	require.NoError(t, outer.Register(wrapper))

	// Simulate the running reader adding a new metric AFTER
	// registration. This is exactly the kind of churn that breaks a
	// naive Unregister(partitionReg) call.
	lateMetric := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "reader_fetch_duration_seconds",
		Help: "Latency of partition fetches.",
		ConstLabels: prometheus.Labels{
			"reader_partition": "0",
		},
	})
	require.NoError(t, partitionReg.Register(lateMetric))

	// All metrics from partitionReg (including the late one) must
	// flow through the wrapper's Collect into a Gather on the outer
	// registry — Describe-stability shouldn't suppress live metrics.
	initialMetric.Inc()
	mfs, err := outer.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	assert.True(t, names["reader_records_total"], "outer registry should expose the initial collector's metric")
	assert.True(t, names["reader_fetch_duration_seconds"], "outer registry should expose the late-registered metric via Collect delegation")

	// Unregister via the wrapper. With the frozen-desc wrapper this
	// succeeds even though partitionReg.Describe() now emits a
	// superset of the original snapshot.
	require.True(t, outer.Unregister(wrapper),
		"Unregister(wrapper) must succeed despite inner registry having added descriptors after Register")

	// Second lifecycle: a fresh partitionReg with the SAME initial
	// descriptor set as the first one. Without the frozen-desc fix
	// this Register would fail with AlreadyRegisteredError because
	// the stale collectorID from the first lifecycle (with the same
	// snapshot) is still in the outer registry.
	partitionReg2 := prometheus.NewRegistry()
	initialMetric2 := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "reader_records_total",
		Help: "Records consumed by the partition reader.",
		ConstLabels: prometheus.Labels{
			"reader_partition": "0",
		},
	})
	require.NoError(t, partitionReg2.Register(initialMetric2))

	wrapper2 := newFrozenDescCollector(partitionReg2)
	require.NoError(t, outer.Register(wrapper2),
		"second Register for the same partition must succeed after Unregister of the first wrapper")
}

// TestFrozenDescCollector_EmptyInnerStillWorks confirms a wrapper
// over an empty inner Registry is safely registrable and
// unregistrable. Empty inner Registries become "unchecked
// collectors" in prometheus terminology, which means the outer
// Unregister doesn't actually remove them — but that's harmless
// because unchecked collectors don't generate name conflicts on the
// next Register either.
func TestFrozenDescCollector_EmptyInnerStillWorks(t *testing.T) {
	outer := prometheus.NewRegistry()
	wrapper := newFrozenDescCollector(prometheus.NewRegistry())
	require.NotNil(t, wrapper)
	require.NoError(t, outer.Register(wrapper))

	// Empty wrapper has no descs; outer registry treats it as an
	// unchecked collector. Unregister returns false in that case,
	// but a follow-up Register of a *different* empty wrapper still
	// succeeds (unchecked collectors aren't dedup'd).
	outer.Unregister(wrapper)
	require.NoError(t, outer.Register(newFrozenDescCollector(prometheus.NewRegistry())))
}

// TestFrozenDescCollector_NilInnerReturnsNil keeps the helper
// defensive: passing nil shouldn't panic and shouldn't yield a
// usable wrapper, mirroring how startKafkaReader skips the wrap
// when there is no main registerer to mount on.
func TestFrozenDescCollector_NilInnerReturnsNil(t *testing.T) {
	assert.Nil(t, newFrozenDescCollector(nil))
}
