// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/exporter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package exporter

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOverridesExporter_noConfig(t *testing.T) {
	exporter, err := NewOverridesExporter(Config{}, &validation.Limits{}, nil, log.NewNopLogger(), nil)
	require.NoError(t, err)

	// With no updated override configurations, there should be no override metrics
	count := testutil.CollectAndCount(exporter, "cortex_limits_overrides")
	assert.Equal(t, 0, count)

	// The defaults should exist though
	count = testutil.CollectAndCount(exporter, "cortex_limits_defaults")
	assert.Equal(t, 10, count)
}

func TestOverridesExporter_emptyConfig(t *testing.T) {
	exporter, err := NewOverridesExporter(Config{}, &validation.Limits{}, validation.NewMockTenantLimits(nil), log.NewNopLogger(), nil)
	require.NoError(t, err)

	// With no updated override configurations, there should be no override metrics
	count := testutil.CollectAndCount(exporter, "cortex_limits_overrides")
	assert.Equal(t, 0, count)

	// The defaults should exist though
	count = testutil.CollectAndCount(exporter, "cortex_limits_defaults")
	assert.Equal(t, 10, count)
}

func TestOverridesExporter_withConfig(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {
			IngestionRate:                10,
			IngestionBurstSize:           11,
			MaxGlobalSeriesPerUser:       12,
			MaxGlobalSeriesPerMetric:     13,
			MaxGlobalExemplarsPerUser:    14,
			MaxChunksPerQuery:            15,
			MaxFetchedSeriesPerQuery:     16,
			MaxFetchedChunkBytesPerQuery: 17,
			RulerMaxRulesPerRuleGroup:    19,
			RulerMaxRuleGroupsPerTenant:  20,
		},
	}

	exporter, err := NewOverridesExporter(Config{}, &validation.Limits{
		IngestionRate:                22,
		IngestionBurstSize:           23,
		MaxGlobalSeriesPerUser:       24,
		MaxGlobalSeriesPerMetric:     25,
		MaxGlobalExemplarsPerUser:    26,
		MaxChunksPerQuery:            27,
		MaxFetchedSeriesPerQuery:     28,
		MaxFetchedChunkBytesPerQuery: 29,
		RulerMaxRulesPerRuleGroup:    31,
		RulerMaxRuleGroupsPerTenant:  32,
	}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	require.NoError(t, err)
	limitsMetrics := `
# HELP cortex_limits_overrides Resource limit overrides applied to tenants
# TYPE cortex_limits_overrides gauge
cortex_limits_overrides{limit_name="ingestion_rate",user="tenant-a"} 10
cortex_limits_overrides{limit_name="ingestion_burst_size",user="tenant-a"} 11
cortex_limits_overrides{limit_name="max_global_series_per_user",user="tenant-a"} 12
cortex_limits_overrides{limit_name="max_global_series_per_metric",user="tenant-a"} 13
cortex_limits_overrides{limit_name="max_global_exemplars_per_user",user="tenant-a"} 14
cortex_limits_overrides{limit_name="max_fetched_chunks_per_query",user="tenant-a"} 15
cortex_limits_overrides{limit_name="max_fetched_series_per_query",user="tenant-a"} 16
cortex_limits_overrides{limit_name="max_fetched_chunk_bytes_per_query",user="tenant-a"} 17
cortex_limits_overrides{limit_name="ruler_max_rules_per_rule_group",user="tenant-a"} 19
cortex_limits_overrides{limit_name="ruler_max_rule_groups_per_tenant",user="tenant-a"} 20
`

	// Make sure each override matches the values from the supplied `Limit`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_overrides")
	assert.NoError(t, err)

	limitsMetrics = `
# HELP cortex_limits_defaults Resource limit defaults for tenants without overrides
# TYPE cortex_limits_defaults gauge
cortex_limits_defaults{limit_name="ingestion_rate"} 22
cortex_limits_defaults{limit_name="ingestion_burst_size"} 23
cortex_limits_defaults{limit_name="max_global_series_per_user"} 24
cortex_limits_defaults{limit_name="max_global_series_per_metric"} 25
cortex_limits_defaults{limit_name="max_global_exemplars_per_user"} 26
cortex_limits_defaults{limit_name="max_fetched_chunks_per_query"} 27
cortex_limits_defaults{limit_name="max_fetched_series_per_query"} 28
cortex_limits_defaults{limit_name="max_fetched_chunk_bytes_per_query"} 29
cortex_limits_defaults{limit_name="ruler_max_rules_per_rule_group"} 31
cortex_limits_defaults{limit_name="ruler_max_rule_groups_per_tenant"} 32
`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_defaults")
	assert.NoError(t, err)
}

func TestOverridesExporter_withRing(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {},
	}

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := Config{RingConfig{Enabled: true}}
	cfg.Ring.KVStore.Mock = ringStore
	cfg.Ring.InstanceID = "overrides-exporter"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.InstancePort = 1234

	// Create an empty ring.
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	exporter, err := NewOverridesExporter(cfg, &validation.Limits{}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	l := exporter.ring.lifecycler
	r := exporter.ring.client
	require.NoError(t, err)
	require.NoError(t, services.StartManagerAndAwaitHealthy(ctx, exporter.subserviceManager))
	t.Cleanup(func() { require.NoError(t, services.StopManagerAndAwaitStopped(ctx, exporter.subserviceManager)) })

	// Register this instance in the ring.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester(
			cfg.Ring.InstanceID,
			fmt.Sprintf("%s:%d", cfg.Ring.InstanceAddr, cfg.Ring.InstancePort),
			"",
			[]uint32{1},
			ring.ACTIVE,
			time.Now(),
		)
		return desc, true, nil
	}))

	// Wait until the ring client observes the ring update.
	test.Poll(t, time.Second, []string{l.GetInstanceAddr()}, func() interface{} {
		rs, _ := r.GetAllHealthy(ringOp)
		return rs.GetAddresses()
	})

	// This instance is now the only ring member (and therefore leader), metrics should be exported.
	require.True(t, hasOverrideMetrics(exporter))

	// Register a different instance.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester("other-instance", "2.3.4.5:6789", "", []uint32{2}, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	// Unregister the original instance.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.RemoveIngester("overrides-exporter")
		return desc, true, nil
	}))

	// Wait until the ring client observes the ring update.
	test.Poll(t, time.Second, []string{"2.3.4.5:6789"}, func() interface{} {
		rs, _ := r.GetAllHealthy(ringOp)
		return rs.GetAddresses()
	})

	// This instance is now no longer the leader, no metrics should be exported.
	require.False(t, hasOverrideMetrics(exporter))

	// Unregister the last remaining instance.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.RemoveIngester("other-instance")
		return desc, true, nil
	}))

	// Wait until the ring client observes the ring update.
	test.Poll(t, time.Second, []string{}, func() interface{} {
		rs, _ := exporter.ring.client.GetAllHealthy(ringOp)
		return rs.GetAddresses()
	})

	// The ring is now empty, this instance should swallow the "empty ring" error and
	// export overrides. Theoretically, the lifecycler could kick in and register the
	// instance back into the ring, but since this test is using the default
	// heartbeat period of 15 seconds, this should not be a concern in practice.
	require.True(t, hasOverrideMetrics(exporter))
}

// TestOverridesExporterRollout tests that metric duplicates are not exported on
// rollout or scale-down of replicas running the overrides-exporter component.
func TestOverridesExporter_scaleDown(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {},
	}

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create an empty ring.
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	cfg := Config{RingConfig{Enabled: true}}
	cfg.Ring.KVStore.Mock = ringStore
	cfg.Ring.InstanceID = "overrides-exporter-new-replica"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.InstancePort = 1234
	cfg.Ring.HeartbeatTimeout = 15 * time.Second

	exporter, err := NewOverridesExporter(cfg, &validation.Limits{}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	l := exporter.ring.lifecycler
	r := exporter.ring.client
	require.NoError(t, err)
	require.NoError(t, services.StartManagerAndAwaitHealthy(ctx, exporter.subserviceManager))
	t.Cleanup(func() { require.NoError(t, services.StopManagerAndAwaitStopped(ctx, exporter.subserviceManager)) })

	// Register some stable instances that have been in the ring for some time.
	// instance-1 was the leader but is about to leave the ring.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester("instance-1", "127.0.0.1", "", []uint32{leaderToken + 1}, ring.ACTIVE, time.Now().Add(-1*time.Hour))
		desc.AddIngester(l.GetInstanceID(), l.GetInstanceAddr(), "", []uint32{leaderToken + 2}, ring.ACTIVE, time.Now().Add(-1*time.Hour))
		return desc, true, nil
	}))

	// Wait until it has received the ring update.
	test.Poll(t, time.Second, 2, func() interface{} {
		rs, _ := r.GetAllHealthy(ringOp)
		return len(rs.Instances)
	})

	// No metrics should be exported because the old leader is still present in the ring.
	require.False(t, hasOverrideMetrics(exporter))

	// Remove previous leader replica
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.RemoveIngester("instance-1")
		return desc, false, nil
	}))

	// Wait until update has been received by the ring client.
	test.Poll(t, 1*time.Second, 1, func() interface{} {
		rs, _ := r.GetAllHealthy(ringOp)
		return len(rs.Instances)
	})

	require.True(t, hasOverrideMetrics(exporter))
}

func TestOverridesExporter_scaleUp(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {},
	}

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Create an empty ring.
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	// Register an instance that is currently the leader.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester("instance-1", "127.0.0.1", "", []uint32{leaderToken + 2}, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	cfg := Config{RingConfig{Enabled: true}}
	cfg.Ring.KVStore.Mock = ringStore
	cfg.Ring.InstanceID = "new-leader"
	cfg.Ring.InstanceAddr = "1.2.3.4"
	cfg.Ring.InstancePort = 1234
	cfg.Ring.HeartbeatTimeout = 15 * time.Second

	exporter, err := NewOverridesExporter(cfg, &validation.Limits{}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	l := exporter.ring.lifecycler
	r := exporter.ring.client
	require.NoError(t, err)
	require.NoError(t, services.StartManagerAndAwaitHealthy(ctx, exporter.subserviceManager))
	t.Cleanup(func() { require.NoError(t, services.StopManagerAndAwaitStopped(ctx, exporter.subserviceManager)) })

	// Register a new replica to the ring with a token that would make it the leader.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		desc.AddIngester(l.GetInstanceID(), l.GetInstanceAddr(), "", []uint32{leaderToken + 1}, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	// Wait until the new instance has received the ring update.
	test.Poll(t, time.Second, 2, func() interface{} {
		rs, _ := r.GetAllHealthy(ringOp)
		return len(rs.Instances)
	})

	// The new replica owns the `leaderToken` token and is about to become leader,
	// but it should back off for now to give the previous leader time to discover
	// that a new leader is present in the ring.
	require.False(t, hasOverrideMetrics(exporter))

	// Set the registered timestamp of the new instance to the past to pretend the
	// instance has been around for longer.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		instance := desc.Ingesters[l.GetInstanceID()]
		instance.RegisteredTimestamp = time.Now().Add(-5 * cfg.Ring.HeartbeatTimeout).Unix()
		desc.Ingesters[l.GetInstanceID()] = instance
		// used only to have an easy target to poll against
		desc.AddIngester("poll-me", "", "", nil, ring.ACTIVE, time.Now())
		return desc, true, nil
	}))

	// Wait until the new instance has received the ring update.
	test.Poll(t, time.Second, true, func() interface{} {
		return r.HasInstance("poll-me")
	})

	// After the wait time has passed, the new leader should become active and export metrics.
	require.True(t, hasOverrideMetrics(exporter))
}

func hasOverrideMetrics(exporter *OverridesExporter) bool {
	count := testutil.CollectAndCount(exporter, "cortex_limits_overrides")
	return count > 0
}
