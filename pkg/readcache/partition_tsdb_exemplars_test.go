// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestEffectiveMaxExemplars(t *testing.T) {
	overrides := func(globalLimit int) *validation.Overrides {
		return validation.NewOverrides(validation.Limits{MaxGlobalExemplarsPerUser: globalLimit}, nil)
	}

	for name, tc := range map[string]struct {
		limits   *validation.Overrides
		cap      int
		expected int64
	}{
		"nil limits":                   {limits: nil, cap: 100, expected: 0},
		"global below cap passes":      {limits: overrides(50), cap: 100, expected: 50},
		"global above cap is clamped":  {limits: overrides(100_000_000), cap: 100_000, expected: 100_000},
		"zero cap disables clamping":   {limits: overrides(100_000_000), cap: 0, expected: 100_000_000},
		"disabled global stays 0":      {limits: overrides(0), cap: 100, expected: 0},
		"negative global clamped to 0": {limits: overrides(-1), cap: 100, expected: 0},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, effectiveMaxExemplars(tc.limits, "tenant-1", tc.cap))
		})
	}
}

// TestPartitionTSDB_MaxExemplarsCap verifies the per-TSDB exemplar cap
// is applied both at open time and through the periodic
// applyTenantTSDBSettings reapply.
//
// Regression test for the dev-15 GC spiral: every per-(tenant,
// partition) TSDB preallocated the tenant's full GLOBAL exemplar limit
// (Prometheus's circular exemplar storage allocates its entire ring
// upfront), so a 100M-limit tenant with 10 open TSDBs held ~52 GiB of
// permanently-live heap for a handful of actual exemplars, pinning
// ~30 cores in GC mark work.
func TestPartitionTSDB_MaxExemplarsCap(t *testing.T) {
	const (
		globalLimit = 1_000_000
		perTSDBCap  = 1_000
	)

	cfg := newTestConfig(t, false, 0)
	limits := validation.NewOverrides(validation.Limits{MaxGlobalExemplarsPerUser: globalLimit}, nil)

	reg := prometheus.NewRegistry()
	p, err := openPartitionTSDB(
		"tenant-1",
		0,
		0, // epoch
		cfg.DataDir,
		cfg.BlocksStorage.TSDB,
		cfg.LocalBlockRetention,
		limits,
		perTSDBCap,
		nil, nil, nil,
		reg,
		log.NewNopLogger(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = p.Close() })

	maxExemplarsGauge := func() float64 {
		families, err := reg.Gather()
		require.NoError(t, err)
		for _, mf := range families {
			if mf.GetName() == "prometheus_tsdb_exemplar_max_exemplars" {
				require.Len(t, mf.GetMetric(), 1)
				return mf.GetMetric()[0].GetGauge().GetValue()
			}
		}
		t.Fatal("prometheus_tsdb_exemplar_max_exemplars not found")
		return 0
	}

	require.Equal(t, float64(perTSDBCap), maxExemplarsGauge())

	// The periodic reapply must clamp with the same cap, or it would
	// resize the ring back up to the global limit on its next tick.
	require.NoError(t, p.applyTenantTSDBSettings(limits, perTSDBCap, log.NewNopLogger()))
	require.Equal(t, float64(perTSDBCap), maxExemplarsGauge())
}
