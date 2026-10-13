// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"flag"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type fakeMemoryLimiter struct{ reason string }

func (f *fakeMemoryLimiter) LimitingReason() string { return f.reason }

func TestReactiveLimiterConfig_Validate(t *testing.T) {
	cfg := ReactiveLimiterConfig{}
	cfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
	require.NoError(t, cfg.Validate())

	cfg.TenantFairnessEnabled = true
	require.EqualError(t, cfg.Validate(), "distributor reactive limiter tenant fairness requires the reactive limiter to be enabled")

	cfg.Enabled = true
	require.NoError(t, cfg.Validate())
}

func TestNewDistributorReactiveLimiter(t *testing.T) {
	newCfg := func() ReactiveLimiterConfig {
		cfg := ReactiveLimiterConfig{}
		cfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError))
		return cfg
	}

	t.Run("disabled", func(t *testing.T) {
		require.Nil(t, newDistributorReactiveLimiter(newCfg(), log.NewNopLogger(), prometheus.NewPedanticRegistry()))
	})

	t.Run("memory guard only", func(t *testing.T) {
		cfg := newCfg()
		cfg.MemoryUtilizationLimit = 1 << 30
		l := newDistributorReactiveLimiter(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NotNil(t, l)
		require.Nil(t, l.limiter)
		require.Len(t, l.services, 1)
		require.True(t, l.canAcquirePermit())
	})

	t.Run("tenant fairness records in-flight bytes per tenant", func(t *testing.T) {
		cfg := newCfg()
		cfg.Enabled = true
		cfg.TenantFairnessEnabled = true
		l := newDistributorReactiveLimiter(cfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NotNil(t, l.usageTracker)
		require.Len(t, l.services, 1)

		for tenant, bytes := range map[string]int64{"heavy": 10 << 20, "light": 1 << 10} {
			release, err := l.acquirePermit(t.Context(), tenant)
			require.NoError(t, err)
			release(false, bytes)
		}

		heavy, ok := l.usageTracker.GetUsage("heavy")
		require.True(t, ok)
		light, ok := l.usageTracker.GetUsage("light")
		require.True(t, ok)
		require.Greater(t, heavy, light)
	})
}

func TestDistributor_ReactiveLimiterMemoryGuard(t *testing.T) {
	var limits validation.Limits
	flagext.DefaultValues(&limits)

	ds, _, _, _ := prepare(t, prepConfig{numDistributors: 1, limits: &limits})
	d := ds[0]
	memory := &fakeMemoryLimiter{}
	d.reactiveLimiter = &distributorReactiveLimiter{memory: memory}

	ctx := user.InjectOrgID(context.Background(), "user")
	wrappedPush := d.wrapPushWithMiddlewares(func(context.Context, *Request) error { return nil })
	req := makeWriteRequestForGenerators(1, func(int) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "__name__", Value: "metric"}}
	}, nil, nil)

	require.NoError(t, wrappedPush(ctx, NewParsedRequest(req, req.Size())))

	memory.reason = "memory"
	err := wrappedPush(ctx, NewParsedRequest(req, req.Size()))
	require.ErrorIs(t, err, errReactiveLimiterMemoryUtilizationExceeded)

	_, err = d.StartPushRequest(ctx, 1024)
	require.ErrorIs(t, err, errReactiveLimiterMemoryUtilizationExceeded)

	require.Equal(t, 2.0, testutil.ToFloat64(d.rejectedRequests.WithLabelValues(reasonDistributorMemoryUtilizationLimit)))
}
