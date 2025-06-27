// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util/validation"
)

type mockDistributorOptions struct {
	limits *validation.Overrides
}

type mockDistributorOption func(*mockDistributorOptions)

func mockDistributorWithLimits(limits *validation.Overrides) mockDistributorOption {
	return func(o *mockDistributorOptions) {
		o.limits = limits
	}
}

func newMockDistributor(t *testing.T, setOptions ...mockDistributorOption) *Distributor {
	distributorCfg := Config{}
	clientCfg := client.Config{}
	flagext.DefaultValues(&distributorCfg, &clientCfg)

	kvStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })
	ingestersRing, err := ring.New(ring.Config{
		KVStore:           kv.Config{Mock: kvStore},
		HeartbeatTimeout:  60 * time.Minute,
		ReplicationFactor: 1,
	}, ingester.IngesterRingKey, ingester.IngesterRingKey, log.NewNopLogger(), nil)
	require.NoError(t, err)

	var options mockDistributorOptions
	for _, setOption := range setOptions {
		setOption(&options)
	}
	limits := options.limits
	if limits == nil {
		limitsCfg := validation.Limits{}
		flagext.DefaultValues(&limitsCfg)
		limits = validation.NewOverrides(
			limitsCfg,
			validation.NewMockTenantLimits(map[string]*validation.Limits{}),
		)
	}

	distributor, err := New(distributorCfg, clientCfg, limits, nil, nil, ingestersRing, nil, false, nil, log.NewNopLogger())
	require.NoError(t, err)
	return distributor
}
