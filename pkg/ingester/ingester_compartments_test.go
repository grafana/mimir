// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_RegistersPartitionInReadCompartmentRing(t *testing.T) {
	ctx := context.Background()

	kvStore, closer := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { require.NoError(t, closer.Close()) })

	const readCompartmentID = 1

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.InstanceID = "ingester-zone-a-0" // Maps to partition 0.
	cfg.IngesterPartitionRing.KVStore.Mock = kvStore
	cfg.Compartments.Enabled = true
	cfg.Compartments.Read.NumCompartments = 2
	cfg.ReadCompartmentID = readCompartmentID

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, nil, util_test.NewTestingLogger(t))

	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	countPartitions := func(key string) int {
		val, err := kvStore.Get(ctx, key)
		require.NoError(t, err)
		if val == nil {
			return 0
		}
		return len(val.(*ring.PartitionRingDesc).Partitions)
	}

	// The partition is registered under the read compartment's ring key.
	compartmentKey := compartments.ReadCompartmentRingKey(readCompartmentID, PartitionRingKey)
	test.Poll(t, 5*time.Second, 1, func() interface{} {
		return countPartitions(compartmentKey)
	})

	// Nothing is registered under the legacy (non-compartment) key.
	assert.Zero(t, countPartitions(PartitionRingKey))
}
