// SPDX-License-Identifier: AGPL-3.0-only

package compactor

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
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestMultitenantCompactor_Compartments_RegistersInReadCompartmentRing(t *testing.T) {
	t.Parallel()

	const readCompartmentID = 1

	ctx := context.Background()

	// No user blocks stored in the bucket.
	bucketClient := &bucket.ClientMock{}
	bucketClient.On("SupportedIterOptions").Return([]objstore.IterOptionType{objstore.Recursive, objstore.UpdatedAt})
	bucketClient.MockIter("", []string{}, nil)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg := prepareConfig(t)
	cfg.ShardingRing.Common.InstanceID = "compactor-1"
	cfg.ShardingRing.Common.InstanceAddr = "1.2.3.4"
	cfg.ShardingRing.Common.KVStore.Mock = ringStore
	cfg.Compartments = compartments.Config{Enabled: true, Read: compartments.ReadConfig{NumCompartments: 2}, Write: compartments.WriteConfig{NumCompartments: 1}}
	cfg.ReadCompartmentID = readCompartmentID

	c, _, _, _, _ := prepare(t, cfg, bucketClient)
	require.NoError(t, services.StartAndAwaitRunning(ctx, c))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, c))
	})

	countInstances := func(key string) int {
		val, err := ringStore.Get(ctx, key)
		require.NoError(t, err)
		if val == nil {
			return 0
		}
		return len(val.(*ring.Desc).Ingesters)
	}

	// The instance is registered under the read compartment's ring key.
	test.Poll(t, 5*time.Second, 1, func() interface{} {
		return countInstances(compartments.WithReadCompartmentSuffix(ringKey, readCompartmentID))
	})

	// Nothing is registered under the non-compartment key.
	assert.Zero(t, countInstances(ringKey))
}
