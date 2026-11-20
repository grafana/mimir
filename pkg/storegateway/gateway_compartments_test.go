// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestStoreGateway_Compartments_RegistersInReadCompartmentRing(t *testing.T) {
	test.VerifyNoLeak(t)

	ctx := context.Background()

	const readCompartmentID = 1

	gatewayCfg := mockGatewayConfig()
	gatewayCfg.Compartments = compartments.Config{Enabled: true, Read: compartments.ReadConfig{NumCompartments: 2}, Write: compartments.WriteConfig{NumCompartments: 1}}
	gatewayCfg.ReadCompartmentID = readCompartmentID

	storageCfg := mockStorageConfig(t)

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	g, err := newStoreGateway(gatewayCfg, storageCfg, newSingleBlocksBucketSet(t, objstore.NewInMemBucket()), ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, g))
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
	assert.Equal(t, 1, countInstances(compartments.WithReadCompartmentSuffix(RingKey, readCompartmentID)))

	// Nothing is registered under the non-compartment key.
	assert.Zero(t, countInstances(RingKey))
}

func TestNewStoreGateway_Compartments_UsesOnlyTheAdditionalReadBucketsOfItsReadCompartment(t *testing.T) {
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.Compartments = compartments.Config{Enabled: true, Read: compartments.ReadConfig{NumCompartments: 2}, Write: compartments.WriteConfig{NumCompartments: 1}}
	gatewayCfg.ReadCompartmentID = 1

	storageCfg := mockStorageConfig(t)
	writeBucketName := storageCfg.Bucket.BucketName()
	// Both entries are invalid in different ways, so the error tells which entry the store-gateway uses.
	storageCfg.AdditionalReadBuckets.BucketNames = []string{"0:", "1:" + writeBucketName}

	_, err := NewStoreGateway(gatewayCfg, storageCfg, defaultLimitsOverrides(t), log.NewNopLogger(), nil, nil)
	require.ErrorContains(t, err, fmt.Sprintf("the blocks bucket name %q is used more than once", writeBucketName))
}
