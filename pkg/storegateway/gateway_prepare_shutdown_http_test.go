package storegateway

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/shutdownmarker"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/util/test"
)

func createStoreGateway(t *testing.T, reg prometheus.Registerer) *StoreGateway {
	gatewayCfg := mockGatewayConfig()
	gatewayCfg.ShardingRing.UnregisterOnShutdown = false

	storageDir := t.TempDir()
	storageCfg := mockStorageConfig(t)
	storageCfg.TSDB.Dir = storageDir

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: storageDir})
	require.NoError(t, err)

	g, err := newStoreGateway(gatewayCfg, storageCfg, bucket, ringStore, defaultLimitsOverrides(t), log.NewNopLogger(), reg, nil)
	require.NoError(t, err)
	return g
}

func TestGatewayPrepareShutdownHttp_PrepareShutdownHandler(t *testing.T) {
	test.VerifyNoLeak(t)
	reg := prometheus.NewPedanticRegistry()
	g := createStoreGateway(t, reg)

	shutdownMarkerPath := shutdownmarker.GetPath(g.storageCfg.TSDB.Dir)
	// ensure that there is no shutdown marker
	exists, err := shutdownmarker.Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.False(t, exists)

	// Start the store-gateway.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	// ensure that unregistering is NOT required
	require.True(t, g.ringLifecycler.ShouldKeepInstanceInTheRingOnShutdown())

	// after GET is invoked, the expected result is "unset"
	response1 := httptest.NewRecorder()
	g.PrepareShutdownHandler(response1, httptest.NewRequest("GET", "/store-gateway/prepare-shutdown", nil))
	require.Equal(t, "unset", response1.Body.String())
	require.Equal(t, 200, response1.Code)
	exists, err = shutdownmarker.Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.False(t, exists)

	// after POST is invoked, it is required that cortex_storegateway_prepare_shutdown_requested gets incremented
	// and that there exists a shutdown marker
	response2 := httptest.NewRecorder()
	g.PrepareShutdownHandler(response2, httptest.NewRequest("POST", "/store-gateway/prepare-shutdown", nil))
	require.Equal(t, 204, response2.Code)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_storegateway_prepare_shutdown_requested If the store-gateway has been requested to prepare for shutdown via endpoint or marker file.
					# TYPE cortex_storegateway_prepare_shutdown_requested gauge
					cortex_storegateway_prepare_shutdown_requested 1
				`), "cortex_storegateway_prepare_shutdown_requested"))

	exists, err = shutdownmarker.Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.True(t, exists)
	// ensure that unregistering is required
	require.False(t, g.ringLifecycler.ShouldKeepInstanceInTheRingOnShutdown())

	// after GET is invoked, the expected result is now "set"
	response3 := httptest.NewRecorder()
	g.PrepareShutdownHandler(response3, httptest.NewRequest("GET", "/store-gateway/prepare-shutdown", nil))
	require.Equal(t, "set", response3.Body.String())
	require.Equal(t, 200, response3.Code)

	// after DELETE is invoked, the effects of POST get deleted
	response4 := httptest.NewRecorder()
	g.PrepareShutdownHandler(response4, httptest.NewRequest("DELETE", "/store-gateway/prepare-shutdown", nil))
	require.Equal(t, 204, response4.Code)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_storegateway_prepare_shutdown_requested If the store-gateway has been requested to prepare for shutdown via endpoint or marker file.
					# TYPE cortex_storegateway_prepare_shutdown_requested gauge
					cortex_storegateway_prepare_shutdown_requested 0
				`), "cortex_storegateway_prepare_shutdown_requested"))
	exists, err = shutdownmarker.Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.False(t, exists)
	// ensure that unregistering is NOT required
	require.True(t, g.ringLifecycler.ShouldKeepInstanceInTheRingOnShutdown())

	// after POST is invoked, and store-gateway is stopped, it is required that it gets removed from the ring
	response5 := httptest.NewRecorder()
	g.PrepareShutdownHandler(response5, httptest.NewRequest("POST", "/store-gateway/prepare-shutdown", nil))
	require.Equal(t, 204, response5.Code)

	// Stop the store-gateway
	require.True(t, g.ringLifecycler.IsRegistered())
	require.Equal(t, ring.ACTIVE, g.ringLifecycler.GetState())
	require.NoError(t, services.StopAndAwaitTerminated(ctx, g))
	require.False(t, g.ringLifecycler.IsRegistered())
	require.NotEqual(t, ring.ACTIVE, g.ringLifecycler.GetState())
}

func TestGatewayPrepareShutdownHttp_RePrepareShutdownHandler(t *testing.T) {
	test.VerifyNoLeak(t)
	reg := prometheus.NewPedanticRegistry()
	g := createStoreGateway(t, reg)

	// create a shutdown marker
	shutdownMarkerPath := shutdownmarker.GetPath(g.storageCfg.TSDB.Dir)
	err := shutdownmarker.Create(shutdownMarkerPath)
	require.Nil(t, err)
	// ensure that there is s shutdown marker
	exists, err := shutdownmarker.Exists(shutdownMarkerPath)
	require.Nil(t, err)
	require.True(t, exists)

	// Start the store-gateway.
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, g))
	// since the shutdown marker is present, ensure that unregistering is required
	require.False(t, g.ringLifecycler.ShouldKeepInstanceInTheRingOnShutdown())

	// after GET is invoked, the expected result is "set"
	response1 := httptest.NewRecorder()
	g.PrepareShutdownHandler(response1, httptest.NewRequest("GET", "/store-gateway/prepare-shutdown", nil))
	require.Equal(t, "set", response1.Body.String())
	require.Equal(t, 200, response1.Code)

	// ensure that cortex_storegateway_prepare_shutdown_requested is 1
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_storegateway_prepare_shutdown_requested If the store-gateway has been requested to prepare for shutdown via endpoint or marker file.
					# TYPE cortex_storegateway_prepare_shutdown_requested gauge
					cortex_storegateway_prepare_shutdown_requested 1
				`), "cortex_storegateway_prepare_shutdown_requested"))

	// Stop the store-gateway
	require.True(t, g.ringLifecycler.IsRegistered())
	require.Equal(t, ring.ACTIVE, g.ringLifecycler.GetState())
	require.NoError(t, services.StopAndAwaitTerminated(ctx, g))
	require.False(t, g.ringLifecycler.IsRegistered())
	require.NotEqual(t, ring.ACTIVE, g.ringLifecycler.GetState())
}
