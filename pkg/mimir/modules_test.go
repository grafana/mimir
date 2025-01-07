// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/modules_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/server"
	hashivault "github.com/hashicorp/vault/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/vault"
)

func changeTargetConfig(c *Config) {
	c.Target = []string{"all", "ruler"}
}

func TestAPIConfig(t *testing.T) {
	actualCfg := newDefaultConfig()

	mimir := &Mimir{
		Server: &server.Server{Registerer: prometheus.NewPedanticRegistry()},
	}

	for _, tc := range []struct {
		name               string
		path               string
		actualCfg          func(*Config)
		expectedStatusCode int
		expectedBody       func(*testing.T, string)
	}{
		{
			name:               "running with default config",
			path:               "/config",
			expectedStatusCode: 200,
		},
		{
			name:               "defaults with default config",
			path:               "/config?mode=defaults",
			expectedStatusCode: 200,
		},
		{
			name:               "diff with default config",
			path:               "/config?mode=diff",
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Equal(t, "{}\n", body)
			},
		},
		{
			name:               "running with changed target config",
			path:               "/config",
			actualCfg:          changeTargetConfig,
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Contains(t, body, "target: all,ruler\n")
			},
		},
		{
			name:               "defaults with changed target config",
			path:               "/config?mode=defaults",
			actualCfg:          changeTargetConfig,
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Contains(t, body, "target: all\n")
			},
		},
		{
			name:               "diff with changed target config",
			path:               "/config?mode=diff",
			actualCfg:          changeTargetConfig,
			expectedStatusCode: 200,
			expectedBody: func(t *testing.T, body string) {
				assert.Equal(t, "target: all,ruler\n", body)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mimir.Server.HTTP = mux.NewRouter()

			mimir.Cfg = *actualCfg
			if tc.actualCfg != nil {
				tc.actualCfg(&mimir.Cfg)
			}

			_, err := mimir.initAPI()
			require.NoError(t, err)

			req := httptest.NewRequest("GET", tc.path, nil)
			resp := httptest.NewRecorder()

			mimir.Server.HTTP.ServeHTTP(resp, req)

			assert.Equal(t, tc.expectedStatusCode, resp.Code)

			if tc.expectedBody != nil {
				tc.expectedBody(t, resp.Body.String())
			}
		})
	}
}

func TestMimir_InitRulerStorage(t *testing.T) {
	tests := map[string]struct {
		config       *Config
		expectedInit bool
	}{
		"should init the ruler storage with target=ruler": {
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"ruler"}
				cfg.RulerStorage.Backend = "local"
				cfg.RulerStorage.Local.Directory = os.TempDir()
				return cfg
			}(),
			expectedInit: true,
		},
		"should not init the ruler storage on default config with target=all": {
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"all"}
				return cfg
			}(),
			expectedInit: false,
		},
		"should init the ruler storage on ruler storage config with target=all": {
			config: func() *Config {
				cfg := newDefaultConfig()
				cfg.Target = []string{"all"}
				cfg.RulerStorage.Backend = "local"
				cfg.RulerStorage.Local.Directory = os.TempDir()
				return cfg
			}(),
			expectedInit: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			mimir := &Mimir{
				Server: &server.Server{Registerer: prometheus.NewPedanticRegistry()},
				Cfg:    *testData.config,
			}

			_, err := mimir.initRulerStorage()
			require.NoError(t, err)

			if testData.expectedInit {
				assert.NotNil(t, mimir.RulerStorage)
			} else {
				assert.Nil(t, mimir.RulerStorage)
			}
		})
	}
}

func TestMultiKVSetup(t *testing.T) {
	dir := t.TempDir()

	for target, checkFn := range map[string]func(t *testing.T, c Config){
		All: func(t *testing.T, c Config) {
			require.NotNil(t, c.Distributor.DistributorRing.Common.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ingester.IngesterRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ingester.IngesterPartitionRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.StoreGateway.ShardingRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Compactor.ShardingRing.Common.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ruler.Ring.Common.KVStore.Multi.ConfigProvider)
		},

		Ruler: func(t *testing.T, c Config) {
			require.NotNil(t, c.Ingester.IngesterRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ingester.IngesterPartitionRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.StoreGateway.ShardingRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ruler.Ring.Common.KVStore.Multi.ConfigProvider)
		},

		AlertManager: func(t *testing.T, c Config) {
			require.NotNil(t, c.Alertmanager.ShardingRing.Common.KVStore.Multi.ConfigProvider)
		},

		Distributor: func(t *testing.T, c Config) {
			require.NotNil(t, c.Distributor.DistributorRing.Common.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Distributor.HATrackerConfig.KVStore.MemberlistKV)
			require.NotNil(t, c.Ingester.IngesterRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ingester.IngesterPartitionRing.KVStore.Multi.ConfigProvider)
		},

		Ingester: func(t *testing.T, c Config) {
			require.NotNil(t, c.Ingester.IngesterRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ingester.IngesterPartitionRing.KVStore.Multi.ConfigProvider)
		},

		StoreGateway: func(t *testing.T, c Config) {
			require.NotNil(t, c.StoreGateway.ShardingRing.KVStore.Multi.ConfigProvider)
		},

		Querier: func(t *testing.T, c Config) {
			require.NotNil(t, c.StoreGateway.ShardingRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ingester.IngesterRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ingester.IngesterPartitionRing.KVStore.Multi.ConfigProvider)
		},

		Compactor: func(t *testing.T, c Config) {
			require.NotNil(t, c.Compactor.ShardingRing.Common.KVStore.Multi.ConfigProvider)
		},

		QueryScheduler: func(t *testing.T, c Config) {
			require.NotNil(t, c.QueryScheduler.ServiceDiscovery.SchedulerRing.KVStore.Multi.ConfigProvider)
		},

		Backend: func(t *testing.T, c Config) {
			require.NotNil(t, c.StoreGateway.ShardingRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Compactor.ShardingRing.Common.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.Ruler.Ring.Common.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.QueryScheduler.ServiceDiscovery.SchedulerRing.KVStore.Multi.ConfigProvider)
			require.NotNil(t, c.OverridesExporter.Ring.Common.KVStore.Multi.ConfigProvider)
		},
	} {
		t.Run(target, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			// Set to 0 to find any free port.
			cfg.Server.HTTPListenPort = 0
			cfg.Server.GRPCListenPort = 0
			cfg.Target = []string{target}

			// Must be set, otherwise MultiKV config provider will not be set.
			cfg.RuntimeConfig.LoadPath = []string{filepath.Join(dir, "config.yaml")}

			c, err := New(cfg, prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			_, err = c.ModuleManager.InitModuleServices(cfg.Target...)
			require.NoError(t, err)
			defer c.Server.Stop()

			checkFn(t, c.Cfg)
		})
	}
}

type mockKVStore struct {
	values map[string]mockValue
}

type mockValue struct {
	secret *hashivault.KVSecret
	err    error
}

func newMockKVStore() *mockKVStore {
	return &mockKVStore{
		values: map[string]mockValue{
			"test/secret1": {
				secret: &hashivault.KVSecret{
					Data: map[string]interface{}{
						"value": "foo1",
					},
				},
				err: nil,
			},
			"test/secret2": {
				secret: &hashivault.KVSecret{
					Data: map[string]interface{}{
						"value": "foo2",
					},
				},
				err: nil,
			},
			"test/secret3": {
				secret: &hashivault.KVSecret{
					Data: map[string]interface{}{
						"value": "foo3",
					},
				},
				err: nil,
			},
		},
	}
}

func (m *mockKVStore) Get(_ context.Context, path string) (*hashivault.KVSecret, error) {
	return m.values[path].secret, m.values[path].err
}

func TestInitVault(t *testing.T) {
	cfg := Config{
		Server: server.Config{
			HTTPTLSConfig: server.TLSConfig{
				TLSCertPath: "test/secret1",
				TLSKeyPath:  "test/secret2",
				ClientCAs:   "test/secret3",
			},
			GRPCTLSConfig: server.TLSConfig{
				TLSCertPath: "test/secret1",
				TLSKeyPath:  "test/secret2",
				ClientCAs:   "test/secret3",
			},
		},
		Vault: vault.Config{
			Enabled: true,
			Mock:    newMockKVStore(),
		},
	}

	mimir := &Mimir{
		Server: &server.Server{Registerer: prometheus.NewPedanticRegistry()},
		Cfg:    cfg,
	}

	_, err := mimir.initVault()
	require.NoError(t, err)

	// Check KVStore
	require.NotNil(t, mimir.Cfg.MemberlistKV.TCPTransport.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Distributor.HATrackerConfig.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Alertmanager.ShardingRing.Common.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Compactor.ShardingRing.Common.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Distributor.DistributorRing.Common.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Ingester.IngesterRing.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Ingester.IngesterPartitionRing.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Ruler.Ring.Common.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.StoreGateway.ShardingRing.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.QueryScheduler.ServiceDiscovery.SchedulerRing.KVStore.StoreConfig.Etcd.TLS.Reader)
	require.NotNil(t, mimir.Cfg.OverridesExporter.Ring.Common.KVStore.StoreConfig.Etcd.TLS.Reader)

	// Check Redis Clients
	require.NotNil(t, mimir.Cfg.BlocksStorage.BucketStore.IndexCache.BackendConfig.Redis.TLS.Reader)
	require.NotNil(t, mimir.Cfg.BlocksStorage.BucketStore.ChunksCache.BackendConfig.Redis.TLS.Reader)
	require.NotNil(t, mimir.Cfg.BlocksStorage.BucketStore.MetadataCache.BackendConfig.Redis.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Frontend.QueryMiddleware.ResultsCacheConfig.BackendConfig.Redis.TLS.Reader)

	// Check GRPC Clients
	require.NotNil(t, mimir.Cfg.IngesterClient.GRPCClientConfig.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Worker.QueryFrontendGRPCClientConfig.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Worker.QuerySchedulerGRPCClientConfig.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Querier.StoreGatewayClient.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Frontend.FrontendV2.GRPCClientConfig.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Ruler.ClientTLSConfig.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Ruler.Notifier.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Ruler.QueryFrontend.GRPCClientConfig.TLS.Reader)
	require.NotNil(t, mimir.Cfg.Alertmanager.AlertmanagerClient.GRPCClientConfig.TLS.Reader)
	require.NotNil(t, mimir.Cfg.QueryScheduler.GRPCClientConfig.TLS.Reader)

	// Check Server
	require.Empty(t, mimir.Cfg.Server.HTTPTLSConfig.TLSCertPath)
	require.Empty(t, mimir.Cfg.Server.HTTPTLSConfig.TLSKeyPath)
	require.Empty(t, mimir.Cfg.Server.HTTPTLSConfig.ClientCAs)

	require.Empty(t, mimir.Cfg.Server.GRPCTLSConfig.TLSCertPath)
	require.Empty(t, mimir.Cfg.Server.GRPCTLSConfig.TLSKeyPath)
	require.Empty(t, mimir.Cfg.Server.HTTPTLSConfig.ClientCAs)

	require.Equal(t, "foo1", mimir.Cfg.Server.HTTPTLSConfig.TLSCert)
	require.Equal(t, config.Secret("foo2"), mimir.Cfg.Server.HTTPTLSConfig.TLSKey)
	require.Equal(t, "foo3", mimir.Cfg.Server.HTTPTLSConfig.ClientCAsText)

	require.Equal(t, "foo1", mimir.Cfg.Server.GRPCTLSConfig.TLSCert)
	require.Equal(t, config.Secret("foo2"), mimir.Cfg.Server.GRPCTLSConfig.TLSKey)
	require.Equal(t, "foo3", mimir.Cfg.Server.GRPCTLSConfig.ClientCAsText)
}
