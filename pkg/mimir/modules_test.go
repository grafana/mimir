// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/modules_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
)

func changeTargetConfig(c *Config) {
	c.Target = []string{"all", "ruler"}
}

func TestAPIConfig(t *testing.T) {
	actualCfg := newDefaultConfig()

	mimir := &Mimir{
		Server: &server.Server{},
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
				Server: &server.Server{},
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

func TestMimirKVSetup(t *testing.T) {
	dir := t.TempDir()

	for _, target := range []string{All} {
		cfg := Config{}
		flagext.DefaultValues(&cfg)
		cfg.Target = []string{target}

		// Must be set, otherwise MultiKV config provider will not be set.
		cfg.RuntimeConfig.LoadPath = filepath.Join(dir, "config.yaml")

		c, err := New(cfg)
		require.NoError(t, err)

		_, err = c.ModuleManager.InitModuleServices(cfg.Target...)
		require.NoError(t, err)

		// Verify that multikv config provider is set in all rings.
		require.NotNil(t, c.Cfg.Distributor.DistributorRing.KVStore.Multi.ConfigProvider)
		require.NotNil(t, c.Cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.Multi)
		require.NotNil(t, c.Cfg.StoreGateway.ShardingRing.KVStore.Multi.ConfigProvider)
		require.NotNil(t, c.Cfg.Compactor.ShardingRing.KVStore.Multi.ConfigProvider)
		require.NotNil(t, c.Cfg.Ruler.Ring.KVStore.Multi.ConfigProvider)
		require.NotNil(t, c.Cfg.Alertmanager.ShardingRing.KVStore.Multi.ConfigProvider)
	}
}
