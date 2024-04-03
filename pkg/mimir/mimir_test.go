// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/cortex_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	dslog "github.com/grafana/dskit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/frontend/v1/frontendv1pb"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMimir(t *testing.T) {
	cfg := Config{
		Server: server.Config{
			HTTPListenAddress: "localhost",
			GRPCListenAddress: "localhost",
		},
		Ingester: ingester.Config{
			BlocksStorageConfig: tsdb.BlocksStorageConfig{
				Bucket: bucket.Config{
					StorageBackendConfig: bucket.StorageBackendConfig{
						Backend: bucket.S3,
						S3: s3.Config{
							Endpoint: "localhost",
						},
					},
				},
			},
			IngesterRing: ingester.RingConfig{
				KVStore: kv.Config{
					Store: "inmemory",
				},
				ReplicationFactor:      3,
				InstanceInterfaceNames: []string{"en0", "eth0", "lo0", "lo"},
			},
		},
		BlocksStorage: tsdb.BlocksStorageConfig{
			Bucket: bucket.Config{
				StorageBackendConfig: bucket.StorageBackendConfig{
					Backend: bucket.S3,
					S3: s3.Config{
						Endpoint: "localhost",
					},
				},
			},
			BucketStore: tsdb.BucketStoreConfig{
				IndexCache: tsdb.IndexCacheConfig{
					BackendConfig: cache.BackendConfig{
						Backend: tsdb.IndexCacheBackendInMemory,
					},
				},
			},
		},
		Ruler: ruler.Config{
			Ring: ruler.RingConfig{
				Common: util.CommonRingConfig{
					KVStore: kv.Config{
						Store: "memberlist",
					},
					InstanceAddr: "test:8080",
				},
			},
		},
		RulerStorage: rulestore.Config{
			Config: bucket.Config{
				StorageBackendConfig: bucket.StorageBackendConfig{
					Backend: "filesystem",
					Filesystem: filesystem.Config{
						Directory: t.TempDir(),
					},
				},
			},
		},
		Compactor: compactor.Config{CompactionJobsOrder: compactor.CompactionOrderOldestFirst},
		Alertmanager: alertmanager.MultitenantAlertmanagerConfig{
			DataDir: t.TempDir(),
			ExternalURL: func() flagext.URLValue {
				v := flagext.URLValue{}
				require.NoError(t, v.Set("http://localhost/alertmanager"))
				return v
			}(),
			ShardingRing: alertmanager.RingConfig{
				Common: util.CommonRingConfig{
					KVStore:                kv.Config{Store: "memberlist"},
					InstanceInterfaceNames: []string{"en0", "eth0", "lo0", "lo"},
				},
				ReplicationFactor: 1,
			},
		},
		AlertmanagerStorage: alertstore.Config{
			Config: bucket.Config{
				StorageBackendConfig: bucket.StorageBackendConfig{
					Backend: "filesystem",
					Filesystem: filesystem.Config{
						Directory: t.TempDir(),
					},
				},
			},
		},
		Distributor: distributor.Config{
			DistributorRing: distributor.RingConfig{
				Common: util.CommonRingConfig{
					KVStore: kv.Config{
						Store: "inmemory",
					},
					InstanceInterfaceNames: []string{"en0", "eth0", "lo0", "lo"},
				},
			},
		},
		StoreGateway: storegateway.Config{ShardingRing: storegateway.RingConfig{
			KVStore:                kv.Config{Store: "memberlist"},
			ReplicationFactor:      1,
			InstanceInterfaceNames: []string{"en0", "eth0", "lo0", "lo"},
		}},
	}
	require.NoError(t, cfg.Server.LogLevel.Set("info"))

	tests := map[string]struct {
		target                  []string
		expectedEnabledModules  []string
		expectedDisabledModules []string
	}{
		"-target=all,alertmanager": {
			target: []string{All, AlertManager},
			expectedEnabledModules: []string{
				// Check random modules that we expect to be configured when using Target=All.
				Server, IngesterService, IngesterRing, DistributorService, Compactor,

				// Check that Alertmanager is configured which is not part of Target=All.
				AlertManager,
			},
		},
		"-target=write": {
			target:                  []string{Write},
			expectedEnabledModules:  []string{DistributorService, IngesterService},
			expectedDisabledModules: []string{Querier, Ruler, StoreGateway, Compactor, AlertManager},
		},
		"-target=read": {
			target:                  []string{Read},
			expectedEnabledModules:  []string{QueryFrontend, Querier},
			expectedDisabledModules: []string{IngesterService, Ruler, StoreGateway, Compactor, AlertManager},
		},
		"-target=backend": {
			target:                  []string{Backend},
			expectedEnabledModules:  []string{QueryScheduler, Ruler, StoreGateway, Compactor, AlertManager},
			expectedDisabledModules: []string{IngesterService, QueryFrontend, Querier},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg.Target = testData.target
			c, err := New(cfg, prometheus.NewPedanticRegistry())
			require.NoError(t, err)

			serviceMap, err := c.ModuleManager.InitModuleServices(cfg.Target...)
			require.NoError(t, err)
			require.NotNil(t, serviceMap)

			for m, s := range serviceMap {
				// make sure each service is still New
				require.Equal(t, services.New, s.State(), "module: %s", m)
			}

			for _, module := range testData.expectedEnabledModules {
				require.NotNilf(t, serviceMap[module], "module=%s", module)
			}

			for _, module := range testData.expectedDisabledModules {
				require.Nilf(t, serviceMap[module], "module=%s", module)
			}
		})
	}
}

func TestMimirServerShutdownWithActivityTrackerEnabled(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	cfg := Config{}

	// This sets default values from flags to the config.
	flagext.RegisterFlagsWithLogger(log.NewNopLogger(), &cfg)

	tmpDir := t.TempDir()
	cfg.ActivityTracker.Filepath = filepath.Join(tmpDir, "activity.log") // Enable activity tracker

	cfg.Target = []string{Querier}
	cfg.Server = getServerConfig(t, dslog.LogfmtFormat, "debug")

	cfg.Server.Log = util_log.InitLogger(cfg.Server.LogFormat, cfg.Server.LogLevel, false, util_log.RateLimitedLoggerCfg{})

	c, err := New(cfg, prometheus.NewPedanticRegistry())
	require.NoError(t, err)

	errCh := make(chan error)
	go func() {
		errCh <- c.Run()
	}()

	test.Poll(t, 10*time.Second, true, func() interface{} {
		r, err := http.Get(fmt.Sprintf("http://%s:%d/ready", cfg.Server.HTTPListenAddress, cfg.Server.HTTPListenPort))
		if err != nil {
			t.Log("Got error when checking /ready:", err)
			return false
		}
		return r.StatusCode == 200
	})

	proc, err := os.FindProcess(os.Getpid())
	require.NoError(t, err)

	// Mimir reacts on SIGINT and does shutdown.
	require.NoError(t, proc.Signal(syscall.SIGINT))

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "Mimir didn't stop in time")
	case err := <-errCh:
		require.NoError(t, err)
	}
}

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		getTestConfig  func() *Config
		expectedError  error
		expectAnyError bool
	}{
		{
			name: "should pass validation if the http prefix is empty",
			getTestConfig: func() *Config {
				return newDefaultConfig()
			},
			expectedError: nil,
		},
		{
			name: "S3: should fail if bucket name is shared between alertmanager and blocks storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config} {
					bucketCfg.Backend = bucket.S3
					bucketCfg.S3.BucketName = "b1"
					bucketCfg.S3.Region = "r1"
				}
				return cfg
			},
			expectedError: errInvalidBucketConfig,
		},
		{
			name: "GCS: should fail if bucket name is shared between alertmanager and blocks storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config} {
					bucketCfg.Backend = bucket.GCS
					bucketCfg.GCS.BucketName = "b1"
				}
				return cfg
			},
			expectedError: errInvalidBucketConfig,
		},
		{
			name: "Azure: should fail if container and account names are shared between alertmanager and blocks storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config} {
					bucketCfg.Backend = bucket.Azure
					bucketCfg.Azure.ContainerName = "c1"
					bucketCfg.Azure.StorageAccountName = "sa1"
				}
				return cfg
			},
			expectedError: errInvalidBucketConfig,
		},
		{
			name: "Azure: should pass if only container name is shared between alertmanager and blocks storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				for i, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config} {
					bucketCfg.Backend = bucket.Azure
					bucketCfg.Azure.ContainerName = "c1"
					bucketCfg.Azure.StorageAccountName = fmt.Sprintf("sa%d", i)
				}
				return cfg
			},
			expectedError: nil,
		},
		{
			name: "Swift: should fail if container and project names are shared between alertmanager and blocks storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config} {
					bucketCfg.Backend = bucket.Swift
					bucketCfg.Swift.ContainerName = "c1"
					bucketCfg.Swift.ProjectName = "p1"
				}
				return cfg
			},
			expectedError: errInvalidBucketConfig,
		},
		{
			name: "Swift: should pass if only container name is shared between alertmanager and blocks storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				for i, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config} {
					bucketCfg.Backend = bucket.Swift
					bucketCfg.Swift.ContainerName = "c1"
					bucketCfg.Swift.ProjectName = fmt.Sprintf("p%d", i)
				}
				return cfg
			},
			expectedError: nil,
		},
		{
			name: "Alertmanager: should ignore invalid alertmanager configuration when alertmanager is not running",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all")

				cfg.Alertmanager.ShardingRing.ZoneAwarenessEnabled = true
				return cfg
			},
			expectedError: nil,
		},
		{
			name: "Alertmanager: should fail with invalid alertmanager configuration when alertmanager is not running",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				cfg.Alertmanager.ShardingRing.ZoneAwarenessEnabled = true
				return cfg
			},
			expectAnyError: true,
		},
		{
			name: "S3: should pass if bucket name is shared between alertmanager and ruler storage because they already use separate prefixes (rules/ and alerts/)",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")

				for _, bucketCfg := range []*bucket.Config{&cfg.RulerStorage.Config, &cfg.AlertmanagerStorage.Config} {
					bucketCfg.Backend = bucket.S3
					bucketCfg.S3.BucketName = "b1"
					bucketCfg.S3.Region = "r1"
				}
				return cfg
			},
			expectedError: nil,
		},
		{
			name: "should fail if querier timeout is bigger than http server timeout",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("all,alertmanager")
				cfg.Querier.EngineConfig.Timeout = cfg.Server.HTTPServerWriteTimeout + time.Second

				return cfg
			},
			expectAnyError: true,
		},
		{
			name: "should pass if grpc errors are disabled, and the distributor is running with ingest storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("distributor")
				cfg.IngestStorage.Enabled = true
				cfg.IngestStorage.KafkaConfig.Address = "localhost:123"
				cfg.IngestStorage.KafkaConfig.Topic = "topic"
				cfg.Ingester.DeprecatedReturnOnlyGRPCErrors = false

				return cfg
			},
			expectAnyError: false,
		},
		{
			name: "should fails if grpc errors are disabled, and the ingester is running with ingest storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("ingester")
				cfg.IngestStorage.Enabled = true
				cfg.IngestStorage.KafkaConfig.Address = "localhost:123"
				cfg.IngestStorage.KafkaConfig.Topic = "topic"
				cfg.Ingester.DeprecatedReturnOnlyGRPCErrors = false

				return cfg
			},
			expectAnyError: true,
		},
		{
			name: "should fails if push api disabled in ingester, and the ingester isn't running with ingest storage",
			getTestConfig: func() *Config {
				cfg := newDefaultConfig()
				_ = cfg.Target.Set("ingester")
				cfg.Ingester.PushGrpcMethodEnabled = false
				cfg.IngestStorage.Enabled = false

				return cfg
			},
			expectAnyError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.getTestConfig().Validate(log.NewNopLogger())
			if tc.expectAnyError {
				require.Error(t, err)
			} else if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfig_ValidateLimits(t *testing.T) {
	for _, tc := range []struct {
		name         string
		testConfig   *Config
		limitsConfig validation.Limits
		hasError     bool
	}{
		{
			name:         "default configs should pass validation",
			testConfig:   newDefaultConfig(),
			limitsConfig: newDefaultConfig().LimitsConfig,
		},
		{
			name: "non overlapping query-store-after and query-ingesters-within should return error",
			testConfig: func() *Config {
				c := newDefaultConfig()
				c.Querier.QueryStoreAfter = time.Hour
				return c
			}(),
			limitsConfig: func() validation.Limits {
				limits := newDefaultConfig().LimitsConfig
				limits.QueryIngestersWithin = model.Duration(time.Minute)
				return limits
			}(),
			hasError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.testConfig.ValidateLimits(tc.limitsConfig)
			if tc.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfig_validateFilesystemPaths(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	tests := map[string]struct {
		setup       func(cfg *Config)
		expectedErr string
	}{
		"should succeed with the default configuration": {
			setup: func(*Config) {},
		},
		"should fail if alertmanager data directory contains bucket store sync directory when running mimir-backend": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Backend}
				cfg.Alertmanager.DataDir = "/data"
				cfg.BlocksStorage.BucketStore.SyncDir = "/data/tsdb"
			},
			expectedErr: `the configured bucket store sync directory "/data/tsdb" cannot overlap with the configured alertmanager data directory "/data"`,
		},
		"should fail if alertmanager filesystem backend directory is equal to alertmanager data directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "/path/to/alertmanager"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/alertmanager/"
			},
			expectedErr: `the configured alertmanager data directory "/path/to/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager/"`,
		},
		"should fail if alertmanager filesystem backend directory is a subdirectory of alertmanager data directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "/path/to/alertmanager"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/alertmanager/subdir"
			},
			expectedErr: `the configured alertmanager data directory "/path/to/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager/subdir"`,
		},
		"should fail if alertmanager data directory is a subdirectory of alertmanager filesystem backend directory, and matches with the prefix used to store alerts": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "/path/to/alertmanager/alerts"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/alertmanager"
			},
			expectedErr: `the configured alertmanager data directory "/path/to/alertmanager/alerts" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager"`,
		},
		"should succeed if alertmanager data directory is a subdirectory of alertmanager filesystem backend directory, but doesn't match with the prefix used to store alerts": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "/path/to/alertmanager/data"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/alertmanager"
			},
		},
		"should fail if alertmanager data directory (relative) is a subdirectory of alertmanager filesystem backend directory (absolute), and matches with the prefix used to store alertmanager config": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "./data/alertmanager"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = filepath.Join(cwd, "data")
			},
			expectedErr: fmt.Sprintf(`the configured alertmanager data directory "./data/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "%s"`, filepath.Join(cwd, "data")),
		},
		"should fail if ruler filesystem backend directory is equal to ruler data directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ruler}
				cfg.Ruler.RulePath = "/path/to/ruler"
				cfg.RulerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.RulerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/ruler/"
			},
			expectedErr: `the configured ruler data directory "/path/to/ruler" cannot overlap with the configured ruler storage filesystem directory "/path/to/ruler/"`,
		},
		"should fail if store-gateway and compactor data directory overlap": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{StoreGateway, Compactor}
				cfg.BlocksStorage.BucketStore.SyncDir = "/path/to/data"
				cfg.Compactor.DataDir = "/path/to/data/compactor"
			},
			expectedErr: `the configured bucket store sync directory "/path/to/data" cannot overlap with the configured compactor data directory "/path/to/data/compactor"`,
		},
		"should succeed if store-gateway and compactor data directory overlap, but it's running only the store-gateway": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{StoreGateway}
				cfg.BlocksStorage.BucketStore.SyncDir = "/path/to/data"
				cfg.Compactor.DataDir = "/path/to/data/compactor"
			},
		},
		"should fail if tsdb directory and blocks storage filesystem directory overlap": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/path/to/data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/path/to/data/blocks"
			},
			expectedErr: `the configured blocks storage filesystem directory "/path/to/data/blocks" cannot overlap with the configured tsdb directory "/path/to/data"`,
		},
		"should succeed if tsdb directory and blocks storage filesystem directory overlap, but blocks storage has prefix configured": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/path/to/data/tsdb"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem

				// The storage directory itself overlaps with TSDB data directory,
				// but it doesn't if you also apply the prefix.
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/path/to/data"
				cfg.BlocksStorage.Bucket.StoragePrefix = "blocks"
			},
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/path/to/data/tsdb"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/path/to/data/blocks"
			},
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap and one has the same prefix of the other one": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/path/to/data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/path/to/data-blocks"
			},
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap and they're both root directories": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/data-blocks"
			},
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap and they're both child of the same directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "./data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "./data-blocks"
			},
		},
		"should succeed if blocks storage filesystem directory overlaps with alertmanager data directory, but we're running alertmanager in microservices mode": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "blocks"
				cfg.AlertmanagerStorage.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Filesystem.Directory = "/data/alertmanager"
				cfg.Alertmanager.DataDir = cwd
			},
		},
		"should fail if blocks storage filesystem directory overlaps with alertmanager data directory, and alertmanager is running along with other components": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{All, AlertManager}
				cfg.Common.Storage.Backend = bucket.Filesystem
				cfg.Common.Storage.Filesystem.Directory = "blocks"
				cfg.Alertmanager.DataDir = cwd
			},
			expectedErr: fmt.Sprintf(`the configured blocks storage filesystem directory "blocks" cannot overlap with the configured alertmanager data directory "%s"`, cwd),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			actualErr := cfg.validateFilesystemPaths(log.NewNopLogger())

			if testData.expectedErr != "" {
				require.Error(t, actualErr)
				require.Contains(t, actualErr.Error(), testData.expectedErr)
			} else {
				require.NoError(t, actualErr)
			}
		})
	}
}

func TestIsAbsPathOverlapping(t *testing.T) {
	tests := []struct {
		first    string
		second   string
		expected bool
	}{
		{
			first:    "/",
			second:   "/",
			expected: true,
		},
		{
			first:    "/data",
			second:   "/",
			expected: true,
		},
		{
			first:    "/",
			second:   "/data",
			expected: true,
		},
		{
			first:    "/data",
			second:   "/data-more",
			expected: false,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/data",
			expected: true,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/data/more",
			expected: true,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/data-more",
			expected: false,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/more/data",
			expected: false,
		},
	}

	for _, testData := range tests {
		t.Run(fmt.Sprintf("check if %q overlaps %q", testData.first, testData.second), func(t *testing.T) {
			assert.Equal(t, testData.expected, isAbsPathOverlapping(testData.first, testData.second))
		})
	}
}

func TestGrpcAuthMiddleware(t *testing.T) {
	cfg := Config{
		MultitenancyEnabled: true, // We must enable this to enable Auth middleware for gRPC server.
		Server:              getServerConfig(t, dslog.LogfmtFormat, "debug"),
		Target:              []string{API}, // Something innocent that doesn't require much config.
	}

	msch := &mockGrpcServiceHandler{}
	ctx := context.Background()

	// Setup server, using Mimir config. This includes authentication middleware.
	{
		c, err := New(cfg, prometheus.NewPedanticRegistry())
		require.NoError(t, err)

		serv, err := c.initServer()
		require.NoError(t, err)

		schedulerpb.RegisterSchedulerForQuerierServer(c.Server.GRPC, msch)
		frontendv1pb.RegisterFrontendServer(c.Server.GRPC, msch)
		ruler.RegisterRulerServer(c.Server.GRPC, msch)

		require.NoError(t, services.StartAndAwaitRunning(ctx, serv))
		defer func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, serv))
		}()
	}

	conn, err := grpc.Dial(net.JoinHostPort(cfg.Server.GRPCListenAddress, strconv.Itoa(cfg.Server.GRPCListenPort)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Close())
	}()

	{
		// Verify that we can call frontendClient.NotifyClientShutdown without user in the context, and we don't get any error.
		require.False(t, msch.clientShutdownCalled.Load())
		frontendClient := frontendv1pb.NewFrontendClient(conn)
		_, err = frontendClient.NotifyClientShutdown(ctx, &frontendv1pb.NotifyClientShutdownRequest{ClientID: "random-client-id"})
		require.NoError(t, err)
		require.True(t, msch.clientShutdownCalled.Load())
	}

	{
		// Verify that we can call schedulerClient.NotifyQuerierShutdown without user in the context, and we don't get any error.
		require.False(t, msch.querierShutdownCalled.Load())
		schedulerClient := schedulerpb.NewSchedulerForQuerierClient(conn)
		_, err = schedulerClient.NotifyQuerierShutdown(ctx, &schedulerpb.NotifyQuerierShutdownRequest{QuerierID: "random-querier-id"})
		require.NoError(t, err)
		require.True(t, msch.querierShutdownCalled.Load())
	}
	{
		// Verify that we can call rulerClient.SyncRules without user in the context, and we don't get any error.
		require.False(t, msch.rulerSyncRulesCalled.Load())
		rulerClient := ruler.NewRulerClient(conn)
		_, err = rulerClient.SyncRules(ctx, &ruler.SyncRulesRequest{UserIds: []string{"random-user-id"}})
		require.NoError(t, err)
		require.True(t, msch.rulerSyncRulesCalled.Load())
	}
}

func TestFlagDefaults(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	c := Config{}

	f := flag.NewFlagSet("test", flag.PanicOnError)
	c.RegisterFlags(f, log.NewNopLogger())

	buf := bytes.Buffer{}

	f.SetOutput(&buf)
	f.PrintDefaults()

	const delim = '\n'

	minTimeChecked := false
	pingWithoutStreamChecked := false
	for {
		line, err := buf.ReadString(delim)
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)

		if strings.Contains(line, "-server.grpc.keepalive.min-time-between-pings") {
			nextLine, err := buf.ReadString(delim)
			require.NoError(t, err)
			assert.Contains(t, nextLine, "(default 10s)")
			minTimeChecked = true
		}

		if strings.Contains(line, "-server.grpc.keepalive.ping-without-stream-allowed") {
			nextLine, err := buf.ReadString(delim)
			require.NoError(t, err)
			assert.Contains(t, nextLine, "(default true)")
			pingWithoutStreamChecked = true
		}
	}

	require.True(t, minTimeChecked)
	require.True(t, pingWithoutStreamChecked)

	require.Equal(t, true, c.Server.GRPCServerPingWithoutStreamAllowed)
	require.Equal(t, 10*time.Second, c.Server.GRPCServerMinTimeBetweenPings)
}

func TestOnlyValidRuntimeConfigIsLoaded(t *testing.T) {
	dir := t.TempDir()
	loadPath := filepath.Join(dir, "test")

	userID := "12345"

	initialValidConfig := `
overrides:
  '12345':
    query_ingesters_within: 15h
`
	err := os.WriteFile(loadPath, []byte(initialValidConfig), 0600)
	require.NoError(t, err)

	cfg := Config{}
	flagext.DefaultValues(&cfg)

	cfg.Target = []string{Overrides}

	cfg.RuntimeConfig.LoadPath = []string{loadPath}
	cfg.RuntimeConfig.ReloadPeriod = 100 * time.Millisecond
	cfg.Querier.QueryStoreAfter = 12 * time.Hour
	cfg.LimitsConfig.QueryIngestersWithin = model.Duration(13 * time.Hour)

	registry := prometheus.NewPedanticRegistry()
	c, err := New(cfg, registry)
	require.NoError(t, err)

	// init services
	sm, err := c.ModuleManager.InitModuleServices(cfg.Target...)
	require.NoError(t, err)
	defer c.Server.Stop()

	servs := []services.Service(nil)
	for _, s := range sm {
		servs = append(servs, s)
	}
	m, err := services.NewManager(servs...)
	require.NoError(t, err)

	// before the services start, all users have the default limits
	duration := c.Overrides.QueryIngestersWithin(userID)
	require.Equal(t, cfg.LimitsConfig.QueryIngestersWithin, model.Duration(duration))

	// start services
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, m.StartAsync(ctx))
	require.NoError(t, m.AwaitHealthy(ctx))

	// the runtime config is now loaded, so the content of initialValidConfig will be used
	duration = c.Overrides.QueryIngestersWithin(userID)
	require.Equal(t, 15*time.Hour, duration)

	// check load is successful via metric
	metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err)
	metric := metrics["cortex_runtime_config_last_reload_successful"].Metric[0]
	require.NotNil(t, metric.Gauge.Value)
	require.Equal(t, 1., metric.Gauge.GetValue())

	// write invalid config with query_ingesters_with < query_store_after
	configYAML2 := `
overrides:
  '12345':
    query_ingesters_within: 2h
`
	err = os.WriteFile(loadPath, []byte(configYAML2), 0600)
	require.NoError(t, err)

	// wait sufficient time for new runtime config to be loaded
	time.Sleep(cfg.RuntimeConfig.ReloadPeriod * 2)

	// invalid config is ignored, still using initialValidConfig
	duration = c.Overrides.QueryIngestersWithin(userID)
	require.Equal(t, 15*time.Hour, duration)

	// check load was unsuccessful via metric
	metrics, err = dskit_metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err)
	metric = metrics["cortex_runtime_config_last_reload_successful"].Metric[0]
	require.NotNil(t, metric.Gauge.Value)
	require.Equal(t, 0., metric.Gauge.GetValue())

	// write a new valid config
	anotherValidConfig := `
overrides:
  '12345':
    query_ingesters_within: 20h
`
	err = os.WriteFile(loadPath, []byte(anotherValidConfig), 0600)
	require.NoError(t, err)

	// wait sufficient time for new runtime config to be loaded
	time.Sleep(cfg.RuntimeConfig.ReloadPeriod * 2)

	// check config has been updated to anotherValidConfig
	duration = c.Overrides.QueryIngestersWithin(userID)
	require.Equal(t, 20*time.Hour, duration)

	// check load is successful via metric
	metrics, err = dskit_metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err)
	metric = metrics["cortex_runtime_config_last_reload_successful"].Metric[0]
	require.NotNil(t, metric.Gauge.Value)
	require.Equal(t, 1., metric.Gauge.GetValue())
}

// Generates server config, with gRPC listening on random port.
func getServerConfig(t *testing.T, logFormat, logLevel string) server.Config {
	grpcHost, grpcPortNum := getHostnameAndRandomPort(t)
	httpHost, httpPortNum := getHostnameAndRandomPort(t)

	cfg := server.Config{
		HTTPListenAddress: httpHost,
		HTTPListenPort:    httpPortNum,

		GRPCListenAddress: grpcHost,
		GRPCListenPort:    grpcPortNum,

		GRPCServerMaxRecvMsgSize: 1024,
		LogFormat:                logFormat,
		Registerer:               prometheus.NewPedanticRegistry(),
	}
	require.NoError(t, cfg.LogLevel.Set(logLevel))
	return cfg
}

func getHostnameAndRandomPort(t *testing.T) (string, int) {
	listen, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	host, port, err := net.SplitHostPort(listen.Addr().String())
	require.NoError(t, err)
	require.NoError(t, listen.Close())

	portNum, err := strconv.Atoi(port)
	require.NoError(t, err)
	return host, portNum
}

type mockGrpcServiceHandler struct {
	clientShutdownCalled  atomic.Bool
	querierShutdownCalled atomic.Bool
	rulerSyncRulesCalled  atomic.Bool
}

func (m *mockGrpcServiceHandler) NotifyClientShutdown(_ context.Context, _ *frontendv1pb.NotifyClientShutdownRequest) (*frontendv1pb.NotifyClientShutdownResponse, error) {
	m.clientShutdownCalled.Store(true)
	return &frontendv1pb.NotifyClientShutdownResponse{}, nil
}

func (m *mockGrpcServiceHandler) NotifyQuerierShutdown(_ context.Context, _ *schedulerpb.NotifyQuerierShutdownRequest) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	m.querierShutdownCalled.Store(true)
	return &schedulerpb.NotifyQuerierShutdownResponse{}, nil
}

func (m *mockGrpcServiceHandler) SyncRules(_ context.Context, _ *ruler.SyncRulesRequest) (*ruler.SyncRulesResponse, error) {
	m.rulerSyncRulesCalled.Store(true)
	return &ruler.SyncRulesResponse{}, nil
}

func (m *mockGrpcServiceHandler) Rules(_ context.Context, _ *ruler.RulesRequest) (*ruler.RulesResponse, error) {
	return &ruler.RulesResponse{}, nil
}

func (m *mockGrpcServiceHandler) Process(_ frontendv1pb.Frontend_ProcessServer) error {
	panic("implement me")
}

func (m *mockGrpcServiceHandler) QuerierLoop(_ schedulerpb.SchedulerForQuerier_QuerierLoopServer) error {
	panic("implement me")
}
