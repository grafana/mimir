// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/cortex_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
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
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/cache"
	"github.com/grafana/mimir/pkg/compactor"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/frontend/v1/frontendv1pb"
	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/ingester/activeseries"
	"github.com/grafana/mimir/pkg/ruler"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storegateway"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func TestMimir(t *testing.T) {
	cfg := Config{
		Ingester: ingester.Config{
			BlocksStorageConfig: tsdb.BlocksStorageConfig{
				Bucket: bucket.Config{
					Backend: bucket.S3,
					S3: s3.Config{
						Endpoint: "localhost",
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
				Backend: bucket.S3,
				S3: s3.Config{
					Endpoint: "localhost",
				},
			},
			BucketStore: tsdb.BucketStoreConfig{
				ChunkPoolMinBucketSizeBytes: tsdb.ChunkPoolDefaultMinBucketSize,
				ChunkPoolMaxBucketSizeBytes: tsdb.ChunkPoolDefaultMaxBucketSize,
				IndexCache: tsdb.IndexCacheConfig{
					BackendConfig: cache.BackendConfig{
						Backend: tsdb.IndexCacheBackendInMemory,
					},
				},
			},
		},
		Ruler: ruler.Config{
			Ring: ruler.RingConfig{
				KVStore: kv.Config{
					Store: "memberlist",
				},
				InstanceAddr: "test:8080",
			},
		},
		RulerStorage: rulestore.Config{
			Config: bucket.Config{
				Backend: "filesystem",
				Filesystem: filesystem.Config{
					Directory: t.TempDir(),
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
				KVStore:                kv.Config{Store: "memberlist"},
				ReplicationFactor:      1,
				InstanceInterfaceNames: []string{"en0", "eth0", "lo0", "lo"},
			},
		},
		AlertmanagerStorage: alertstore.Config{
			Config: bucket.Config{
				Backend: "filesystem",
				Filesystem: filesystem.Config{
					Directory: t.TempDir(),
				},
			},
		},
		Distributor: distributor.Config{
			DistributorRing: distributor.RingConfig{
				KVStore: kv.Config{
					Store: "inmemory",
				},
				InstanceInterfaceNames: []string{"en0", "eth0", "lo0", "lo"},
			},
		},
		StoreGateway: storegateway.Config{ShardingRing: storegateway.RingConfig{
			KVStore:                kv.Config{Store: "memberlist"},
			ReplicationFactor:      1,
			InstanceInterfaceNames: []string{"en0", "eth0", "lo0", "lo"},
		}},

		Target: []string{All, AlertManager},
	}

	c, err := New(cfg)
	require.NoError(t, err)

	serviceMap, err := c.ModuleManager.InitModuleServices(cfg.Target...)
	require.NoError(t, err)
	require.NotNil(t, serviceMap)

	for m, s := range serviceMap {
		// make sure each service is still New
		require.Equal(t, services.New, s.State(), "module: %s", m)
	}

	// check random modules that we expect to be configured when using Target=All
	require.NotNil(t, serviceMap[Server])
	require.NotNil(t, serviceMap[IngesterService])
	require.NotNil(t, serviceMap[Ring])
	require.NotNil(t, serviceMap[DistributorService])
	require.NotNil(t, serviceMap[Compactor])

	// check that alertmanager is configured which is not part of Target=All
	require.NotNil(t, serviceMap[AlertManager])
}

func TestMimirServerShutdownWithActivityTrackerEnabled(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	prepareGlobalMetricsRegistry(t)

	cfg := Config{}

	// This sets default values from flags to the config.
	flagext.RegisterFlagsWithLogger(log.NewNopLogger(), &cfg)

	tmpDir := t.TempDir()
	cfg.ActivityTracker.Filepath = filepath.Join(tmpDir, "activity.log") // Enable activity tracker

	cfg.Target = []string{API}
	cfg.Server = getServerConfig(t)
	require.NoError(t, cfg.Server.LogFormat.Set("logfmt"))
	require.NoError(t, cfg.Server.LogLevel.Set("debug"))

	util_log.InitLogger(&cfg.Server)

	c, err := New(cfg)
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.getTestConfig().Validate(nil)
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

func TestGrpcAuthMiddleware(t *testing.T) {
	prepareGlobalMetricsRegistry(t)

	cfg := Config{
		MultitenancyEnabled: true, // We must enable this to enable Auth middleware for gRPC server.
		Server:              getServerConfig(t),
		Target:              []string{API}, // Something innocent that doesn't require much config.
	}

	msch := &mockGrpcServiceHandler{}
	ctx := context.Background()

	// Setup server, using Mimir config. This includes authentication middleware.
	{
		c, err := New(cfg)
		require.NoError(t, err)

		serv, err := c.initServer()
		require.NoError(t, err)

		schedulerpb.RegisterSchedulerForQuerierServer(c.Server.GRPC, msch)
		frontendv1pb.RegisterFrontendServer(c.Server.GRPC, msch)

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
		if err == io.EOF {
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

// TODO Remove in Mimir 2.3.
func (t *Mimir) initTest() (services.Service, error) {

	return services.NewBasicService(
		nil,
		func(_ context.Context) error {
			// Sleep to avoid issue https://github.com/grafana/dskit/issues/151 .
			time.Sleep(100 * time.Millisecond)
			if t.Overrides.ActiveSeriesCustomTrackersConfig("1235").Empty() {
				return errors.New("active series config should not be empty")
			}
			return modules.ErrStopProcess
		},
		nil), nil
}

// TODO Remove in Mimir 2.3.
//      Previously ActiveSeriesCustomTrackers was an ingester config, now it's in LimitsConfig.
//      We provide backwards compatibility for it by parsing the old YAML location and copying it to LimitsConfig here,
//      unless it's also defined in the limits, which is invalid.
//		This needs to be set before setting default limits for unmarshalling.
// 		For more context see https://github.com/grafana/mimir/pull/1188#discussion_r830129443
func TestActiveSeriesDeprecationDefaultOverrideWithSomeRuntimeOverrides(t *testing.T) {
	prepareGlobalMetricsRegistry(t)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	yamlContent := `
overrides:
  '1235':
    ingestion_burst_size: 15000
    ingestion_rate: 1500
    max_global_series_per_metric: 7000
    max_global_series_per_user: 15000
    ruler_max_rule_groups_per_tenant: 20
    ruler_max_rules_per_rule_group: 20
`
	TestModuleName := "test"
	cfg := Config{}

	// This sets default values from flags to the config.
	flagext.RegisterFlagsWithLogger(log.NewNopLogger(), &cfg)

	// Creating test file with runtime overrides.
	tmpDir := t.TempDir()
	cfg.RuntimeConfig.LoadPath = filepath.Join(tmpDir, "overrides.yml")
	err := ioutil.WriteFile(cfg.RuntimeConfig.LoadPath, []byte(yamlContent), 0777)
	require.NoError(t, err, "Failed to write test override.yml.")

	// Setting up tracker config value as an deprecated ingester config.
	cfg.Ingester.ActiveSeriesCustomTrackers, err = activeseries.NewCustomTrackersConfig(map[string]string{
		"bool_is_true_flag-based": `{bool="true"}`,
		"bool_is_false_flagbased": `{bool="false"}`,
	})
	require.NoError(t, err)

	cfg.Target = []string{TestModuleName}
	cfg.Server = getServerConfig(t)
	require.NoError(t, cfg.Server.LogFormat.Set("logfmt"))
	require.NoError(t, cfg.Server.LogLevel.Set("debug"))
	util_log.InitLogger(&cfg.Server)

	c, err := New(cfg)
	require.NoError(t, err)
	// Creating a test module to ensure that runtime config check happens after initialization.
	c.ModuleManager.RegisterModule(TestModuleName, c.initTest)
	err = c.ModuleManager.AddDependency(TestModuleName, Overrides)
	require.NoError(t, err)

	errCh := make(chan error)
	go func() {
		errCh <- c.Run()
	}()

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "Mimir didn't stop in time")
	case err := <-errCh:
		require.NoError(t, err, "Active series deprecation override not in place!")
	}
}

// Generates server config, with gRPC listening on random port.
func getServerConfig(t *testing.T) server.Config {
	grpcHost, grpcPortNum := getHostnameAndRandomPort(t)
	httpHost, httpPortNum := getHostnameAndRandomPort(t)

	return server.Config{
		HTTPListenAddress: httpHost,
		HTTPListenPort:    httpPortNum,

		GRPCListenAddress: grpcHost,
		GRPCListenPort:    grpcPortNum,

		GPRCServerMaxRecvMsgSize: 1024,
	}
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
}

func (m *mockGrpcServiceHandler) NotifyClientShutdown(_ context.Context, _ *frontendv1pb.NotifyClientShutdownRequest) (*frontendv1pb.NotifyClientShutdownResponse, error) {
	m.clientShutdownCalled.Store(true)
	return &frontendv1pb.NotifyClientShutdownResponse{}, nil
}

func (m *mockGrpcServiceHandler) NotifyQuerierShutdown(_ context.Context, _ *schedulerpb.NotifyQuerierShutdownRequest) (*schedulerpb.NotifyQuerierShutdownResponse, error) {
	m.querierShutdownCalled.Store(true)
	return &schedulerpb.NotifyQuerierShutdownResponse{}, nil
}

func (m *mockGrpcServiceHandler) Process(_ frontendv1pb.Frontend_ProcessServer) error {
	panic("implement me")
}

func (m *mockGrpcServiceHandler) QuerierLoop(_ schedulerpb.SchedulerForQuerier_QuerierLoopServer) error {
	panic("implement me")
}

func prepareGlobalMetricsRegistry(t *testing.T) {
	oldReg, oldGat := prometheus.DefaultRegisterer, prometheus.DefaultGatherer

	reg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer, prometheus.DefaultGatherer = reg, reg

	t.Cleanup(func() {
		prometheus.DefaultRegisterer, prometheus.DefaultGatherer = oldReg, oldGat
	})
}
