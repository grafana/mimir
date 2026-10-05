// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/multitenant_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/kv/consul"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/grafana/regexp"
	"github.com/prometheus/alertmanager/alert"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"go.uber.org/goleak"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerpb"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util"
	utillog "github.com/grafana/mimir/pkg/util/log"
	utiltest "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	simpleConfigOne = `route:
  receiver: dummy

receivers:
  - name: dummy`

	simpleConfigTwo = `route:
  receiver: dummy2

receivers:
  - name: dummy2`

	simpleTemplateOne = `{{ define "some.template.one" }}{{ end }}`
	simpleTemplateTwo = `{{ define "some.template.two" }}{{ end }}`
	badConfig         = `
route:
  receiver: NOT_EXIST`
)

func mockAlertmanagerConfig(t *testing.T) *MultitenantAlertmanagerConfig {
	t.Helper()

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost/alertmanager")
	require.NoError(t, err)

	tempDir := t.TempDir()

	cfg := &MultitenantAlertmanagerConfig{}
	flagext.DefaultValues(cfg)

	cfg.ExternalURL = externalURL
	cfg.DataDir = tempDir
	cfg.ShardingRing.Common.InstanceID = "test"
	cfg.ShardingRing.Common.InstanceAddr = "127.0.0.1"
	cfg.PollInterval = time.Minute
	cfg.ShardingRing.ReplicationFactor = 1
	cfg.Persister = PersisterConfig{Interval: time.Hour}

	return cfg
}

func setupSingleMultitenantAlertmanager(t *testing.T, cfg *MultitenantAlertmanagerConfig, store alertstore.AlertStore, limits Limits, features featurecontrol.Flagger, logger log.Logger, registerer prometheus.Registerer) *MultitenantAlertmanager {
	// The mock ring store means we do not need a real e.g. Consul running.
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() {
		assert.NoError(t, closer.Close())
	})

	// The mock will have the default fallback config.
	amCfg, err := ComputeFallbackConfig("")
	require.NoError(t, err)

	if limits == nil {
		limits = &mockAlertManagerLimits{}
	}
	am, err := createMultitenantAlertmanager(cfg, amCfg, store, ringStore, limits, features, logger, registerer)
	require.NoError(t, err)

	// The mock client pool allows the distributor to talk to the instance
	// without requiring a gRPC server to be running.
	clientPool := newPassthroughAlertmanagerClientPool()
	clientPool.setServer(cfg.ShardingRing.Common.InstanceAddr+":0", am)
	am.alertmanagerClientsPool = clientPool
	am.distributor.alertmanagerClientsPool = clientPool

	// We need to start the alertmanager for most tests in order for tenant
	// ownership checking to work, as it queries the ring.
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), am))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), am))
	})

	return am
}

func TestMultitenantAlertmanagerConfig_Validate(t *testing.T) {
	tests := map[string]struct {
		setup    func(t *testing.T, cfg *MultitenantAlertmanagerConfig)
		expected error
	}{
		"should pass with default config": {
			setup:    func(*testing.T, *MultitenantAlertmanagerConfig) {},
			expected: nil,
		},
		"should fail with empty external URL": {
			setup: func(t *testing.T, cfg *MultitenantAlertmanagerConfig) {
				require.NoError(t, cfg.ExternalURL.Set(""))
			},
			expected: errEmptyExternalURL,
		},
		"should fail if persistent interval is 0": {
			setup: func(_ *testing.T, cfg *MultitenantAlertmanagerConfig) {
				cfg.Persister.Interval = 0
			},
			expected: errInvalidPersistInterval,
		},
		"should fail if persistent interval is negative": {
			setup: func(_ *testing.T, cfg *MultitenantAlertmanagerConfig) {
				cfg.Persister.Interval = -1
			},
			expected: errInvalidPersistInterval,
		},
		"should fail if external URL ends with /": {
			setup: func(t *testing.T, cfg *MultitenantAlertmanagerConfig) {
				require.NoError(t, cfg.ExternalURL.Set("http://localhost/prefix/"))
			},
			expected: errInvalidExternalURLEndingSlash,
		},
		"should succeed if external URL does not end with /": {
			setup: func(t *testing.T, cfg *MultitenantAlertmanagerConfig) {
				require.NoError(t, cfg.ExternalURL.Set("http://localhost/prefix"))
			},
			expected: nil,
		},
		"should fail if external URL has no scheme": {
			setup: func(t *testing.T, cfg *MultitenantAlertmanagerConfig) {
				require.NoError(t, cfg.ExternalURL.Set("example.com/alertmanager"))
			},
			expected: errInvalidExternalURLMissingScheme,
		},
		"should fail if external URL has no hostname": {
			setup: func(t *testing.T, cfg *MultitenantAlertmanagerConfig) {
				require.NoError(t, cfg.ExternalURL.Set("https:///alertmanager"))
			},
			expected: errInvalidExternalURLMissingHostname,
		},
		"should fail if zone aware is enabled but zone is not set": {
			setup: func(_ *testing.T, cfg *MultitenantAlertmanagerConfig) {
				cfg.ShardingRing.ZoneAwarenessEnabled = true
			},
			expected: errZoneAwarenessEnabledWithoutZoneInfo,
		},
		"should pass if the URL just contains the path with the leading /": {
			setup: func(t *testing.T, cfg *MultitenantAlertmanagerConfig) {
				require.NoError(t, cfg.ExternalURL.Set("/alertmanager"))
			},
			expected: nil,
		},
		"should fail if the URL just contains the hostname (because it can't be disambiguated with a path)": {
			setup: func(t *testing.T, cfg *MultitenantAlertmanagerConfig) {
				require.NoError(t, cfg.ExternalURL.Set("alertmanager"))
			},
			expected: errInvalidExternalURLMissingScheme,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := &MultitenantAlertmanagerConfig{}
			flagext.DefaultValues(cfg)
			testData.setup(t, cfg)
			assert.Equal(t, testData.expected, cfg.Validate())
		})
	}
}

func TestMultitenantAlertmanager_relativeDataDir(t *testing.T) {
	ctx := context.Background()

	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()
	require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User: "user",
		RawConfig: simpleConfigOne + `
templates:
- 'first.tpl'
- 'second.tpl'
`,
		Templates: []*alertspb.TemplateDesc{
			{
				Filename: "first.tpl",
				Body:     `{{ define "t1" }}Template 1 ... {{end}}`,
			},
			{
				Filename: "second.tpl",
				Body:     `{{ define "t2" }}Template 2{{ end}}`,
			},
		},
	}))

	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	// Alter datadir to get a relative path
	cwd, err := os.Getwd()
	require.NoError(t, err)
	cfg.DataDir, err = filepath.Rel(cwd, cfg.DataDir)
	require.NoError(t, err)
	am := setupSingleMultitenantAlertmanager(t, cfg, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	// Ensure the configs are synced correctly and persist across several syncs
	for i := 0; i < 3; i++ {
		err := am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		require.Len(t, am.alertmanagers, 1)

		dirs := am.getPerUserDirectories()
		userDir := dirs["user"]
		require.NotZero(t, userDir)
		require.True(t, dirExists(t, userDir))
	}
}

func TestMultitenantAlertmanager_loadAndSyncConfigs(t *testing.T) {
	utiltest.VerifyNoLeak(t,
		// Upstream alertmanager's Inhibitor.Stop() and Dispatcher.Stop() signal
		// cancellation but don't fully wait for all spawned goroutines to return.
		// These goroutines exit eventually, but may still be draining when goleak
		// checks after a prior test's deferred StopAndWait.
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/dispatch.(*Dispatcher).run"),
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/dispatch.(*aggrGroup).run"),
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/inhibit.(*Inhibitor).run"),
		goleak.IgnoreTopFunction("github.com/oklog/run.(*Group).Run"),
	)

	ctx := context.Background()

	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()
	user1Cfg := &alertspb.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{},
	}
	require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))

	user2Cfg := &alertspb.AlertConfigDesc{
		User:      "user2",
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{},
	}
	require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am := setupSingleMultitenantAlertmanager(t, cfg, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	// Ensure the configs are synced correctly
	err := am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 2)

	currentConfigFp, cfgExists := am.cfgs["user1"]
	require.True(t, cfgExists)
	require.Equal(t, fingerprint(user1Cfg), currentConfigFp)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	// Ensure when a 3rd config is added, it is synced correctly
	user3Cfg := &alertspb.AlertConfigDesc{
		User: "user3",
		RawConfig: simpleConfigOne + `
templates:
- 'first.tpl'
- 'second.tpl'
`,
		Templates: []*alertspb.TemplateDesc{
			{
				Filename: "first.tpl",
				Body:     `{{ define "t1" }}Template 1 ... {{end}}`,
			},
			{
				Filename: "second.tpl",
				Body:     `{{ define "t2" }}Template 2{{ end}}`,
			},
		},
	}
	require.NoError(t, store.SetAlertConfig(ctx, user3Cfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 3)

	dirs := am.getPerUserDirectories()
	user3Dir := dirs["user3"]
	require.NotZero(t, user3Dir)
	require.True(t, dirExists(t, user3Dir))
	finalUserCfgFp, ok := am.cfgs["user3"]
	require.True(t, ok)
	require.Equal(t, fingerprint(user3Cfg), finalUserCfgFp)
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
		cortex_alertmanager_config_last_reload_successful{user="user3"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	user1Cfg = &alertspb.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigTwo,
		Templates: []*alertspb.TemplateDesc{},
	}
	// Ensure the config is updated
	require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	currentConfigFp, cfgExists = am.cfgs["user1"]
	require.True(t, cfgExists)
	expectedFp := fingerprint(user1Cfg)
	require.Equal(t, expectedFp, currentConfigFp)

	// Ensure the config is reloaded if only templates changed
	user1Cfg = &alertspb.AlertConfigDesc{
		User: "user1",
		RawConfig: simpleConfigTwo + `
templates:
- 'some-template.tmpl'
`,
		Templates: []*alertspb.TemplateDesc{
			{
				Filename: "some-template.tmpl",
				Body:     simpleTemplateOne,
			},
		},
	}
	require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	currentConfigFp, cfgExists = am.cfgs["user1"]
	require.True(t, cfgExists)
	expectedFp = fingerprint(user1Cfg)
	require.Equal(t, expectedFp, currentConfigFp)

	// Test Delete User, ensure config is removed and the resources are freed.
	require.NoError(t, store.DeleteAlertConfig(ctx, "user3"))
	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	_, cfgExists = am.cfgs["user3"]
	require.False(t, cfgExists)

	_, cfgExists = am.alertmanagers["user3"]
	require.False(t, cfgExists)
	dirs = am.getPerUserDirectories()
	require.NotZero(t, dirs["user1"])
	require.NotZero(t, dirs["user2"])
	require.Zero(t, dirs["user3"]) // User3 is deleted, so we should have no more files for it.
	require.False(t, fileExists(t, user3Dir))

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	// Ensure when a 3rd config is re-added, it is synced correctly
	require.NoError(t, store.SetAlertConfig(ctx, user3Cfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	currentConfigFp, cfgExists = am.cfgs["user3"]
	require.True(t, cfgExists)
	expectedFp = fingerprint(user3Cfg)
	require.Equal(t, expectedFp, currentConfigFp)

	_, cfgExists = am.alertmanagers["user3"]
	require.True(t, cfgExists)
	dirs = am.getPerUserDirectories()
	require.NotZero(t, dirs["user1"])
	require.NotZero(t, dirs["user2"])
	require.Equal(t, user3Dir, dirs["user3"]) // Dir should exist, even though state files are not generated yet.

	// Hierarchy that existed before should exist again.
	require.True(t, dirExists(t, user3Dir))

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
		cortex_alertmanager_config_last_reload_successful{user="user3"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	// Removed template files should be cleaned up
	user3Cfg.Templates = []*alertspb.TemplateDesc{
		{
			Filename: "first.tpl",
			Body:     `{{ define "t1" }}Template 1 ... {{end}}`,
		},
	}

	require.NoError(t, store.SetAlertConfig(ctx, user3Cfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	require.True(t, dirExists(t, user3Dir))

	t.Run("when bad config is loaded", func(t *testing.T) {
		require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      "user4",
			RawConfig: badConfig,
			Templates: []*alertspb.TemplateDesc{},
		}))

		err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
			# TYPE cortex_alertmanager_config_last_reload_successful gauge
			cortex_alertmanager_config_last_reload_successful{user="user1"} 1
			cortex_alertmanager_config_last_reload_successful{user="user2"} 1
			cortex_alertmanager_config_last_reload_successful{user="user3"} 1
			cortex_alertmanager_config_last_reload_successful{user="user4"} 0
		`), "cortex_alertmanager_config_last_reload_successful"))

		_, amExists := am.alertmanagers["user4"]
		require.False(t, amExists)
	})

	t.Run("when bad templates are loaded", func(t *testing.T) {
		require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      "user5",
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{
				{Filename: "bad.tmpl", Body: "{{ invalid template }}"},
			},
		}))

		err := am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
			# TYPE cortex_alertmanager_config_last_reload_successful gauge
			cortex_alertmanager_config_last_reload_successful{user="user1"} 1
			cortex_alertmanager_config_last_reload_successful{user="user2"} 1
			cortex_alertmanager_config_last_reload_successful{user="user3"} 1
			cortex_alertmanager_config_last_reload_successful{user="user4"} 0
			cortex_alertmanager_config_last_reload_successful{user="user5"} 0
		`), "cortex_alertmanager_config_last_reload_successful"))

		_, amExists := am.alertmanagers["user5"]
		require.False(t, amExists)
	})
}

func TestMultitenantAlertmanager_FirewallShouldBlockHTTPBasedReceiversWhenEnabled(t *testing.T) {
	tests := map[string]struct {
		getAlertmanagerConfig func(backendURL string) string
	}{
		"webhook": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: webhook
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: webhook
    webhook_configs:
      - url: %s
`, backendURL)
			},
		},
		"pagerduty": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: pagerduty
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: pagerduty
    pagerduty_configs:
      - url: %s
        routing_key: secret
`, backendURL)
			},
		},
		"slack": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: slack
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: slack
    slack_configs:
      - api_url: %s
        channel: test
`, backendURL)
			},
		},
		"opsgenie": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: opsgenie
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: opsgenie
    opsgenie_configs:
      - api_url: %s
        api_key: secret
`, backendURL)
			},
		},
		"wechat": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: wechat
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: wechat
    wechat_configs:
      - api_url: %s
        api_secret: secret
        corp_id: babycorp
`, backendURL)
			},
		},
		"sns": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: sns
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: sns
    sns_configs:
      - api_url: %s
        topic_arn: arn:aws:sns:us-east-1:123456789012:MyTopic
        sigv4:
          region: us-east-1
          access_key: xxx
          secret_key: xxx
`, backendURL)
			},
		},
		"telegram": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: telegram
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: telegram
    telegram_configs:
      - api_url: %s
        bot_token: xxx
        chat_id: 111
`, backendURL)
			},
		},
		"discord": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: discord
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: discord
    discord_configs:
      - webhook_url: %s
`, backendURL)
			},
		},
		"webex": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: webex
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: webex
    webex_configs:
      - api_url: %s
        room_id: test
        http_config:
          authorization:
            type: Bearer
            credentials: secret
`, backendURL)
			},
		},
		"msteams": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: msteams
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: msteams
    msteams_configs:
      - webhook_url: %s
`, backendURL)
			},
		},
		// We expect requests against the HTTP proxy to be blocked too.
		"HTTP proxy": {
			getAlertmanagerConfig: func(backendURL string) string {
				return fmt.Sprintf(`
route:
  receiver: webhook
  group_wait: 0s
  group_interval: 1s

receivers:
  - name: webhook
    webhook_configs:
      - url: https://www.google.com
        http_config:
          proxy_url: %s
`, backendURL)
			},
		},
	}

	for receiverName, testData := range tests {
		for _, firewallEnabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("receiver=%s firewall enabled=%v", receiverName, firewallEnabled), func(t *testing.T) {
				t.Parallel()

				ctx := context.Background()
				userID := "user-1"
				serverInvoked := atomic.NewBool(false)

				// Create a local HTTP server to test whether the request is received.
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
					serverInvoked.Store(true)
					writer.WriteHeader(http.StatusOK)
				}))
				defer server.Close()

				// Create the alertmanager config.
				alertmanagerCfg := testData.getAlertmanagerConfig(fmt.Sprintf("http://%s", server.Listener.Addr().String()))

				// Store the alertmanager config in the bucket.
				store := prepareInMemoryAlertStore()
				require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
					User:      userID,
					RawConfig: alertmanagerCfg,
				}))

				// Prepare the alertmanager config.
				cfg := mockAlertmanagerConfig(t)

				// Prepare the limits config.
				var limits validation.Limits
				flagext.DefaultValues(&limits)
				limits.AlertmanagerReceiversBlockPrivateAddresses = firewallEnabled

				overrides := validation.NewOverrides(limits, nil)
				features := featurecontrol.NoopFlags{}

				// Start the alertmanager.
				reg := prometheus.NewPedanticRegistry()
				logs := &concurrency.SyncBuffer{}
				logger := log.NewLogfmtLogger(logs)
				am := setupSingleMultitenantAlertmanager(t, cfg, store, overrides, features, logger, reg)

				// Ensure the configs are synced correctly.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user-1"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

				// Create an alert to push.
				alerts := alert.Alerts(&alert.Alert{
					Alert: model.Alert{
						Labels:   map[model.LabelName]model.LabelValue{model.AlertNameLabel: "test"},
						StartsAt: time.Now().Add(-time.Minute),
						EndsAt:   time.Now().Add(time.Minute),
					},
					UpdatedAt: time.Now(),
					Timeout:   false,
				})

				alertsPayload, err := json.Marshal(alerts)
				require.NoError(t, err)

				// Push an alert.
				req := httptest.NewRequest(http.MethodPost, cfg.ExternalURL.String()+"/api/v2/alerts", bytes.NewReader(alertsPayload))
				req.Header.Set("content-type", "application/json")
				reqCtx := user.InjectOrgID(req.Context(), userID)
				{
					w := httptest.NewRecorder()
					am.ServeHTTP(w, req.WithContext(reqCtx))

					resp := w.Result()
					_, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					assert.Equal(t, http.StatusOK, w.Code)
				}

				// Ensure the server endpoint has not been called if firewall is enabled. Since the alert is delivered
				// asynchronously, we should pool it for a short period.
				deadline := time.Now().Add(3 * time.Second)
				for !time.Now().After(deadline) && !serverInvoked.Load() {

					time.Sleep(100 * time.Millisecond)
				}

				assert.Equal(t, !firewallEnabled, serverInvoked.Load())

				// Print all alertmanager logs to have more information if this test fails in CI.
				t.Logf("Alertmanager logs:\n%s", logs.String())
			})
		}
	}
}

func fileExists(t *testing.T, path string) bool {
	return checkExists(t, path, false)
}

func dirExists(t *testing.T, path string) bool {
	return checkExists(t, path, true)
}

func checkExists(t *testing.T, path string, dir bool) bool {
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		require.NoError(t, err)
	}

	require.Equal(t, dir, fi.IsDir())
	return true
}

func TestMultitenantAlertmanager_deleteUnusedLocalUserState(t *testing.T) {
	ctx := context.Background()

	const (
		user1 = "user1"
		user2 = "user2"
	)

	store := prepareInMemoryAlertStore()
	require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User:      user2,
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{},
	}))

	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am := setupSingleMultitenantAlertmanager(t, cfg, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	createFile(t, filepath.Join(cfg.DataDir, user1, notificationLogSnapshot))
	createFile(t, filepath.Join(cfg.DataDir, user1, silencesSnapshot))
	createFile(t, filepath.Join(cfg.DataDir, user2, notificationLogSnapshot))
	createFile(t, filepath.Join(cfg.DataDir, user2, templatesDir, "template.tpl"))

	dirs := am.getPerUserDirectories()
	require.Equal(t, 2, len(dirs))
	require.NotZero(t, dirs[user1])
	require.NotZero(t, dirs[user2])

	// Ensure the configs are synced correctly
	err := am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	// loadAndSyncConfigs also cleans up obsolete files. Let's verify that.
	dirs = am.getPerUserDirectories()

	require.Zero(t, dirs[user1])    // has no configuration, files were deleted
	require.NotZero(t, dirs[user2]) // has config, files survived
}

func TestMultitenantAlertmanager_zoneAwareSharding(t *testing.T) {
	ctx := context.Background()
	alertStore := prepareInMemoryAlertStore()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	const (
		user1 = "user1"
		user2 = "user2"
		user3 = "user3"
	)

	createInstance := func(i int, zone string, registries *dskit_metrics.TenantRegistries) *MultitenantAlertmanager {
		reg := prometheus.NewPedanticRegistry()
		cfg := mockAlertmanagerConfig(t)
		instanceID := fmt.Sprintf("instance-%d", i)
		registries.AddTenantRegistry(instanceID, reg)

		cfg.ShardingRing.ReplicationFactor = 2
		cfg.ShardingRing.Common.InstanceID = instanceID
		cfg.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.1-%d", i)
		cfg.ShardingRing.ZoneAwarenessEnabled = true
		cfg.ShardingRing.InstanceZone = zone

		am, err := createMultitenantAlertmanager(cfg, nil, alertStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewLogfmtLogger(os.Stdout), reg)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, am))
		})
		require.NoError(t, services.StartAndAwaitRunning(ctx, am))

		return am
	}

	registriesZoneA := dskit_metrics.NewTenantRegistries(log.NewNopLogger())
	registriesZoneB := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

	am1ZoneA := createInstance(1, "zoneA", registriesZoneA)
	am2ZoneA := createInstance(2, "zoneA", registriesZoneA)
	am1ZoneB := createInstance(3, "zoneB", registriesZoneB)
	allInstances := []*MultitenantAlertmanager{am1ZoneA, am2ZoneA, am1ZoneB}

	// Wait until every alertmanager has updated the ring, in order to get stable tests.
	require.Eventually(t, func() bool {
		for _, am := range allInstances {
			set, err := am.ring.GetAllHealthy(SyncRingOp)
			if err != nil || len(set.Instances) != len(allInstances) {
				return false
			}
		}

		return true
	}, 2*time.Second, 10*time.Millisecond)

	{
		require.NoError(t, alertStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      user1,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      user2,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      user3,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))

		err := am1ZoneA.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am2ZoneA.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am1ZoneB.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
	}

	metricsZoneA := registriesZoneA.BuildMetricFamiliesPerTenant()
	metricsZoneB := registriesZoneB.BuildMetricFamiliesPerTenant()

	assert.Equal(t, float64(3), metricsZoneA.GetSumOfGauges("cortex_alertmanager_tenants_owned"))
	assert.Equal(t, float64(3), metricsZoneB.GetSumOfGauges("cortex_alertmanager_tenants_owned"))
}

func TestMultitenantAlertmanager_deleteUnusedRemoteUserState(t *testing.T) {
	ctx := context.Background()

	const (
		user1 = "user1"
		user2 = "user2"
	)

	alertStore := prepareInMemoryAlertStore()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	createInstance := func(i int) *MultitenantAlertmanager {
		reg := prometheus.NewPedanticRegistry()
		cfg := mockAlertmanagerConfig(t)

		cfg.ShardingRing.ReplicationFactor = 1
		cfg.ShardingRing.Common.InstanceID = fmt.Sprintf("instance-%d", i)
		cfg.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.1-%d", i)

		// Increase state write interval so that state gets written sooner, making test faster.
		cfg.Persister.Interval = 500 * time.Millisecond

		am, err := createMultitenantAlertmanager(cfg, nil, alertStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewLogfmtLogger(os.Stdout), reg)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, am))
		})
		require.NoError(t, services.StartAndAwaitRunning(ctx, am))

		return am
	}

	// Create two instances. With replication factor of 1, this means that only one
	// of the instances will own the user. This tests that an instance does not delete
	// state for users that are configured, but are owned by other instances.
	am1 := createInstance(1)
	am2 := createInstance(2)

	// Configure the users and wait for the state persister to write some state for both.
	{
		require.NoError(t, alertStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      user1,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      user2,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))

		err := am1.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am2.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, err1 := alertStore.GetFullState(context.Background(), user1)
			_, err2 := alertStore.GetFullState(context.Background(), user2)
			return err1 == nil && err2 == nil
		}, 5*time.Second, 100*time.Millisecond, "timed out waiting for state to be persisted")
	}

	// Perform another sync to trigger cleanup; this should have no effect.
	{
		err := am1.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am2.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		_, err = alertStore.GetFullState(context.Background(), user1)
		require.NoError(t, err)
		_, err = alertStore.GetFullState(context.Background(), user2)
		require.NoError(t, err)
	}

	// Delete one configuration and trigger cleanup; state for only that user should be deleted.
	{
		require.NoError(t, alertStore.DeleteAlertConfig(ctx, user1))

		err := am1.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am2.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		_, err = alertStore.GetFullState(context.Background(), user1)
		require.Equal(t, alertspb.ErrNotFound, err)
		_, err = alertStore.GetFullState(context.Background(), user2)
		require.NoError(t, err)
	}
}

func TestMultitenantAlertmanager_deleteUnusedRemoteUserStateDisabled(t *testing.T) {
	ctx := context.Background()

	const (
		user1 = "user1"
		user2 = "user2"
	)

	alertStore := prepareInMemoryAlertStore()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	createInstance := func(i int) *MultitenantAlertmanager {
		reg := prometheus.NewPedanticRegistry()
		cfg := mockAlertmanagerConfig(t)

		cfg.ShardingRing.ReplicationFactor = 1
		cfg.ShardingRing.Common.InstanceID = fmt.Sprintf("instance-%d", i)
		cfg.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.1-%d", i)

		// Increase state write interval so that state gets written sooner, making test faster.
		cfg.Persister.Interval = 500 * time.Millisecond

		// Disable state cleanup.
		cfg.EnableStateCleanup = false

		am, err := createMultitenantAlertmanager(cfg, nil, alertStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewLogfmtLogger(os.Stdout), reg)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, am))
		})
		require.NoError(t, services.StartAndAwaitRunning(ctx, am))

		return am
	}

	// Create two instances. With replication factor of 1, this means that only one
	// of the instances will own the user. This tests that an instance does not delete
	// state for users that are configured, but are owned by other instances.
	am1 := createInstance(1)
	am2 := createInstance(2)

	// Configure the users and wait for the state persister to write some state for both.
	{
		require.NoError(t, alertStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      user1,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
			User:      user2,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))

		err := am1.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am2.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			_, err1 := alertStore.GetFullState(context.Background(), user1)
			_, err2 := alertStore.GetFullState(context.Background(), user2)
			return err1 == nil && err2 == nil
		}, 5*time.Second, 100*time.Millisecond, "timed out waiting for state to be persisted")
	}

	// Perform another sync to trigger cleanup; this should have no effect.
	{
		err := am1.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am2.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		_, err = alertStore.GetFullState(context.Background(), user1)
		require.NoError(t, err)
		_, err = alertStore.GetFullState(context.Background(), user2)
		require.NoError(t, err)
	}

	// Delete one configuration and trigger cleanup; state should not be deleted.
	{
		require.NoError(t, alertStore.DeleteAlertConfig(ctx, user1))

		err := am1.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)
		err = am2.loadAndSyncConfigs(context.Background(), reasonPeriodic)
		require.NoError(t, err)

		_, err = alertStore.GetFullState(context.Background(), user1)
		require.NoError(t, err)
		_, err = alertStore.GetFullState(context.Background(), user2)
		require.NoError(t, err)
	}
}

func createFile(t *testing.T, path string) string {
	dir := filepath.Dir(path)
	require.NoError(t, os.MkdirAll(dir, 0777))
	f, err := os.Create(path)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return path
}

func TestMultitenantAlertmanager_ServeHTTP(t *testing.T) {
	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()

	amConfig := mockAlertmanagerConfig(t)

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	amConfig.ExternalURL = externalURL

	// Create the Multitenant Alertmanager.
	reg := prometheus.NewPedanticRegistry()
	am := setupSingleMultitenantAlertmanager(t, amConfig, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	// We hit a real API endpoint (the alertmanager v0.32.0 dropped the UI, so we
	// can no longer rely on the redirect-to-UI 301 to confirm the per-tenant
	// alertmanager is reachable). /api/v2/status is served by the AM and returns
	// 200 when the request was correctly routed to a live alertmanager instance.
	req := httptest.NewRequest("GET", externalURL.String()+"/api/v2/status", nil)
	ctx := user.InjectOrgID(req.Context(), "user1")

	// Request when fallback user configuration is used, as user hasn't created a
	// configuration yet — the AM should still answer with the fallback config.
	{
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		require.Equal(t, http.StatusOK, w.Code)
	}

	// Create a configuration for the user in storage.
	require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigTwo,
		Templates: []*alertspb.TemplateDesc{},
	}))

	// Make the alertmanager pick it up.
	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	// Request when AM is active with the user's config.
	{
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		require.Equal(t, http.StatusOK, w.Code)
	}

	// Verify that GET /metrics returns 404 even when AM is active.
	{
		metricURL := externalURL.String() + "/metrics"
		require.Equal(t, "http://localhost:8080/alertmanager/metrics", metricURL)
		verify404(ctx, t, am, "GET", metricURL)
	}

	// Verify that POST /-/reload returns 404 even when AM is active.
	{
		metricURL := externalURL.String() + "/-/reload"
		require.Equal(t, "http://localhost:8080/alertmanager/-/reload", metricURL)
		verify404(ctx, t, am, "POST", metricURL)
	}

	// Verify that GET /debug/index returns 404 even when AM is active.
	{
		// Register pprof Index (under non-standard path, but this path is exposed by AM using default MUX!)
		http.HandleFunc("/alertmanager/debug/index", pprof.Index)

		metricURL := externalURL.String() + "/debug/index"
		require.Equal(t, "http://localhost:8080/alertmanager/debug/index", metricURL)
		verify404(ctx, t, am, "GET", metricURL)
	}

	// Remove the tenant's Alertmanager
	require.NoError(t, store.DeleteAlertConfig(ctx, "user1"))
	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	{
		// Request when the alertmanager is gone should result in the multitenant
		// AM falling back to the default config and answering the API request.
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		require.Equal(t, http.StatusOK, w.Code)
	}
}

func verify404(ctx context.Context, t *testing.T, am *MultitenantAlertmanager, method string, url string) {
	metricsReq := httptest.NewRequest(method, url, strings.NewReader("Hello")) // Body for POST Request.
	w := httptest.NewRecorder()
	am.ServeHTTP(w, metricsReq.WithContext(ctx))

	require.Equal(t, 404, w.Code)
}

func TestMultitenantAlertmanager_ServeHTTPWithFallbackConfig(t *testing.T) {
	ctx := context.Background()
	amConfig := mockAlertmanagerConfig(t)

	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	fallbackCfg := `
global:
  smtp_smarthost: 'localhost:25'
  smtp_from: 'youraddress@example.org'
route:
  receiver: example-email
receivers:
  - name: example-email
    email_configs:
    - to: 'youraddress@example.org'
`
	amConfig.ExternalURL = externalURL

	// Create the Multitenant Alertmanager.
	am := setupSingleMultitenantAlertmanager(t, amConfig, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), nil)
	require.NoError(t, err)
	am.fallbackConfig = fallbackCfg

	// Request when no user configuration is present.
	req := httptest.NewRequest("GET", externalURL.String()+"/api/v2/status", nil)
	w := httptest.NewRecorder()

	am.ServeHTTP(w, req.WithContext(user.InjectOrgID(req.Context(), "user1")))

	resp := w.Result()

	// It succeeds and the Alertmanager is started.
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, am.alertmanagers, 1)
	_, exists := am.alertmanagers["user1"]
	require.True(t, exists)

	// Even after a poll...
	err = am.loadAndSyncConfigs(ctx, reasonPeriodic)
	require.NoError(t, err)

	//  It does not remove the Alertmanager.
	require.Len(t, am.alertmanagers, 1)
	_, exists = am.alertmanagers["user1"]
	require.True(t, exists)

	// Remove the Alertmanager configuration.
	require.NoError(t, store.DeleteAlertConfig(ctx, "user1"))
	err = am.loadAndSyncConfigs(ctx, reasonPeriodic)
	require.NoError(t, err)

	// Even after removing it.. We start it again with the fallback configuration.
	w = httptest.NewRecorder()
	am.ServeHTTP(w, req.WithContext(user.InjectOrgID(req.Context(), "user1")))

	resp = w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMultitenantAlertmanager_ServeHTTPWithStrictInitialization(t *testing.T) {
	const testUser = "user"

	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()

	amConfig := mockAlertmanagerConfig(t)
	amConfig.StrictInitializationEnabled = true

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)
	amConfig.ExternalURL = externalURL

	// Create the Multitenant Alertmanager.
	reg := prometheus.NewPedanticRegistry()
	am := setupSingleMultitenantAlertmanager(t, amConfig, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	// Create a tenant with an empty config - it should be skipped by the MOA.
	ctx := context.Background()
	require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User: testUser,
	}))

	// Sync configurations - the Alertmanager shouldn't be initialized.
	err = am.loadAndSyncConfigs(ctx, reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 0)

	// Make requests as the users - the Alertmanager should be initialized.
	req := httptest.NewRequest("GET", externalURL.String()+"/api/v2/status", nil)
	w := httptest.NewRecorder()

	require.NoError(t, err)
	am.ServeHTTP(w, req.WithContext(user.InjectOrgID(req.Context(), testUser)))
	require.Equal(t, http.StatusOK, w.Result().StatusCode)
	require.Len(t, am.alertmanagers, 1)

	// Set the idle period to 0 - the Alertmanager should be turned off after the next sync.
	am.cfg.StrictInitializationIdleGracePeriod = 0
	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 0)
}

// This test checks that the fallback configuration does not overwrite a configuration
// written to storage before it is picked up by the instance.
func TestMultitenantAlertmanager_ServeHTTPBeforeSyncFailsIfConfigExists(t *testing.T) {
	ctx := context.Background()
	amConfig := mockAlertmanagerConfig(t)

	// Prevent polling configurations, we want to test the window of time
	// between the configuration existing and it being incorporated.
	amConfig.PollInterval = time.Hour

	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()

	externalURL := flagext.URLValue{}
	err := externalURL.Set("http://localhost:8080/alertmanager")
	require.NoError(t, err)

	fallbackCfg := `
global:
  smtp_smarthost: 'localhost:25'
  smtp_from: 'youraddress@example.org'
route:
  receiver: example-email
receivers:
  - name: example-email
    email_configs:
    - to: 'youraddress@example.org'
`
	amConfig.ExternalURL = externalURL

	// Create the Multitenant Alertmanager.
	am := setupSingleMultitenantAlertmanager(t, amConfig, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), nil)
	am.fallbackConfig = fallbackCfg

	// Upload config for the user.
	require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{},
	}))

	// Request before the user configuration loaded by polling loop.
	req := httptest.NewRequest("GET", externalURL.String()+"/api/v2/status", nil)
	w := httptest.NewRecorder()
	am.ServeHTTP(w, req.WithContext(user.InjectOrgID(req.Context(), "user1")))
	resp := w.Result()
	assert.Equal(t, http.StatusNotAcceptable, resp.StatusCode)

	// Check the configuration has not been replaced.
	readConfigDesc, err := store.GetAlertConfig(ctx, "user1")
	require.NoError(t, err)
	assert.Equal(t, simpleConfigOne, readConfigDesc.RawConfig)

	// We expect the request to fail because the user has already uploaded a
	// configuration and should not replace it.
	assert.Len(t, am.alertmanagers, 0)

	// Now force a poll to actually load the configuration.
	err = am.loadAndSyncConfigs(ctx, reasonPeriodic)
	require.NoError(t, err)

	// Now it should exist. This is to sanity check the test is working as expected, and
	// that the user configuration was written correctly such that it can be picked up.
	require.Len(t, am.alertmanagers, 1)
	_, exists := am.alertmanagers["user1"]
	require.True(t, exists)

	// Request should now succeed.
	w = httptest.NewRecorder()
	am.ServeHTTP(w, req.WithContext(user.InjectOrgID(req.Context(), "user1")))
	resp = w.Result()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMultitenantAlertmanager_InitialSync(t *testing.T) {
	tc := []struct {
		name          string
		existing      bool
		initialState  ring.InstanceState
		initialTokens ring.Tokens
	}{
		{
			name:     "with no instance in the ring",
			existing: false,
		},
		{
			name:          "with an instance already in the ring with PENDING state and no tokens",
			existing:      true,
			initialState:  ring.PENDING,
			initialTokens: ring.Tokens{},
		},
		{
			name:          "with an instance already in the ring with JOINING state and some tokens",
			existing:      true,
			initialState:  ring.JOINING,
			initialTokens: ring.Tokens{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name:          "with an instance already in the ring with ACTIVE state and all tokens",
			existing:      true,
			initialState:  ring.ACTIVE,
			initialTokens: ring.NewRandomTokenGenerator().GenerateTokens(128, nil),
		},
		{
			name:          "with an instance already in the ring with LEAVING state and all tokens",
			existing:      true,
			initialState:  ring.LEAVING,
			initialTokens: ring.Tokens{100000},
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			amConfig := mockAlertmanagerConfig(t)
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			// Use an alert store with a mocked backend.
			bkt := &bucket.ClientMock{}
			alertStore := bucketclient.NewBucketAlertStore(bkt, nil, log.NewNopLogger())

			// Setup the initial instance state in the ring.
			if tt.existing {
				require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
					ringDesc := ring.GetOrCreateRingDesc(in)
					ringDesc.AddIngester(amConfig.ShardingRing.Common.InstanceID, amConfig.ShardingRing.Common.InstanceAddr, "", tt.initialTokens, tt.initialState, time.Now(), false, time.Time{}, nil)
					return ringDesc, true, nil
				}))
			}

			am, err := createMultitenantAlertmanager(amConfig, nil, alertStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), nil)
			require.NoError(t, err)
			defer services.StopAndAwaitTerminated(ctx, am) //nolint:errcheck

			// Before being registered in the ring.
			require.False(t, am.ringLifecycler.IsRegistered())
			require.Equal(t, ring.PENDING.String(), am.ringLifecycler.GetState().String())
			require.Equal(t, 0, len(am.ringLifecycler.GetTokens()))
			require.Equal(t, ring.Tokens{}, am.ringLifecycler.GetTokens())

			// During the initial sync, we expect two things. That the instance is already
			// registered with the ring (meaning we have tokens) and that its state is JOINING.
			bkt.MockIterWithCallback("alerts/", nil, nil, func() {
				require.True(t, am.ringLifecycler.IsRegistered())
				require.Equal(t, ring.JOINING.String(), am.ringLifecycler.GetState().String())
			})
			bkt.MockIter("alertmanager/", nil, nil)

			// Once successfully started, the instance should be ACTIVE in the ring.
			require.NoError(t, services.StartAndAwaitRunning(ctx, am))

			// After being registered in the ring.
			require.True(t, am.ringLifecycler.IsRegistered())
			require.Equal(t, ring.ACTIVE.String(), am.ringLifecycler.GetState().String())
			require.Equal(t, 128, len(am.ringLifecycler.GetTokens()))
			require.Subset(t, am.ringLifecycler.GetTokens(), tt.initialTokens)
		})
	}
}

func TestMultitenantAlertmanager_PerTenantSharding(t *testing.T) {
	tc := []struct {
		name              string
		tenantShardSize   int
		replicationFactor int
		instances         int
		configs           int
		expectedTenants   int
	}{
		{
			name:              "1 instance, RF = 1",
			instances:         1,
			replicationFactor: 1,
			configs:           10,
			expectedTenants:   10, // same as no sharding and 1 instance
		},
		{
			name:              "2 instances, RF = 1",
			instances:         2,
			replicationFactor: 1,
			configs:           10,
			expectedTenants:   10, // configs * replication factor
		},
		{
			name:              "3 instances, RF = 2",
			instances:         3,
			replicationFactor: 2,
			configs:           10,
			expectedTenants:   20, // configs * replication factor
		},
		{
			name:              "5 instances, RF = 3",
			instances:         5,
			replicationFactor: 3,
			configs:           10,
			expectedTenants:   30, // configs * replication factor
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			alertStore := prepareInMemoryAlertStore()

			var instances []*MultitenantAlertmanager
			var instanceIDs []string
			registries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

			// First, add the number of configs to the store.
			for i := 1; i <= tt.configs; i++ {
				u := fmt.Sprintf("u-%d", i)
				require.NoError(t, alertStore.SetAlertConfig(context.Background(), &alertspb.AlertConfigDesc{
					User:      u,
					RawConfig: simpleConfigOne,
					Templates: []*alertspb.TemplateDesc{},
				}))
			}

			// Then, create the alertmanager instances, start them and add their registries to the slice.
			for i := 1; i <= tt.instances; i++ {
				instanceIDs = append(instanceIDs, fmt.Sprintf("alertmanager-%d", i))
				instanceID := fmt.Sprintf("alertmanager-%d", i)

				amConfig := mockAlertmanagerConfig(t)
				amConfig.ShardingRing.ReplicationFactor = tt.replicationFactor
				amConfig.ShardingRing.Common.InstanceID = instanceID
				amConfig.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
				// Do not check the ring topology changes or poll in an interval in this test (we explicitly sync alertmanagers).
				amConfig.PollInterval = time.Hour
				amConfig.ShardingRing.RingCheckPeriod = time.Hour

				reg := prometheus.NewPedanticRegistry()
				am, err := createMultitenantAlertmanager(amConfig, nil, alertStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)
				require.NoError(t, err)
				defer services.StopAndAwaitTerminated(ctx, am) //nolint:errcheck

				require.NoError(t, services.StartAndAwaitRunning(ctx, am))

				instances = append(instances, am)
				instanceIDs = append(instanceIDs, instanceID)
				registries.AddTenantRegistry(instanceID, reg)
			}

			// We need make sure the ring is settled.
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			// The alertmanager is ready to be tested once all instances are ACTIVE and the ring settles.
			for _, am := range instances {
				for _, id := range instanceIDs {
					require.NoError(t, ring.WaitInstanceState(ctx, am.ring, id, ring.ACTIVE))
				}
			}

			// Now that the ring has settled, sync configs with the instances.
			var numConfigs, numInstances int
			for _, am := range instances {
				err := am.loadAndSyncConfigs(ctx, reasonRingChange)
				require.NoError(t, err)
				numConfigs += len(am.cfgs)
				numInstances += len(am.alertmanagers)
			}

			metrics := registries.BuildMetricFamiliesPerTenant()
			assert.Equal(t, tt.expectedTenants, numConfigs)
			assert.Equal(t, tt.expectedTenants, numInstances)
			assert.Equal(t, float64(tt.expectedTenants), metrics.GetSumOfGauges("cortex_alertmanager_tenants_owned"))
			assert.Equal(t, float64(tt.configs*tt.instances), metrics.GetSumOfGauges("cortex_alertmanager_tenants_discovered"))
		})
	}
}

func TestMultitenantAlertmanager_SyncOnRingTopologyChanges(t *testing.T) {
	registeredAt := time.Now()

	tc := []struct {
		name       string
		setupRing  func(desc *ring.Desc)
		updateRing func(desc *ring.Desc)
		expected   bool
	}{
		{
			name: "when an instance is added to the ring",
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			updateRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			expected: true,
		},
		{
			name: "when an instance is removed from the ring",
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				desc.AddIngester("alertmanager-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			updateRing: func(desc *ring.Desc) {
				desc.RemoveIngester("alertmanager-1")
			},
			expected: true,
		},
		{
			name: "should sync when an instance changes state",
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				desc.AddIngester("alertmanager-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.JOINING, registeredAt, false, time.Time{}, nil)
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["alertmanager-2"]
				instance.State = ring.ACTIVE
				desc.Ingesters["alertmanager-2"] = instance
			},
			expected: true,
		},
		{
			name: "should sync when an healthy instance becomes unhealthy",
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				desc.AddIngester("alertmanager-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["alertmanager-1"]
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["alertmanager-1"] = instance
			},
			expected: true,
		},
		{
			name: "should sync when an unhealthy instance becomes healthy",
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)

				instance := desc.AddIngester("alertmanager-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["alertmanager-2"] = instance
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["alertmanager-2"]
				instance.Timestamp = time.Now().Unix()
				desc.Ingesters["alertmanager-2"] = instance
			},
			expected: true,
		},
		{
			name: "should NOT sync when an instance updates the heartbeat",
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				desc.AddIngester("alertmanager-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
			},
			updateRing: func(desc *ring.Desc) {
				instance := desc.Ingesters["alertmanager-1"]
				instance.Timestamp = time.Now().Add(time.Second).Unix()
				desc.Ingesters["alertmanager-1"] = instance
			},
			expected: false,
		},
		{
			name: "should NOT sync when an instance is auto-forgotten in the ring but was already unhealthy in the previous state",
			setupRing: func(desc *ring.Desc) {
				desc.AddIngester("alertmanager-1", "127.0.0.1", "", ring.Tokens{1, 2, 3}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)
				desc.AddIngester("alertmanager-2", "127.0.0.2", "", ring.Tokens{4, 5, 6}, ring.ACTIVE, registeredAt, false, time.Time{}, nil)

				instance := desc.Ingesters["alertmanager-2"]
				instance.Timestamp = time.Now().Add(-time.Hour).Unix()
				desc.Ingesters["alertmanager-2"] = instance
			},
			updateRing: func(desc *ring.Desc) {
				desc.RemoveIngester("alertmanager-2")
			},
			expected: false,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			amConfig := mockAlertmanagerConfig(t)
			amConfig.ShardingRing.RingCheckPeriod = 100 * time.Millisecond
			amConfig.PollInterval = time.Hour // Don't trigger the periodic check.

			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			alertStore := prepareInMemoryAlertStore()

			reg := prometheus.NewPedanticRegistry()
			am, err := createMultitenantAlertmanager(amConfig, nil, alertStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)
			require.NoError(t, err)

			require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
				ringDesc := ring.GetOrCreateRingDesc(in)
				tt.setupRing(ringDesc)
				return ringDesc, true, nil
			}))

			require.NoError(t, services.StartAndAwaitRunning(ctx, am))
			defer services.StopAndAwaitTerminated(ctx, am) //nolint:errcheck

			// Make sure the initial sync happened.
			regs := dskit_metrics.NewTenantRegistries(log.NewNopLogger())
			regs.AddTenantRegistry("test", reg)
			metrics := regs.BuildMetricFamiliesPerTenant()
			assert.Equal(t, float64(1), metrics.GetSumOfCounters("cortex_alertmanager_sync_configs_total"))

			// Change the ring topology.
			require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
				ringDesc := ring.GetOrCreateRingDesc(in)
				tt.updateRing(ringDesc)
				return ringDesc, true, nil
			}))

			// Assert if we expected an additional sync or not.
			expectedSyncs := 1
			if tt.expected {
				expectedSyncs++
			}
			test.Poll(t, 3*time.Second, float64(expectedSyncs), func() interface{} {
				metrics := regs.BuildMetricFamiliesPerTenant()
				return metrics.GetSumOfCounters("cortex_alertmanager_sync_configs_total")
			})
		})
	}
}

func TestMultitenantAlertmanager_RingLifecyclerShouldAutoForgetUnhealthyInstances(t *testing.T) {
	const unhealthyInstanceID = "alertmanager-bad-1"
	const heartbeatTimeout = time.Minute
	ctx := context.Background()
	amConfig := mockAlertmanagerConfig(t)
	amConfig.ShardingRing.Common.HeartbeatPeriod = 100 * time.Millisecond
	amConfig.ShardingRing.Common.HeartbeatTimeout = heartbeatTimeout

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	alertStore := prepareInMemoryAlertStore()

	am, err := createMultitenantAlertmanager(amConfig, nil, alertStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, am))
	defer services.StopAndAwaitTerminated(ctx, am) //nolint:errcheck

	require.NoError(t, ringStore.CAS(ctx, RingKey, func(in interface{}) (interface{}, bool, error) {
		ringDesc := ring.GetOrCreateRingDesc(in)
		instance := ringDesc.AddIngester(unhealthyInstanceID, "127.0.0.1", "", ring.NewRandomTokenGenerator().GenerateTokens(RingNumTokens, nil), ring.ACTIVE, time.Now(), false, time.Time{}, nil)
		instance.Timestamp = time.Now().Add(-(ringAutoForgetUnhealthyPeriods + 1) * heartbeatTimeout).Unix()
		ringDesc.Ingesters[unhealthyInstanceID] = instance

		return ringDesc, true, nil
	}))

	test.Poll(t, time.Second, false, func() interface{} {
		d, err := ringStore.Get(ctx, RingKey)
		if err != nil {
			return err
		}

		_, ok := ring.GetOrCreateRingDesc(d).Ingesters[unhealthyInstanceID]
		return ok
	})
}

func TestMultitenantAlertmanager_InitialSyncFailure(t *testing.T) {
	ctx := context.Background()
	amConfig := mockAlertmanagerConfig(t)
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	// Mock the store to fail listing configs.
	bkt := &bucket.ClientMock{}
	bkt.MockIter("alerts/", nil, errors.New("failed to list alerts"))
	bkt.MockIter("alertmanager/", nil, nil)
	store := bucketclient.NewBucketAlertStore(bkt, nil, log.NewNopLogger())

	am, err := createMultitenantAlertmanager(amConfig, nil, store, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), nil)
	require.NoError(t, err)
	defer services.StopAndAwaitTerminated(ctx, am) //nolint:errcheck

	require.NoError(t, am.StartAsync(ctx))
	err = am.AwaitRunning(ctx)
	require.Error(t, err)
	require.Equal(t, services.Failed, am.State())
	require.False(t, am.ringLifecycler.IsRegistered())
	require.NotNil(t, am.ring)
}

func TestAlertmanager_ReplicasPosition(t *testing.T) {
	ctx := context.Background()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mockStore := prepareInMemoryAlertStore()
	require.NoError(t, mockStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User:      "user-1",
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{},
	}))

	var instances []*MultitenantAlertmanager
	var instanceIDs []string
	registries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

	// First, create the alertmanager instances, we'll use a replication factor of 3 and create 3 instances so that we can get the tenant on each replica.
	for i := 1; i <= 3; i++ {
		// instanceIDs = append(instanceIDs, fmt.Sprintf("alertmanager-%d", i))
		instanceID := fmt.Sprintf("alertmanager-%d", i)

		amConfig := mockAlertmanagerConfig(t)
		amConfig.ShardingRing.ReplicationFactor = 3
		amConfig.ShardingRing.Common.InstanceID = instanceID
		amConfig.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)

		// Do not check the ring topology changes or poll in an interval in this test (we explicitly sync alertmanagers).
		amConfig.PollInterval = time.Hour
		amConfig.ShardingRing.RingCheckPeriod = time.Hour

		reg := prometheus.NewPedanticRegistry()
		am, err := createMultitenantAlertmanager(amConfig, nil, mockStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)
		require.NoError(t, err)
		defer services.StopAndAwaitTerminated(ctx, am) //nolint:errcheck

		require.NoError(t, services.StartAndAwaitRunning(ctx, am))

		instances = append(instances, am)
		instanceIDs = append(instanceIDs, instanceID)
		registries.AddTenantRegistry(instanceID, reg)
	}

	// We need make sure the ring is settled. The alertmanager is ready to be tested once all instances are ACTIVE and the ring settles.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for _, am := range instances {
		for _, id := range instanceIDs {
			require.NoError(t, ring.WaitInstanceState(ctx, am.ring, id, ring.ACTIVE))
		}
	}

	// Now that the ring has settled, sync configs with the instances.
	for _, am := range instances {
		err := am.loadAndSyncConfigs(ctx, reasonRingChange)
		require.NoError(t, err)
	}

	// Now that the ring has settled, we expect each AM instance to have a different position.
	// Let's walk through them and collect the positions.
	var positions []int
	for _, instance := range instances {
		instance.alertmanagersMtx.Lock()
		am, ok := instance.alertmanagers["user-1"]
		require.True(t, ok)
		positions = append(positions, am.state.Position())
		instance.alertmanagersMtx.Unlock()
	}

	require.ElementsMatch(t, []int{0, 1, 2}, positions)
}

func TestAlertmanager_StateReplication(t *testing.T) {
	tc := []struct {
		name              string
		replicationFactor int
		instances         int
	}{
		{
			name:              "RF = 1, 1 instance",
			instances:         1,
			replicationFactor: 1,
		},
		{
			name:              "RF = 2, 2 instances",
			instances:         2,
			replicationFactor: 2,
		},
		{
			name:              "RF = 3, 10 instance",
			instances:         10,
			replicationFactor: 3,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			mockStore := prepareInMemoryAlertStore()
			clientPool := newPassthroughAlertmanagerClientPool()
			externalURL := flagext.URLValue{}
			err := externalURL.Set("http://localhost:8080/alertmanager")
			require.NoError(t, err)

			var instances []*MultitenantAlertmanager
			var instanceIDs []string
			registries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

			// First, add the number of configs to the store.
			for i := 1; i <= 12; i++ {
				u := fmt.Sprintf("u-%d", i)
				require.NoError(t, mockStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
					User:      u,
					RawConfig: simpleConfigOne,
					Templates: []*alertspb.TemplateDesc{},
				}))
			}

			// Then, create the alertmanager instances, start them and add their registries to the slice.
			for i := 1; i <= tt.instances; i++ {
				instanceIDs = append(instanceIDs, fmt.Sprintf("alertmanager-%d", i))
				instanceID := fmt.Sprintf("alertmanager-%d", i)

				amConfig := mockAlertmanagerConfig(t)
				amConfig.ExternalURL = externalURL
				amConfig.ShardingRing.ReplicationFactor = tt.replicationFactor
				amConfig.ShardingRing.Common.InstanceID = instanceID
				amConfig.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)

				// Do not check the ring topology changes or poll in an interval in this test (we explicitly sync alertmanagers).
				amConfig.PollInterval = time.Hour
				amConfig.ShardingRing.RingCheckPeriod = time.Hour

				reg := prometheus.NewPedanticRegistry()
				am, err := createMultitenantAlertmanager(amConfig, nil, mockStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)
				require.NoError(t, err)
				defer services.StopAndAwaitTerminated(ctx, am) //nolint:errcheck

				clientPool.setServer(amConfig.ShardingRing.Common.InstanceAddr+":0", am)
				am.alertmanagerClientsPool = clientPool

				require.NoError(t, services.StartAndAwaitRunning(ctx, am))

				instances = append(instances, am)
				instanceIDs = append(instanceIDs, instanceID)
				registries.AddTenantRegistry(instanceID, reg)
			}

			// We need make sure the ring is settled.
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			// The alertmanager is ready to be tested once all instances are ACTIVE and the ring settles.
			for _, am := range instances {
				for _, id := range instanceIDs {
					require.NoError(t, ring.WaitInstanceState(ctx, am.ring, id, ring.ACTIVE))
				}
			}

			// Now that the ring has settled, sync configs with the instances.
			var numConfigs, numInstances int
			for _, am := range instances {
				err := am.loadAndSyncConfigs(ctx, reasonRingChange)
				require.NoError(t, err)
				numConfigs += len(am.cfgs)
				numInstances += len(am.alertmanagers)
			}

			// 1. First, get a random multitenant instance
			//    We must pick an instance which actually has a user configured.
			var multitenantAM *MultitenantAlertmanager
			for {
				multitenantAM = instances[rand.Intn(len(instances))]

				multitenantAM.alertmanagersMtx.Lock()
				amount := len(multitenantAM.alertmanagers)
				multitenantAM.alertmanagersMtx.Unlock()
				if amount > 0 {
					break
				}
			}

			// 2. Then, get a random user that exists in that particular alertmanager instance.
			multitenantAM.alertmanagersMtx.Lock()
			require.Greater(t, len(multitenantAM.alertmanagers), 0)
			k := rand.Intn(len(multitenantAM.alertmanagers))
			var userID string
			for u := range multitenantAM.alertmanagers {
				if k == 0 {
					userID = u
					break
				}
				k--
			}
			multitenantAM.alertmanagersMtx.Unlock()

			// 3. Now that we have our alertmanager user, let's create a silence and make sure it is replicated.
			silence := struct {
				Matchers  labels.Matchers `json:"matchers"`
				Comment   string          `json:"comment,omitempty"`
				CreatedBy string          `json:"createdBy"`
				StartsAt  time.Time       `json:"startsAt"`
				EndsAt    time.Time       `json:"endsAt"`
			}{
				Matchers: labels.Matchers{
					{Name: "instance", Value: "prometheus-one"},
				},
				Comment:   "Created for a test case.",
				CreatedBy: "test",
				StartsAt:  time.Now(),
				EndsAt:    time.Now().Add(time.Hour),
			}
			data, err := json.Marshal(silence)
			require.NoError(t, err)

			// 4. Create the silence in one of the alertmanagers
			req := httptest.NewRequest(http.MethodPost, externalURL.String()+"/api/v2/silences", bytes.NewReader(data))
			req.Header.Set("content-type", "application/json")
			reqCtx := user.InjectOrgID(req.Context(), userID)
			{
				w := httptest.NewRecorder()
				multitenantAM.serveRequest(w, req.WithContext(reqCtx))

				resp := w.Result()
				body, _ := io.ReadAll(resp.Body)
				assert.Equal(t, http.StatusOK, w.Code)
				require.Regexp(t, regexp.MustCompile(`{"silenceID":".+"}`), string(body))
			}

			var metrics dskit_metrics.MetricFamiliesPerTenant

			// 5. Then, make sure it is propagated successfully.
			//    Replication is asynchronous, so we may have to wait a short period of time.
			if tt.replicationFactor > 1 {
				assert.Eventually(t, func() bool {
					metrics = registries.BuildMetricFamiliesPerTenant()
					return (float64(tt.replicationFactor) == metrics.GetSumOfGauges("cortex_alertmanager_silences") &&
						float64(tt.replicationFactor) == metrics.GetSumOfCounters("cortex_alertmanager_state_replication_total"))
				}, 5*time.Second, 100*time.Millisecond)
				assert.Equal(t, float64(tt.replicationFactor), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_total"))
			} else {
				assert.Equal(t, float64(0), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_total"))
			}

			assert.Equal(t, float64(0), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_failed_total"))

			// 5b. Check the number of partial states merged are as we expect.
			// Partial states are currently replicated twice:
			//   For RF=1 1 -> 0      = Total 0 merges
			//   For RF=2 1 -> 1 -> 1 = Total 2 merges
			//   For RF=3 1 -> 2 -> 4 = Total 6 merges
			nFanOut := tt.replicationFactor - 1
			nMerges := nFanOut + (nFanOut * nFanOut)

			assert.Eventually(t, func() bool {
				metrics = registries.BuildMetricFamiliesPerTenant()
				return float64(nMerges) == metrics.GetSumOfCounters("cortex_alertmanager_partial_state_merges_total")
			}, 5*time.Second, 100*time.Millisecond)

			assert.Equal(t, float64(0), metrics.GetSumOfCounters("cortex_alertmanager_partial_state_merges_failed_total"))
		})
	}
}

func TestAlertmanager_StateReplication_InitialSyncFromPeers(t *testing.T) {
	tc := []struct {
		name              string
		replicationFactor int
	}{
		{
			name:              "RF = 2",
			replicationFactor: 2,
		},
		{
			name:              "RF = 3",
			replicationFactor: 3,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
			t.Cleanup(func() { assert.NoError(t, closer.Close()) })

			mockStore := prepareInMemoryAlertStore()
			clientPool := newPassthroughAlertmanagerClientPool()
			externalURL := flagext.URLValue{}
			err := externalURL.Set("http://localhost:8080/alertmanager")
			require.NoError(t, err)

			var instances []*MultitenantAlertmanager
			var instanceIDs []string
			registries := dskit_metrics.NewTenantRegistries(log.NewNopLogger())

			// Create only two users - no need for more for these test cases.
			for i := 1; i <= 2; i++ {
				u := fmt.Sprintf("u-%d", i)
				require.NoError(t, mockStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
					User:      u,
					RawConfig: simpleConfigOne,
					Templates: []*alertspb.TemplateDesc{},
				}))
			}

			createInstance := func(i int) *MultitenantAlertmanager {
				instanceIDs = append(instanceIDs, fmt.Sprintf("alertmanager-%d", i))
				instanceID := fmt.Sprintf("alertmanager-%d", i)

				amConfig := mockAlertmanagerConfig(t)
				amConfig.ExternalURL = externalURL
				amConfig.ShardingRing.ReplicationFactor = tt.replicationFactor
				amConfig.ShardingRing.Common.InstanceID = instanceID
				amConfig.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)

				// Do not check the ring topology changes or poll in an interval in this test (we explicitly sync alertmanagers).
				amConfig.PollInterval = time.Hour
				amConfig.ShardingRing.RingCheckPeriod = time.Hour

				reg := prometheus.NewPedanticRegistry()
				am, err := createMultitenantAlertmanager(amConfig, nil, mockStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)
				require.NoError(t, err)

				clientPool.setServer(amConfig.ShardingRing.Common.InstanceAddr+":0", am)
				am.alertmanagerClientsPool = clientPool

				require.NoError(t, services.StartAndAwaitRunning(ctx, am))
				t.Cleanup(func() {
					require.NoError(t, services.StopAndAwaitTerminated(ctx, am))
				})

				instances = append(instances, am)
				instanceIDs = append(instanceIDs, instanceID)
				registries.AddTenantRegistry(instanceID, reg)

				// Make sure the ring is settled.
				{
					ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
					defer cancel()

					// The alertmanager is ready to be tested once all instances are ACTIVE and the ring settles.
					for _, am := range instances {
						for _, id := range instanceIDs {
							require.NoError(t, ring.WaitInstanceState(ctx, am.ring, id, ring.ACTIVE))
						}
					}
				}

				// Now that the ring has settled, sync configs with the instances.
				require.NoError(t, am.loadAndSyncConfigs(ctx, reasonRingChange))

				return am
			}

			writeSilence := func(i *MultitenantAlertmanager, userID string) {
				silence := struct {
					Matchers  labels.Matchers `json:"matchers"`
					Comment   string          `json:"comment,omitempty"`
					CreatedBy string          `json:"createdBy"`
					StartsAt  time.Time       `json:"startsAt"`
					EndsAt    time.Time       `json:"endsAt"`
				}{
					Matchers: labels.Matchers{
						{Name: "instance", Value: "prometheus-one"},
					},
					Comment:   "Created for a test case.",
					CreatedBy: "test",
					StartsAt:  time.Now(),
					EndsAt:    time.Now().Add(time.Hour),
				}
				data, err := json.Marshal(silence)
				require.NoError(t, err)

				req := httptest.NewRequest(http.MethodPost, externalURL.String()+"/api/v2/silences", bytes.NewReader(data))
				req.Header.Set("content-type", "application/json")
				reqCtx := user.InjectOrgID(req.Context(), userID)
				{
					w := httptest.NewRecorder()
					i.serveRequest(w, req.WithContext(reqCtx))

					resp := w.Result()
					body, _ := io.ReadAll(resp.Body)
					assert.Equal(t, http.StatusOK, w.Code)
					require.Regexp(t, regexp.MustCompile(`{"silenceID":".+"}`), string(body))
				}
			}

			checkSilence := func(i *MultitenantAlertmanager, userID string) {
				req := httptest.NewRequest(http.MethodGet, externalURL.String()+"/api/v2/silences", nil)
				req.Header.Set("content-type", "application/json")
				reqCtx := user.InjectOrgID(req.Context(), userID)
				{
					w := httptest.NewRecorder()
					i.serveRequest(w, req.WithContext(reqCtx))

					resp := w.Result()
					body, _ := io.ReadAll(resp.Body)
					assert.Equal(t, http.StatusOK, w.Code)
					require.Regexp(t, regexp.MustCompile(`"comment":"Created for a test case."`), string(body))
				}
			}

			// 1. Create the first instance and load the user configurations.
			i1 := createInstance(1)

			// 2. Create a silence in the first alertmanager instance and check we can read it.
			writeSilence(i1, "u-1")
			// 2.a. Check the silence was created (paranoia).
			checkSilence(i1, "u-1")
			// 2.b. Check the relevant metrics were updated.
			{
				metrics := registries.BuildMetricFamiliesPerTenant()
				assert.Equal(t, float64(1), metrics.GetSumOfGauges("cortex_alertmanager_silences"))
			}
			// 2.c. Wait for the silence replication to be attempted; note this is asynchronous.
			{
				test.Poll(t, 5*time.Second, float64(1), func() interface{} {
					metrics := registries.BuildMetricFamiliesPerTenant()
					return metrics.GetSumOfCounters("cortex_alertmanager_state_replication_total")
				})
				metrics := registries.BuildMetricFamiliesPerTenant()
				assert.Equal(t, float64(0), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_failed_total"))
			}

			// 3. Create a second instance. This should attempt to fetch the silence from the first.
			i2 := createInstance(2)

			// 3.a. Check the silence was fetched from the first instance successfully.
			checkSilence(i2, "u-1")

			// 3.b. Check the metrics: We should see the additional silences without any replication activity.
			{
				metrics := registries.BuildMetricFamiliesPerTenant()
				assert.Equal(t, float64(2), metrics.GetSumOfGauges("cortex_alertmanager_silences"))
				assert.Equal(t, float64(1), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_total"))
				assert.Equal(t, float64(0), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_failed_total"))
			}

			if tt.replicationFactor >= 3 {
				// 4. When testing RF = 3, create a third instance, to test obtaining state from multiple places.
				i3 := createInstance(3)

				// 4.a. Check the silence was fetched one or both of the instances successfully.
				checkSilence(i3, "u-1")

				// 4.b. Check the metrics one more time. We should have three replicas of the silence.
				{
					metrics := registries.BuildMetricFamiliesPerTenant()
					assert.Equal(t, float64(3), metrics.GetSumOfGauges("cortex_alertmanager_silences"))
					assert.Equal(t, float64(1), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_total"))
					assert.Equal(t, float64(0), metrics.GetSumOfCounters("cortex_alertmanager_state_replication_failed_total"))
				}
			}
		})
	}
}

// prepareInMemoryAlertStore builds and returns an in-memory alert store.
func prepareInMemoryAlertStore() alertstore.AlertStore {
	return bucketclient.NewBucketAlertStore(objstore.NewInMemBucket(), nil, log.NewNopLogger())
}

func TestSafeTemplateFilepath(t *testing.T) {
	tests := map[string]struct {
		dir          string
		template     string
		expectedPath string
		expectedErr  error
	}{
		"should succeed if the provided template is a filename": {
			dir:          "/data/tenant",
			template:     "test.tmpl",
			expectedPath: "/data/tenant/test.tmpl",
		},
		"should fail if the provided template is escaping the dir": {
			dir:         "/data/tenant",
			template:    "../test.tmpl",
			expectedErr: errors.New(`invalid template name "../test.tmpl": the template filepath is escaping the per-tenant local directory`),
		},
		"template name starting with /": {
			dir:          "/tmp",
			template:     "/file",
			expectedErr:  nil,
			expectedPath: "/tmp/file",
		},
		"escaping template name that has prefix of dir (tmp is prefix of tmpfile)": {
			dir:         "/sub/tmp",
			template:    "../tmpfile",
			expectedErr: errors.New(`invalid template name "../tmpfile": the template filepath is escaping the per-tenant local directory`),
		},
		"empty template name": {
			dir:         "/tmp",
			template:    "",
			expectedErr: errors.New(`invalid template name ""`),
		},
		"dot template name": {
			dir:         "/tmp",
			template:    ".",
			expectedErr: errors.New(`invalid template name "."`),
		},
		"root dir": {
			dir:          "/",
			template:     "file",
			expectedPath: "/file",
		},
		"root dir 2": {
			dir:          "/",
			template:     "/subdir/file",
			expectedPath: "/subdir/file",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualPath, actualErr := safeTemplateFilepath(testData.dir, testData.template)
			assert.Equal(t, testData.expectedErr, actualErr)
			assert.Equal(t, testData.expectedPath, actualPath)
		})
	}
}

func TestStoreTemplateFile(t *testing.T) {
	tempDir := t.TempDir()
	testTemplateDir := filepath.Join(tempDir, templatesDir)

	changed, err := storeTemplateFile(filepath.Join(testTemplateDir, "some-template"), "content")
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = storeTemplateFile(filepath.Join(testTemplateDir, "some-template"), "new content")
	require.NoError(t, err)
	require.True(t, changed)

	changed, err = storeTemplateFile(filepath.Join(testTemplateDir, "some-template"), "new content") // reusing previous content
	require.NoError(t, err)
	require.False(t, changed)
}

func TestMultitenantAlertmanager_verifyRateLimitedEmailConfig(t *testing.T) {
	ctx := context.Background()

	config := `global:
  resolve_timeout: 1m
  smtp_require_tls: false

route:
  receiver: 'email'

receivers:
- name: 'email'
  email_configs:
  - to: test@example.com
    from: test@example.com
    smarthost: smtp:2525
`

	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()
	require.NoError(t, store.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User:      "user",
		RawConfig: config,
		Templates: []*alertspb.TemplateDesc{},
	}))

	limits := mockAlertManagerLimits{
		emailNotificationRateLimit: 0,
		emailNotificationBurst:     0,
	}
	features := featurecontrol.NoopFlags{}

	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)

	am := setupSingleMultitenantAlertmanager(t, cfg, store, &limits, features, log.NewNopLogger(), reg)

	err := am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 1)

	am.alertmanagersMtx.Lock()
	uam := am.alertmanagers["user"]
	am.alertmanagersMtx.Unlock()

	require.NotNil(t, uam)

	ctx = notify.WithReceiverName(ctx, "email")
	ctx = notify.WithRouteID(ctx, "default-route")
	ctx = notify.WithGroupKey(ctx, "key")
	ctx = notify.WithRepeatInterval(ctx, time.Minute)
	ctx = notify.WithNow(ctx, time.Now())

	// Verify that rate-limiter is in place for email notifier.
	_, _, err = uam.lastPipeline.Exec(ctx, utillog.SlogFromGoKit(log.NewNopLogger()), &alert.Alert{})
	require.NotNil(t, err)
	require.Contains(t, err.Error(), errRateLimited.Error())
}

func TestMultitenantAlertmanager_computeFallbackConfig(t *testing.T) {
	// If no fallback configuration is set, it returns a valid empty configuration.
	fallbackConfig, err := ComputeFallbackConfig("")
	require.NoError(t, err)

	_, err = amconfig.Load(string(fallbackConfig))
	require.NoError(t, err)

	// If a fallback configuration file is set, it returns its content.
	configDir := t.TempDir()
	configFile := filepath.Join(configDir, "test.yaml")
	err = os.WriteFile(configFile, []byte(simpleConfigOne), 0664)
	assert.NoError(t, err)

	fallbackConfig, err = ComputeFallbackConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, simpleConfigOne, string(fallbackConfig))
}

func TestShouldStartAM(t *testing.T) {
	store := prepareInMemoryAlertStore()
	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am := setupSingleMultitenantAlertmanager(t, cfg, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	reg2 := prometheus.NewPedanticRegistry()
	cfg2 := mockAlertmanagerConfig(t)
	cfg2.StrictInitializationEnabled = true
	amWithStrictInit := setupSingleMultitenantAlertmanager(t, cfg2, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg2)

	testTenant := "test-tenant"
	tenantReceivingRequests := "test-tenant-receiving"
	tenantReceivingRequestsExpired := "test-tenant-idle"

	amWithStrictInit.lastRequestTime.Store(tenantReceivingRequests, time.Now().Unix())
	amWithStrictInit.lastRequestTime.Store(tenantReceivingRequestsExpired, time.Now().Add(-time.Hour).Unix())

	tests := []struct {
		name       string
		cfg        *alertspb.AlertConfigDesc
		expStartAM bool
	}{
		{
			name: "custom config",
			cfg: &alertspb.AlertConfigDesc{
				User:      testTenant,
				RawConfig: simpleConfigOne,
			},
			expStartAM: true,
		},
		{
			name: "custom config, receiving requests",
			cfg: &alertspb.AlertConfigDesc{
				User:      tenantReceivingRequests,
				RawConfig: simpleConfigOne,
			},
			expStartAM: true,
		},
		{
			name: "custom config, idle Alertmanager",
			cfg: &alertspb.AlertConfigDesc{
				User:      tenantReceivingRequestsExpired,
				RawConfig: simpleConfigOne,
			},
			expStartAM: true,
		},
		{
			name: "default config",
			cfg: &alertspb.AlertConfigDesc{
				User:      testTenant,
				RawConfig: am.fallbackConfig,
			},
		},
		{
			name: "default config, receiving requests",
			cfg: &alertspb.AlertConfigDesc{
				User:      tenantReceivingRequests,
				RawConfig: am.fallbackConfig,
			},
			expStartAM: true,
		},
		{
			name: "default config, idle Alertmanager",
			cfg: &alertspb.AlertConfigDesc{
				User:      tenantReceivingRequestsExpired,
				RawConfig: am.fallbackConfig,
			},
			expStartAM: false,
		},
		{
			name: "empty config",
			cfg: &alertspb.AlertConfigDesc{
				User: testTenant,
			},
		},
		{
			name: "empty config, receiving requests",
			cfg: &alertspb.AlertConfigDesc{
				User: tenantReceivingRequests,
			},
			expStartAM: true,
		},
		{
			name: "empty config, idle Alertmanager",
			cfg: &alertspb.AlertConfigDesc{
				User: tenantReceivingRequestsExpired,
			},
			expStartAM: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.True(t, am.shouldStartAM(test.cfg))
		})

		t.Run(fmt.Sprintf("%s with strict initialization", test.name), func(t *testing.T) {
			// Set a recent last request time for the tenant receiving requests.
			amWithStrictInit.lastRequestTime.Store(tenantReceivingRequests, time.Now().Unix())
			require.Equal(t, test.expStartAM, amWithStrictInit.shouldStartAM(test.cfg))
		})
	}
}

func Test_fingerprint(t *testing.T) {
	// Total exported fields across the protobuf-generated structs that the fingerprint
	// has to cover: 2 in TemplateDesc + 3 in AlertConfigDesc. Internal protoimpl fields
	// (state, unknownFields, sizeCache) are unexported and excluded.
	const expectedTotalFields = 5
	t.Run("ensure all fields in the fingerprint", func(t *testing.T) {
		// Helper function to count the exported fields of a struct.
		getExportedFieldCount := func(v interface{}) int {
			t := reflect.TypeOf(v)
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			n := 0
			for i := 0; i < t.NumField(); i++ {
				if t.Field(i).IsExported() {
					n++
				}
			}
			return n
		}

		// Calculate total exported fields across all structs.
		totalFields := 0
		totalFields += getExportedFieldCount(alertspb.TemplateDesc{})
		totalFields += getExportedFieldCount(alertspb.AlertConfigDesc{})

		require.Equalf(t, expectedTotalFields, totalFields, "Total fields across structs is %d, expected %d; new fields may require updating fingerprint method", totalFields, expectedTotalFields)
	})

	fullConfig := &alertspb.AlertConfigDesc{
		User:      "user",
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{
			{
				Filename: "test",
				Body:     "test",
			},
			{
				Filename: "test2",
				Body:     "test2",
			},
			{
				Filename: "test3",
				Body:     "test3",
			},
		},
	}

	jsonCfg, err := json.Marshal(fullConfig)
	require.NoError(t, err)

	t.Run("fingerprint should be stable", func(t *testing.T) {
		expected := fingerprint(fullConfig)

		// Do it many times to make sure order of elements in the map does not affect fingerprint
		for i := 0; i < 100; i++ {
			cfg2 := &alertspb.AlertConfigDesc{}
			require.NoError(t, json.Unmarshal(jsonCfg, cfg2)) // copy structure
			assert.Empty(t, cmp.Diff(fullConfig, cfg2, protocmp.Transform()))
			rand.Shuffle(len(cfg2.Templates), func(i, j int) {
				cfg2.Templates[i], cfg2.Templates[j] = cfg2.Templates[j], cfg2.Templates[i]
			})
			require.Equal(t, expected, fingerprint(cfg2))
		}
	})

	t.Run("fingerprint should change", func(t *testing.T) {
		cfg := &alertspb.AlertConfigDesc{}
		require.NoError(t, json.Unmarshal(jsonCfg, cfg)) // copy structure
		notChecked := expectedTotalFields
		setStringFieldsWithRandomValue := func(val reflect.Value, callback func(fieldName string)) {
			t := val.Type()
			for i := 0; i < t.NumField(); i++ {
				field := val.Field(i)
				// Skip unexported fields (cannot be set via reflection)
				if !field.CanSet() {
					continue
				}
				switch field.Kind() {
				case reflect.String:
					field.SetString(uuid.NewString())
				case reflect.Bool:
					field.SetBool(!field.Bool())
				default:
					continue
				}
				callback(t.Field(i).Name)
				notChecked--
			}
		}

		lastFingerprint := fingerprint(cfg)
		assertField := func(prefix string) func(fieldName string) {
			return func(fieldName string) {
				newFP := fingerprint(cfg)
				assert.NotEqualf(t, lastFingerprint, newFP, "Changes in fields [%s%s] did not cause fingerprint to change", prefix, fieldName)
				lastFingerprint = newFP
			}
		}

		setStringFieldsWithRandomValue(reflect.ValueOf(cfg).Elem(), assertField(""))
		setStringFieldsWithRandomValue(reflect.ValueOf(cfg.Templates[1]).Elem(), assertField("Templates[1]."))
		cfg.Templates = append(cfg.Templates, &alertspb.TemplateDesc{
			Filename: "test3",
			Body:     "test3",
		})
		assertField("")("Templates")
		notChecked--

		require.Equal(t, 0, notChecked)
	})
}

type passthroughAlertmanagerClient struct {
	server alertmanagerpb.AlertmanagerServer
}

func (am *passthroughAlertmanagerClient) UpdateState(ctx context.Context, in *clusterpb.Part, _ ...grpc.CallOption) (*alertmanagerpb.UpdateStateResponse, error) {
	return am.server.UpdateState(ctx, in)
}

func (am *passthroughAlertmanagerClient) ReadState(ctx context.Context, in *alertmanagerpb.ReadStateRequest, _ ...grpc.CallOption) (*alertmanagerpb.ReadStateResponse, error) {
	return am.server.ReadState(ctx, in)
}

// ReadTenantDigests mirrors the real gRPC client's mandatory ClientUserHeaderInterceptor, which
// errors out before ever sending the RPC if ctx has no org ID. Without this check here, a
// passthrough test could pass while the real client silently failed every call in production.
func (am *passthroughAlertmanagerClient) ReadTenantDigests(ctx context.Context, in *alertmanagerpb.TenantDigestsRequest, _ ...grpc.CallOption) (*alertmanagerpb.TenantDigestsResponse, error) {
	if _, err := user.ExtractOrgID(ctx); err != nil {
		return nil, err
	}
	return am.server.ReadTenantDigests(ctx, in)
}

func (am *passthroughAlertmanagerClient) HandleRequest(ctx context.Context, in *httpgrpc.HTTPRequest, _ ...grpc.CallOption) (*httpgrpc.HTTPResponse, error) {
	return am.server.HandleRequest(ctx, in)
}

func (am *passthroughAlertmanagerClient) RemoteAddress() string {
	return ""
}

// passthroughAlertmanagerClientPool allows testing the logic of gRPC calls between alertmanager instances
// by invoking client calls directly to a peer instance in the unit test, without the server running.
type passthroughAlertmanagerClientPool struct {
	serversMtx sync.Mutex
	servers    map[string]alertmanagerpb.AlertmanagerServer
}

func newPassthroughAlertmanagerClientPool() *passthroughAlertmanagerClientPool {
	return &passthroughAlertmanagerClientPool{
		servers: make(map[string]alertmanagerpb.AlertmanagerServer),
	}
}

func (f *passthroughAlertmanagerClientPool) setServer(addr string, server alertmanagerpb.AlertmanagerServer) {
	f.serversMtx.Lock()
	defer f.serversMtx.Unlock()
	f.servers[addr] = server
}

func (f *passthroughAlertmanagerClientPool) GetClientFor(addr string) (Client, error) {
	f.serversMtx.Lock()
	defer f.serversMtx.Unlock()
	s, ok := f.servers[addr]
	if !ok {
		return nil, fmt.Errorf("client not found for address: %v", addr)
	}
	return Client(&passthroughAlertmanagerClient{s}), nil
}

type mockAlertManagerLimits struct {
	emailNotificationRateLimit     rate.Limit
	emailNotificationBurst         int
	maxConfigSize                  int
	maxSilencesCount               int
	maxSilenceSizeBytes            int
	maxTemplatesCount              int
	maxSizeOfTemplate              int
	maxDispatcherAggregationGroups int
	maxAlertsCount                 int
	maxAlertsSizeBytes             int

	receiversBlockPrivateAddresses bool
	receiversBlockCIDRNetworks     []flagext.CIDR
}

func (m *mockAlertManagerLimits) AlertmanagerMaxConfigSize(string) int {
	return m.maxConfigSize
}

func (m *mockAlertManagerLimits) AlertmanagerMaxSilencesCount(string) int { return m.maxSilencesCount }

func (m *mockAlertManagerLimits) AlertmanagerMaxSilenceSizeBytes(string) int {
	return m.maxSilenceSizeBytes
}

func (m *mockAlertManagerLimits) AlertmanagerMaxTemplatesCount(string) int {
	return m.maxTemplatesCount
}

func (m *mockAlertManagerLimits) AlertmanagerMaxTemplateSize(string) int {
	return m.maxSizeOfTemplate
}

func (m *mockAlertManagerLimits) AlertmanagerReceiversBlockCIDRNetworks(string) []flagext.CIDR {
	return m.receiversBlockCIDRNetworks
}

func (m *mockAlertManagerLimits) AlertmanagerReceiversBlockPrivateAddresses(string) bool {
	return m.receiversBlockPrivateAddresses
}

func (m *mockAlertManagerLimits) NotificationRateLimit(string, string) rate.Limit {
	return m.emailNotificationRateLimit
}

func (m *mockAlertManagerLimits) NotificationBurstSize(string, string) int {
	return m.emailNotificationBurst
}

func (m *mockAlertManagerLimits) AlertmanagerMaxDispatcherAggregationGroups(_ string) int {
	return m.maxDispatcherAggregationGroups
}

func (m *mockAlertManagerLimits) AlertmanagerMaxAlertsCount(_ string) int {
	return m.maxAlertsCount
}

func (m *mockAlertManagerLimits) AlertmanagerMaxAlertsSizeBytes(_ string) int {
	return m.maxAlertsSizeBytes
}

func TestMultitenantAlertmanager_ClusterValidation(t *testing.T) {
	requestDuration := func(reg prometheus.Registerer) *prometheus.HistogramVec {
		return promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_alertmanager_distributor_client_request_duration_seconds",
			Help:    "Time spent executing requests from an alertmanager to another alertmanager.",
			Buckets: prometheus.ExponentialBuckets(0.008, 4, 7),
		}, []string{"operation", "status_code"})
	}
	testCases := map[string]struct {
		serverClusterValidation clusterutil.ServerClusterValidationConfig
		clientClusterValidation clusterutil.ClusterValidationConfig
		expectedError           *status.Status
		expectedMetrics         string
	}{
		"if server and client have equal cluster validation labels and cluster validation is disabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "cluster"},
				GRPC:                    clusterutil.ClusterValidationProtocolConfig{Enabled: false},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "cluster"},
			expectedError:           nil,
		},
		"if server and client have different cluster validation labels and cluster validation is disabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC:                    clusterutil.ClusterValidationProtocolConfig{Enabled: false},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "client-cluster"},
			expectedError:           nil,
		},
		"if server and client have equal cluster validation labels and cluster validation is enabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "cluster"},
				GRPC:                    clusterutil.ClusterValidationProtocolConfig{Enabled: true},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "cluster"},
			expectedError:           nil,
		},
		"if server and client have different cluster validation labels and soft cluster validation is enabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: true,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "client-cluster"},
			expectedError:           nil,
		},
		"if server and client have different cluster validation labels and cluster validation is enabled an error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: false,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{Label: "client-cluster"},
			expectedError:           grpcutil.Status(codes.Internal, `request rejected by the server: rejected request with wrong cluster validation label "client-cluster" - it should be one of [server-cluster]`),
			expectedMetrics: `
				# HELP cortex_client_invalid_cluster_validation_label_requests_total Number of requests with invalid cluster validation label.
        	    # TYPE cortex_client_invalid_cluster_validation_label_requests_total counter
        	    cortex_client_invalid_cluster_validation_label_requests_total{client="alertmanager",method="/alertmanagerpb.Alertmanager/HandleRequest",protocol="grpc"} 1
			`,
		},
		"if client has no cluster validation label and soft cluster validation is enabled no error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: true,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{},
			expectedError:           nil,
		},
		"if client has no cluster validation label and cluster validation is enabled an error is returned": {
			serverClusterValidation: clusterutil.ServerClusterValidationConfig{
				ClusterValidationConfig: clusterutil.ClusterValidationConfig{Label: "server-cluster"},
				GRPC: clusterutil.ClusterValidationProtocolConfig{
					Enabled:        true,
					SoftValidation: false,
				},
			},
			clientClusterValidation: clusterutil.ClusterValidationConfig{},
			expectedError:           grpcutil.Status(codes.FailedPrecondition, `rejected request with empty cluster validation label - it should be one of [server-cluster]`),
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			var grpcOptions []grpc.ServerOption
			if testCase.serverClusterValidation.GRPC.Enabled {
				reg := prometheus.NewPedanticRegistry()
				grpcOptions = []grpc.ServerOption{
					grpc.ChainUnaryInterceptor(middleware.ClusterUnaryServerInterceptor(
						[]string{testCase.serverClusterValidation.Label}, testCase.serverClusterValidation.GRPC.SoftValidation,
						middleware.NewInvalidClusterRequests(reg, "cortex"), log.NewNopLogger(),
					)),
				}
			}

			grpcServer := grpc.NewServer(grpcOptions...)
			defer grpcServer.GracefulStop()

			srv := &mockAlertmanagerServer{}
			alertmanagerpb.RegisterAlertmanagerServer(grpcServer, srv)

			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			go func() {
				require.NoError(t, grpcServer.Serve(listener))
			}()

			cfg := grpcclient.Config{}
			flagext.DefaultValues(&cfg)
			cfg.ClusterValidation = testCase.clientClusterValidation

			reg := prometheus.NewPedanticRegistry()
			inst := ring.InstanceDesc{Addr: listener.Addr().String()}
			client, err := dialAlertmanagerClient(cfg, inst, requestDuration(reg), util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "alertmanager", util.GRPCProtocol), log.NewNopLogger())
			require.NoError(t, err)
			defer client.Close() //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), "test")
			_, err = client.HandleRequest(ctx, &httpgrpc.HTTPRequest{})
			if testCase.expectedError == nil {
				require.NoError(t, err)
			} else {
				stat, ok := grpcutil.ErrorToStatus(err)
				require.True(t, ok)
				require.Equal(t, testCase.expectedError.Code(), stat.Code())
				require.Equal(t, testCase.expectedError.Message(), stat.Message())
			}
			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), "cortex_client_invalid_cluster_validation_label_requests_total")
			assert.NoError(t, err)
		})
	}
}

type mockAlertmanagerServer struct {
	alertmanagerpb.UnimplementedAlertmanagerServer
}

func (*mockAlertmanagerServer) HandleRequest(context.Context, *httpgrpc.HTTPRequest) (*httpgrpc.HTTPResponse, error) {
	return &httpgrpc.HTTPResponse{}, nil
}

func TestMultitenantAlertmanager_ReadTenantDigests(t *testing.T) {
	const tenantID = "user-1"
	const otherTenantID = "user-2"

	ctx := t.Context()

	am := setupSingleMultitenantAlertmanager(t,
		mockAlertmanagerConfig(t),
		prepareInMemoryAlertStore(),
		nil,
		featurecontrol.NoopFlags{},
		log.NewNopLogger(),
		prometheus.NewPedanticRegistry(),
	)
	for _, userID := range []string{tenantID, otherTenantID} {
		_, err := am.setConfig(&alertspb.AlertConfigDesc{User: userID, RawConfig: simpleConfigOne})
		require.NoError(t, err)
		require.NoError(t, am.alertmanagers[userID].WaitInitialStateSync(ctx))
	}

	t.Run("reports found=false for a tenant this instance doesn't own", func(t *testing.T) {
		resp, err := am.ReadTenantDigests(ctx, &alertmanagerpb.TenantDigestsRequest{UserIds: []string{"no-such-user"}})
		require.NoError(t, err)
		require.Len(t, resp.Digests, 1)
		assert.False(t, resp.Digests[0].Found)
		assert.Empty(t, resp.Digests[0].Digests, "a tenant we don't have can't have any authoritative part")
	})

	t.Run("reports a digest matching this instance's own for an owned tenant", func(t *testing.T) {
		wantDigest, err := am.alertmanagers[tenantID].silencesDigest(ctx)
		require.NoError(t, err)

		resp, err := am.ReadTenantDigests(ctx, &alertmanagerpb.TenantDigestsRequest{UserIds: []string{tenantID}})
		require.NoError(t, err)
		require.Len(t, resp.Digests, 1)

		d := resp.Digests[0]
		assert.True(t, d.Found)
		assert.Equal(t, tenantID, d.UserId)

		got, sent := silencesDigestFrom(d)
		require.True(t, sent, "a synced tenant must carry a silences digest")
		assert.Equal(t, wantDigest, got)
	})

	t.Run("handles a batch mixing owned and unknown tenants", func(t *testing.T) {
		resp, err := am.ReadTenantDigests(ctx, &alertmanagerpb.TenantDigestsRequest{UserIds: []string{tenantID, "no-such-user"}})
		require.NoError(t, err)
		require.Len(t, resp.Digests, 2)

		byUser := map[string]*alertmanagerpb.TenantDigest{}
		for _, d := range resp.Digests {
			byUser[d.UserId] = d
		}
		assert.True(t, byUser[tenantID].Found)
		assert.False(t, byUser["no-such-user"].Found)
	})

	t.Run("stops early instead of working through the batch for a caller that gave up", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		_, err := am.ReadTenantDigests(cancelled, &alertmanagerpb.TenantDigestsRequest{UserIds: []string{tenantID}})
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("omits the silences digest while the tenant's own initial sync hasn't finished", func(t *testing.T) {
		am.alertmanagers[tenantID].state.initialSyncDone.Store(false)
		t.Cleanup(func() { am.alertmanagers[tenantID].state.initialSyncDone.Store(true) })

		resp, err := am.ReadTenantDigests(ctx, &alertmanagerpb.TenantDigestsRequest{UserIds: []string{tenantID}})
		require.NoError(t, err)
		require.Len(t, resp.Digests, 1)

		d := resp.Digests[0]
		assert.True(t, d.Found, "the tenant is on this instance, which is all found means")
		_, sent := silencesDigestFrom(d)
		assert.False(t, sent, "an unsynced replica's silences aren't authoritative, so it must send no digest for them rather than a hash of what it happens to hold")
	})

	t.Run("returns the other tenants before the deadline while one is blocked", func(t *testing.T) {
		blockSilencesDigest(t, am.alertmanagers[tenantID])

		const timeout = 500 * time.Millisecond
		deadlineCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		start := time.Now()
		resp, err := am.ReadTenantDigests(deadlineCtx, &alertmanagerpb.TenantDigestsRequest{UserIds: []string{tenantID, otherTenantID}})
		require.NoError(t, err)
		assert.Less(t, time.Since(start), timeout)
		require.Len(t, resp.Digests, 1)
		assert.Equal(t, otherTenantID, resp.Digests[0].UserId)
	})

	t.Run("waits for every tenant when the request has no deadline", func(t *testing.T) {
		release := blockSilencesDigest(t, am.alertmanagers[tenantID])

		type result struct {
			resp *alertmanagerpb.TenantDigestsResponse
			err  error
		}
		done := make(chan result, 1)
		go func() {
			resp, err := am.ReadTenantDigests(ctx, &alertmanagerpb.TenantDigestsRequest{UserIds: []string{tenantID, otherTenantID}})
			done <- result{resp, err}
		}()

		select {
		case <-done:
			t.Fatal("returned while a tenant was still blocked")
		case <-time.After(100 * time.Millisecond):
		}

		release()
		r := <-done
		require.NoError(t, r.err)
		assert.Len(t, r.resp.Digests, 2)
	})
}

// dropBroadcasts stops am's silence writes from reaching its peers, as if every delivery was lost.
// It still moves silencesGeneration, as the real broadcast does, so cached digests stay correct.
func dropBroadcasts(am *Alertmanager) {
	am.silences.SetBroadcast(func([]byte) { am.silencesGeneration.Inc() })
}

// blockSilencesDigest holds am's digest cache lock, which every silencesDigest call takes first,
// until the returned release is called or the test ends.
func blockSilencesDigest(t *testing.T, am *Alertmanager) func() {
	am.silencesDigestCache.mtx.Lock()
	release := sync.OnceFunc(am.silencesDigestCache.mtx.Unlock)
	t.Cleanup(release)
	return release
}

func TestMultitenantAlertmanager_ReconcileSilences(t *testing.T) {
	ctx := t.Context()
	tenants := []string{"user-1", "user-2", "user-3"}

	// setup starts three replicas at replication factor 3, so every replica owns every tenant.
	setup := func(t *testing.T) ([]*MultitenantAlertmanager, *passthroughAlertmanagerClientPool) {
		ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
		t.Cleanup(func() { assert.NoError(t, closer.Close()) })

		mockStore := prepareInMemoryAlertStore()
		for _, userID := range tenants {
			require.NoError(t, mockStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
				User:      userID,
				RawConfig: simpleConfigOne,
				Templates: []*alertspb.TemplateDesc{},
			}))
		}

		clientPool := newPassthroughAlertmanagerClientPool()

		var instances []*MultitenantAlertmanager
		var instanceIDs []string
		for i := 1; i <= 3; i++ {
			instanceID := fmt.Sprintf("alertmanager-%d", i)
			instanceIDs = append(instanceIDs, instanceID)

			amConfig := mockAlertmanagerConfig(t)
			amConfig.ShardingRing.ReplicationFactor = 3
			amConfig.ShardingRing.Common.InstanceID = instanceID
			amConfig.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
			// This test drives config sync and reconciliation explicitly.
			amConfig.PollInterval = time.Hour
			amConfig.ShardingRing.RingCheckPeriod = time.Hour
			amConfig.SilenceReconcileInterval = 0

			am, err := createMultitenantAlertmanager(amConfig, nil, mockStore, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), am)) })

			clientPool.setServer(amConfig.ShardingRing.Common.InstanceAddr+":0", am)
			am.alertmanagerClientsPool = clientPool

			require.NoError(t, services.StartAndAwaitRunning(ctx, am))
			instances = append(instances, am)
		}

		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for _, am := range instances {
			for _, id := range instanceIDs {
				require.NoError(t, ring.WaitInstanceState(waitCtx, am.ring, id, ring.ACTIVE))
			}
		}

		for _, am := range instances {
			require.NoError(t, am.loadAndSyncConfigs(ctx, reasonRingChange))
			for _, userID := range tenants {
				require.NoError(t, am.alertmanagers[userID].WaitInitialStateSync(ctx))
			}
		}
		return instances, clientPool
	}

	addrOf := func(am *MultitenantAlertmanager) string { return am.cfg.ShardingRing.Common.InstanceAddr + ":0" }
	digestOf := func(t *testing.T, am *MultitenantAlertmanager, userID string) uint64 {
		d, err := am.alertmanagers[userID].silencesDigest(ctx)
		require.NoError(t, err)
		return d
	}
	checks := func(am *MultitenantAlertmanager, outcome string) float64 {
		return testutil.ToFloat64(am.silenceReconcileChecksTotal.WithLabelValues(outcome))
	}
	// addMissedSilence adds a silence to one replica without broadcasting it, as if every delivery was lost.
	addMissedSilence := func(t *testing.T, am *MultitenantAlertmanager, userID string) {
		dropBroadcasts(am.alertmanagers[userID])
		addTestSilence(t, am.alertmanagers[userID], "missed by the other replicas")
	}

	// startReconcile runs am.reconcileSilences in the background, canceled at cleanup, and returns a channel closed when it returns.
	startReconcile := func(t *testing.T, am *MultitenantAlertmanager) <-chan struct{} {
		reconcileCtx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		done := make(chan struct{})
		go func() {
			defer close(done)
			am.reconcileSilences(reconcileCtx)
		}()
		return done
	}

	t.Run("repairs a replica missing a silence from the diverging peer only", func(t *testing.T) {
		instances, clientPool := setup(t)
		source, other, stale := instances[0], instances[1], instances[2]

		addMissedSilence(t, source, "user-1")
		require.NotEqual(t, digestOf(t, source, "user-1"), digestOf(t, stale, "user-1"))

		// other sends no digests, so it never diverges.
		otherReads := &countingReadStateServer{AlertmanagerServer: &digestlessServer{AlertmanagerServer: other}}
		clientPool.setServer(addrOf(other), otherReads)

		stale.reconcileSilences(ctx)

		assert.Equal(t, digestOf(t, source, "user-1"), digestOf(t, stale, "user-1"), "the missed silence should have been pulled from the diverging peer")
		assert.Equal(t, float64(1), checks(stale, checkRepaired))
		assert.Equal(t, float64(2), checks(stale, checkInSync))
		assert.Equal(t, float64(len(tenants)), checks(stale, checkSkipped))
		assert.Zero(t, otherReads.calls.Load(), "a peer that didn't diverge must not be read")
	})

	t.Run("counts a stale peer without changing local silences", func(t *testing.T) {
		instances, _ := setup(t)
		source := instances[0]

		addMissedSilence(t, source, "user-1")
		before := digestOf(t, source, "user-1")

		source.reconcileSilences(ctx)

		assert.Equal(t, before, digestOf(t, source, "user-1"))
		assert.Equal(t, float64(2), checks(source, checkNoChange), "both peers are the stale ones")
	})

	t.Run("repairs a tenant whose own initial sync failed and marks it synced", func(t *testing.T) {
		instances, _ := setup(t)
		source, stale := instances[0], instances[2]

		addMissedSilence(t, source, "user-1")
		stale.alertmanagers["user-1"].state.initialSyncDone.Store(false)

		stale.reconcileSilences(ctx)

		assert.Equal(t, digestOf(t, source, "user-1"), digestOf(t, stale, "user-1"), "a replica whose initial sync failed should still pull from a synced peer")
		assert.Equal(t, float64(1), checks(stale, checkRepaired))
		assert.True(t, stale.alertmanagers["user-1"].initialStateSynced(), "merging a synced peer's silences makes this replica's authoritative")
	})

	t.Run("asks peers for digests concurrently", func(t *testing.T) {
		instances, clientPool := setup(t)
		source, other, stale := instances[0], instances[1], instances[2]

		addMissedSilence(t, source, "user-1")
		// Each peer only answers once the other has been asked, so asking one at a time never finishes.
		sourceAsked, otherAsked := make(chan struct{}), make(chan struct{})
		clientPool.setServer(addrOf(source), &gatedDigestsServer{AlertmanagerServer: source, entered: sourceAsked, wait: otherAsked})
		clientPool.setServer(addrOf(other), &gatedDigestsServer{AlertmanagerServer: other, entered: otherAsked, wait: sourceAsked})
		// Longer than the wait below, so a timed out call can't let the other through.
		stale.cfg.AlertmanagerClient.RemoteTimeout = time.Minute

		done := startReconcile(t, stale)

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "reconciliation should finish once both peers have been asked")
		}
		assert.Equal(t, float64(1), checks(stale, checkRepaired))
	})

	t.Run("resyncs different tenants concurrently", func(t *testing.T) {
		instances, clientPool := setup(t)
		source, stale := instances[0], instances[2]

		addMissedSilence(t, source, "user-1")
		addMissedSilence(t, source, "user-2")
		// Each tenant's read only returns once the other's has started, so resyncing one at a time never finishes.
		reading := map[string]chan struct{}{"user-1": make(chan struct{}), "user-2": make(chan struct{})}
		clientPool.setServer(addrOf(source), &hookedReadStateServer{AlertmanagerServer: source, before: func(ctx context.Context, userID string) error {
			other := map[string]string{"user-1": "user-2", "user-2": "user-1"}[userID]
			close(reading[userID])
			select {
			case <-reading[other]:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}})

		done := startReconcile(t, stale)

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "reconciliation should finish once both tenants are being read")
		}
		assert.Equal(t, float64(2), checks(stale, checkRepaired))
	})

	t.Run("resyncs a tenant from one peer at a time and skips a resync the first one made unneeded", func(t *testing.T) {
		instances, clientPool := setup(t)
		source, other, stale := instances[0], instances[1], instances[2]

		// Both peers hold a silence stale missed.
		addMissedSilence(t, source, "user-1")
		dropBroadcasts(other.alertmanagers["user-1"])
		other.reconcileSilences(ctx)
		require.Equal(t, digestOf(t, source, "user-1"), digestOf(t, other, "user-1"))

		// The first read blocks until released, so the resync from the other peer is pending meanwhile.
		var reads atomic.Int32
		firstReading, release := make(chan struct{}), make(chan struct{})
		releaseFirst := sync.OnceFunc(func() { close(release) })
		t.Cleanup(releaseFirst)
		blockFirstRead := func(ctx context.Context, _ string) error {
			if reads.Inc() > 1 {
				return nil
			}
			close(firstReading)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		clientPool.setServer(addrOf(source), &hookedReadStateServer{AlertmanagerServer: source, before: blockFirstRead})
		clientPool.setServer(addrOf(other), &hookedReadStateServer{AlertmanagerServer: other, before: blockFirstRead})

		done := startReconcile(t, stale)

		<-firstReading
		assert.Never(t, func() bool { return reads.Load() > 1 }, 200*time.Millisecond, 10*time.Millisecond,
			"the tenant must not be read from the other peer while the first resync is in flight")
		releaseFirst()
		<-done

		assert.Equal(t, digestOf(t, source, "user-1"), digestOf(t, stale, "user-1"))
		assert.Equal(t, float64(1), checks(stale, checkRepaired))
		assert.Equal(t, int32(1), reads.Load(), "the tenant was already in sync with the other peer once the first resync finished")
	})

	t.Run("a tenant stuck on a peer doesn't stop the other tenants syncing", func(t *testing.T) {
		instances, _ := setup(t)
		source, stale := instances[0], instances[2]

		addMissedSilence(t, source, "user-2")
		blockSilencesDigest(t, source.alertmanagers["user-1"])

		select {
		case <-startReconcile(t, stale):
		case <-time.After(5 * time.Second):
			t.Fatal("reconcile run didn't finish")
		}

		assert.Equal(t, digestOf(t, source, "user-2"), digestOf(t, stale, "user-2"))
		assert.Equal(t, float64(1), checks(stale, checkRepaired))
		assert.Equal(t, float64(1), checks(stale, checkSkipped))
	})

	t.Run("skips a tick while the previous run is still going", func(t *testing.T) {
		instances, clientPool := setup(t)
		local, peer := instances[0], instances[1]

		release := make(chan struct{})
		clientPool.setServer(addrOf(peer), &gatedDigestsServer{AlertmanagerServer: peer, entered: make(chan struct{}), wait: release})

		require.True(t, local.tryReconcileSilencesAsync(ctx), "the first tick should start a run")
		require.False(t, local.tryReconcileSilencesAsync(ctx), "a tick during a blocked run should be skipped")

		close(release)
		require.Eventually(t, func() bool { return local.tryReconcileSilencesAsync(ctx) }, time.Second, 10*time.Millisecond,
			"a tick after the run finished should start a new one")
	})
}

// countingReadStateServer counts ReadState calls before delegating to the real server.
type countingReadStateServer struct {
	alertmanagerpb.AlertmanagerServer
	calls atomic.Int32
}

func (c *countingReadStateServer) ReadState(ctx context.Context, req *alertmanagerpb.ReadStateRequest) (*alertmanagerpb.ReadStateResponse, error) {
	c.calls.Inc()
	return c.AlertmanagerServer.ReadState(ctx, req)
}

// digestlessServer reports every tenant as found but attaches no digests, the way a replica that
// holds the tenant but can't vouch for its silences yet answers.
type digestlessServer struct {
	alertmanagerpb.AlertmanagerServer
}

func (d *digestlessServer) ReadTenantDigests(_ context.Context, req *alertmanagerpb.TenantDigestsRequest) (*alertmanagerpb.TenantDigestsResponse, error) {
	resp := &alertmanagerpb.TenantDigestsResponse{Digests: make([]*alertmanagerpb.TenantDigest, 0, len(req.UserIds))}
	for _, userID := range req.UserIds {
		resp.Digests = append(resp.Digests, &alertmanagerpb.TenantDigest{UserId: userID, Found: true})
	}
	return resp, nil
}

// gatedDigestsServer closes entered on its first ReadTenantDigests call and blocks every call until
// wait is closed or the call's context ends.
type gatedDigestsServer struct {
	alertmanagerpb.AlertmanagerServer
	entered     chan struct{}
	enteredOnce sync.Once
	wait        <-chan struct{}
}

func (g *gatedDigestsServer) ReadTenantDigests(ctx context.Context, req *alertmanagerpb.TenantDigestsRequest) (*alertmanagerpb.TenantDigestsResponse, error) {
	g.enteredOnce.Do(func() { close(g.entered) })
	select {
	case <-g.wait:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return g.AlertmanagerServer.ReadTenantDigests(ctx, req)
}

// hookedReadStateServer calls before with the tenant of every ReadState call, failing the call if it errors.
type hookedReadStateServer struct {
	alertmanagerpb.AlertmanagerServer
	before func(ctx context.Context, userID string) error
}

func (h *hookedReadStateServer) ReadState(ctx context.Context, req *alertmanagerpb.ReadStateRequest) (*alertmanagerpb.ReadStateResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}
	if err := h.before(ctx, userID); err != nil {
		return nil, err
	}
	return h.AlertmanagerServer.ReadState(ctx, req)
}

// unreachableForBroadcastServer drops every broadcast but serves every other RPC.
type unreachableForBroadcastServer struct {
	alertmanagerpb.AlertmanagerServer
}

func (u *unreachableForBroadcastServer) UpdateState(context.Context, *clusterpb.Part) (*alertmanagerpb.UpdateStateResponse, error) {
	return nil, errors.New("simulated: peer unreachable for broadcast")
}

// TestMultitenantAlertmanager_SilenceReconcileIntervalDrivesReconciliation covers the flag actually
// wired into run()'s own ticker, as opposed to every other reconciliation test, which calls
// reconcileSilences or tryReconcileSilencesAsync directly and never exercises the ticker at all.
func TestMultitenantAlertmanager_SilenceReconcileIntervalDrivesReconciliation(t *testing.T) {
	ctx := t.Context()
	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	mockStore := prepareInMemoryAlertStore()
	require.NoError(t, mockStore.SetAlertConfig(ctx, &alertspb.AlertConfigDesc{
		User:      "user-1",
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{},
	}))

	clientPool := newPassthroughAlertmanagerClientPool()

	var instances []*MultitenantAlertmanager
	var stores []*stallingAlertStore
	var instanceIDs []string
	for i := 1; i <= 2; i++ {
		instanceID := fmt.Sprintf("alertmanager-%d", i)
		instanceIDs = append(instanceIDs, instanceID)

		amConfig := mockAlertmanagerConfig(t)
		amConfig.ShardingRing.ReplicationFactor = 2
		amConfig.ShardingRing.Common.InstanceID = instanceID
		amConfig.ShardingRing.Common.InstanceAddr = fmt.Sprintf("127.0.0.%d", i)
		amConfig.PollInterval = 10 * time.Millisecond
		amConfig.ShardingRing.RingCheckPeriod = time.Hour
		// Unlike every other reconciliation test, this is the one thing under test: a real,
		// running ticker, not a direct call to reconcileSilences/tryReconcileSilencesAsync.
		amConfig.SilenceReconcileInterval = 20 * time.Millisecond

		store := &stallingAlertStore{AlertStore: mockStore, stalled: make(chan struct{})}
		stores = append(stores, store)
		am, err := createMultitenantAlertmanager(amConfig, nil, store, ringStore, &mockAlertManagerLimits{}, featurecontrol.NoopFlags{}, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), am)) })

		clientPool.setServer(amConfig.ShardingRing.Common.InstanceAddr+":0", am)
		am.alertmanagerClientsPool = clientPool

		require.NoError(t, services.StartAndAwaitRunning(ctx, am))
		instances = append(instances, am)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, am := range instances {
		for _, id := range instanceIDs {
			require.NoError(t, ring.WaitInstanceState(waitCtx, am.ring, id, ring.ACTIVE))
		}
	}

	for _, am := range instances {
		require.NoError(t, am.loadAndSyncConfigs(ctx, reasonRingChange))
		require.NoError(t, am.alertmanagers["user-1"].WaitInitialStateSync(ctx))
	}

	// A config sync can block the run loop, so reconciliation must not wait for it.
	for _, store := range stores {
		store.stalling.Store(true)
		select {
		case <-store.stalled:
		case <-time.After(2 * time.Second):
			t.Fatal("the periodic config sync should have started and stalled")
		}
	}

	inSync, divergent := instances[0], instances[1]
	if _, ok := inSync.alertmanagers["user-1"]; !ok {
		inSync, divergent = instances[1], instances[0]
	}

	// Make ordinary broadcast delivery to divergent fail permanently, so only the reconcile
	// ticker itself can converge the two replicas, not the pre-existing incremental replication
	// path racing to deliver the same update on its own.
	clientPool.setServer(divergent.cfg.ShardingRing.Common.InstanceAddr+":0", &unreachableForBroadcastServer{AlertmanagerServer: divergent})

	addTestSilence(t, inSync.alertmanagers["user-1"], "missed by the other replica")

	inSyncDigest, err := inSync.alertmanagers["user-1"].silencesDigest(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		afterDigest, err := divergent.alertmanagers["user-1"].silencesDigest(ctx)
		return err == nil && afterDigest == inSyncDigest
	}, 2*time.Second, 10*time.Millisecond, "the running service's own ticker should have reconciled this without any direct call to reconcileSilences")
}

// stallingAlertStore blocks the config sync in ListAllUsers once stalling is set, until the
// caller's context is canceled.
type stallingAlertStore struct {
	alertstore.AlertStore
	stalling    atomic.Bool
	stalled     chan struct{}
	stalledOnce sync.Once
}

func (s *stallingAlertStore) ListAllUsers(ctx context.Context) ([]string, error) {
	if s.stalling.Load() {
		s.stalledOnce.Do(func() { close(s.stalled) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return s.AlertStore.ListAllUsers(ctx)
}
