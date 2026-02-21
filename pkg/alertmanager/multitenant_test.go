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
	"maps"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/pprof"
	"net/url"
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
	"github.com/grafana/alerting/definition"
	alertingReceivers "github.com/grafana/alerting/receivers"
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
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	amconfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/featurecontrol"
	pb "github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/prometheus/alertmanager/types"
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

	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerpb"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util"
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

	grafanaConfig     = `{"template_files":{},"alertmanager_config":{"route":{"receiver":"grafana-default-email","group_by":["grafana_folder","alertname"]},"templates":null,"receivers":[{"name":"grafana-default-email","grafana_managed_receiver_configs":[{"uid":"dde6ntuob69dtf","name":"WH","type":"webhook","disableResolveMessage":false,"settings":{"url":"http://localhost:8080","username":"test"},"secureSettings":{"password":"test"}}]}]}}`
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
	require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
		// This package's init() function statically starts a singleton goroutine that runs forever.
		goleak.IgnoreTopFunction("github.com/grafana/mimir/pkg/alertmanager.init.0.func1"),
	)

	ctx := context.Background()

	// Run this test using a real storage client.
	store := prepareInMemoryAlertStore()
	user1Cfg := alertspb.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigOne,
		Templates: []*alertspb.TemplateDesc{},
	}
	require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))

	user2Cfg := alertspb.AlertConfigDesc{
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
	require.Equal(t, amConfigFromMimirConfig(user1Cfg, cfg.ExternalURL.URL).fingerprint(), currentConfigFp)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	// Ensure when a 3rd config is added, it is synced correctly
	user3Cfg := alertspb.AlertConfigDesc{
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
	require.Equal(t, amConfigFromMimirConfig(user3Cfg, cfg.ExternalURL.URL).fingerprint(), finalUserCfgFp)
	user3Am, ok := am.alertmanagers["user3"]
	require.True(t, ok)
	require.Len(t, user3Am.templates, 2)
	require.Equal(t, "first.tpl", user3Am.templates[0].Name)
	require.Equal(t, "second.tpl", user3Am.templates[1].Name)

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
		cortex_alertmanager_config_last_reload_successful{user="user3"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	user1Cfg = alertspb.AlertConfigDesc{
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
	expectedFp := amConfigFromMimirConfig(user1Cfg, cfg.ExternalURL.URL).fingerprint()
	require.Equal(t, expectedFp, currentConfigFp)

	// Ensure the config is reloaded if only templates changed
	user1Cfg = alertspb.AlertConfigDesc{
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
	expectedFp = amConfigFromMimirConfig(user1Cfg, cfg.ExternalURL.URL).fingerprint()
	require.Equal(t, expectedFp, currentConfigFp)
	user1Am, ok := am.alertmanagers["user1"]
	require.True(t, ok)
	require.Len(t, user1Am.templates, 1)
	require.Equal(t, "some-template.tmpl", user1Am.templates[0].Name)
	require.Contains(t, user1Am.templates[0].Template, "some.template")

	// Ensure that when a Grafana config is added, it is synced correctly.
	testSmtpFrom := "test@grafana.com"
	smtpConfig := &alertspb.SmtpConfig{
		FromAddress:   testSmtpFrom,
		StaticHeaders: map[string]string{"Header1": "Value1"},
	}
	externalUrl, err := url.Parse("test.grafana.com")
	require.NoError(t, err)
	userGrafanaCfg := alertspb.GrafanaAlertConfigDesc{
		User:               "user4",
		RawConfig:          grafanaConfig,
		Hash:               "test",
		CreatedAtTimestamp: time.Now().Unix(),
		Default:            false,
		Promoted:           true,
		ExternalUrl:        externalUrl.String(),
		SmtpConfig:         smtpConfig,
	}
	emptyMimirConfig := alertspb.AlertConfigDesc{User: "user4"}
	url, err := url.Parse("http://localhost/alertmanager")
	require.NoError(t, err)
	emptyMimirAmConfig := amConfig{
		User:            "user4",
		TmplExternalURL: url,
		Templates:       []definition.PostableApiTemplate{},
	}
	require.NoError(t, store.SetGrafanaAlertConfig(ctx, userGrafanaCfg))
	require.NoError(t, store.SetAlertConfig(ctx, emptyMimirConfig))
	require.NoError(t, store.SetGrafanaAlertConfig(ctx, userGrafanaCfg))
	require.NoError(t, store.SetAlertConfig(ctx, emptyMimirConfig))

	err = am.loadAndSyncConfigs(ctx, reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 4)

	// The Mimir configuration was empty, so the Grafana configuration should be chosen for user 4.
	amCfg, err := am.amConfigFromGrafanaConfig(userGrafanaCfg)
	require.NoError(t, err)
	grafanaAlertConfigDesc := amCfg
	require.Equal(t, grafanaAlertConfigDesc.fingerprint(), am.cfgs["user4"])

	dirs = am.getPerUserDirectories()
	user4Dir := dirs["user4"]
	require.NotZero(t, user4Dir)
	require.True(t, dirExists(t, user4Dir))

	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_alertmanager_config_last_reload_successful Boolean set to 1 whenever the last configuration reload attempt was successful.
		# TYPE cortex_alertmanager_config_last_reload_successful gauge
		cortex_alertmanager_config_last_reload_successful{user="user1"} 1
		cortex_alertmanager_config_last_reload_successful{user="user2"} 1
		cortex_alertmanager_config_last_reload_successful{user="user3"} 1
		cortex_alertmanager_config_last_reload_successful{user="user4"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	// Ensure the config can be unpromoted.
	userGrafanaCfg.Promoted = false
	require.NoError(t, store.SetGrafanaAlertConfig(ctx, userGrafanaCfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Equal(t, emptyMimirAmConfig.fingerprint(), am.cfgs["user4"])

	// Ensure the Grafana config is used when it's promoted again.
	userGrafanaCfg.Promoted = true
	require.NoError(t, store.SetGrafanaAlertConfig(ctx, userGrafanaCfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Equal(t, grafanaAlertConfigDesc.fingerprint(), am.cfgs["user4"])

	// Add a Mimir fallback config for the same user.
	defaultConfig := alertspb.AlertConfigDesc{
		User:      "user4",
		RawConfig: am.fallbackConfig,
	}
	expectedDefaultAmConfig := amConfig{
		User:            defaultConfig.User,
		RawConfig:       defaultConfig.RawConfig,
		Templates:       []definition.PostableApiTemplate{},
		TmplExternalURL: url,
	}

	require.NoError(t, store.SetAlertConfig(ctx, defaultConfig))

	// The Grafana config + Mimir global config section should be used.
	require.NoError(t, am.loadAndSyncConfigs(context.Background(), reasonPeriodic))

	var gCfg GrafanaAlertmanagerConfig
	require.NoError(t, json.Unmarshal([]byte(userGrafanaCfg.RawConfig), &gCfg))
	mCfg, err := definition.LoadCompat([]byte(defaultConfig.RawConfig))
	require.NoError(t, err)

	gCfg.AlertmanagerConfig.Global = mCfg.Global

	rawCfg, err := json.Marshal(gCfg.AlertmanagerConfig)
	require.NoError(t, err)

	expCfg := amConfig{
		User:               "user4",
		RawConfig:          string(rawCfg),
		UsingGrafanaConfig: true,
		TmplExternalURL:    externalUrl,
		EmailConfig: alertingReceivers.EmailSenderConfig{
			AuthPassword: "",
			AuthUser:     "",
			CertFile:     "",
			ContentTypes: []string{
				"text/html",
			},
			EhloIdentity:  "localhost",
			ExternalURL:   "test.grafana.com",
			FromName:      "Grafana",
			FromAddress:   smtpConfig.FromAddress,
			StaticHeaders: smtpConfig.StaticHeaders,
			SentBy:        "Mimir vunknown",
		},
	}
	require.Equal(t, expCfg.fingerprint(), am.cfgs["user4"])

	// Ensure the Grafana config is not ignored when it's marked as default.
	userGrafanaCfg.Default = true
	require.NoError(t, store.SetGrafanaAlertConfig(ctx, userGrafanaCfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Equal(t, expCfg.fingerprint(), am.cfgs["user4"])

	// Ensure the Grafana config is ignored when it's empty.
	userGrafanaCfg.Default = false
	userGrafanaCfg.RawConfig = ""
	require.NoError(t, store.SetGrafanaAlertConfig(ctx, userGrafanaCfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)
	require.Equal(t, expectedDefaultAmConfig.fingerprint(), am.cfgs["user4"])

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
		cortex_alertmanager_config_last_reload_successful{user="user4"} 1
	`), "cortex_alertmanager_config_last_reload_successful"))

	// Ensure when a 3rd config is re-added, it is synced correctly
	require.NoError(t, store.SetAlertConfig(ctx, user3Cfg))

	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	currentConfigFp, cfgExists = am.cfgs["user3"]
	require.True(t, cfgExists)
	expectedFp = amConfigFromMimirConfig(user3Cfg, cfg.ExternalURL.URL).fingerprint()
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
		cortex_alertmanager_config_last_reload_successful{user="user4"} 1
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
		require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
			User:      "user5",
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
			cortex_alertmanager_config_last_reload_successful{user="user4"} 1
			cortex_alertmanager_config_last_reload_successful{user="user5"} 0
		`), "cortex_alertmanager_config_last_reload_successful"))

		_, amExists := am.alertmanagers["user5"]
		require.False(t, amExists)
	})

	t.Run("when bad templates are loaded", func(t *testing.T) {
		require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
			User:      "user6",
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
			cortex_alertmanager_config_last_reload_successful{user="user4"} 1
			cortex_alertmanager_config_last_reload_successful{user="user5"} 0
			cortex_alertmanager_config_last_reload_successful{user="user6"} 0
		`), "cortex_alertmanager_config_last_reload_successful"))

		_, amExists := am.alertmanagers["user6"]
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
				require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
				alerts := types.Alerts(&types.Alert{
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
	require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
		require.NoError(t, alertStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
			User:      user1,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
			User:      user2,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
		require.NoError(t, alertStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
			User:      user1,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
		require.NoError(t, alertStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
			User:      user1,
			RawConfig: simpleConfigOne,
			Templates: []*alertspb.TemplateDesc{},
		}))
		require.NoError(t, alertStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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

	// Request when fallback user configuration is used, as user hasn't
	// created a configuration yet.
	req := httptest.NewRequest("GET", externalURL.String(), nil)
	ctx := user.InjectOrgID(req.Context(), "user1")

	{
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		_ = w.Result()
		require.Equal(t, 301, w.Code) // redirect to UI
	}

	// Create a configuration for the user in storage.
	require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
		User:      "user1",
		RawConfig: simpleConfigTwo,
		Templates: []*alertspb.TemplateDesc{},
	}))

	// Make the alertmanager pick it up.
	err = am.loadAndSyncConfigs(context.Background(), reasonPeriodic)
	require.NoError(t, err)

	// Request when AM is active.
	{
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		require.Equal(t, 301, w.Code) // redirect to UI
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
		// Request when the alertmanager is gone should result in setting the
		// default fallback config, thus redirecting to the ui.
		w := httptest.NewRecorder()
		am.ServeHTTP(w, req.WithContext(ctx))

		_ = w.Result()
		require.Equal(t, 301, w.Code) // redirect to UI
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
	const testGrafanaUser = "user1"
	const testMimirUser = "user2"

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

	// Create a tenant with a default Grafana and an empty Mimir config.
	// It should be skipped by the MOA.
	ctx := context.Background()
	require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
		User: testGrafanaUser,
	}))
	smtpConfig := &alertspb.SmtpConfig{
		EhloIdentity:   "test-identity",
		FromAddress:    "test@test.com",
		FromName:       "Test Name",
		Host:           "test:8080",
		Password:       "test password",
		SkipVerify:     true,
		StartTlsPolicy: "test",
		StaticHeaders:  map[string]string{"test-key": "test-value"},
		User:           "test-user",
	}
	require.NoError(t, store.SetGrafanaAlertConfig(ctx, alertspb.GrafanaAlertConfigDesc{
		User:       testGrafanaUser,
		RawConfig:  grafanaConfig,
		Promoted:   true,
		Default:    true,
		SmtpConfig: smtpConfig,
	}))

	// Create another tenant with an empty Mimir config.
	// It should be skipped by the MOA.
	require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
		User: testMimirUser,
	}))

	// Sync configurations, the Alertmanagers shouldn't be initialized.
	err = am.loadAndSyncConfigs(ctx, reasonPeriodic)
	require.NoError(t, err)
	require.Len(t, am.alertmanagers, 0)

	// Make requests as the users. The Alertmanagers should be initialized.
	req := httptest.NewRequest("GET", externalURL.String()+"/api/v2/status", nil)
	w := httptest.NewRecorder()

	require.NoError(t, err)
	am.ServeHTTP(w, req.WithContext(user.InjectOrgID(req.Context(), testGrafanaUser)))
	require.Equal(t, http.StatusOK, w.Result().StatusCode)
	require.Len(t, am.alertmanagers, 1)

	// The configuration should have the custom SMTP settings.
	exp := alertingReceivers.EmailSenderConfig{
		EhloIdentity:   "test-identity",
		FromAddress:    "test@test.com",
		FromName:       "Test Name",
		Host:           "test:8080",
		AuthPassword:   "test password",
		SkipVerify:     true,
		StartTLSPolicy: "test",
		StaticHeaders:  map[string]string{"test-key": "test-value"},
		AuthUser:       "test-user",

		ContentTypes: []string{"text/html"}, // Added by default
		SentBy:       "Mimir vunknown",      // The version in tests is "unknown"
	}
	gAM, ok := am.alertmanagers[testGrafanaUser]
	require.True(t, ok)
	require.Equal(t, exp, gAM.emailCfg)

	w = httptest.NewRecorder()
	am.ServeHTTP(w, req.WithContext(user.InjectOrgID(req.Context(), testMimirUser)))
	require.Equal(t, http.StatusOK, w.Result().StatusCode)
	require.Len(t, am.alertmanagers, 2)

	// Set the idle period to 0.
	// The Alertmanagers should be turned off after the next sync.
	am.cfg.GrafanaAlertmanagerIdleGracePeriod = 0
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
	require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
			alertStore := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, bkt, nil, log.NewNopLogger())

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
				require.NoError(t, alertStore.SetAlertConfig(context.Background(), alertspb.AlertConfigDesc{
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
	store := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, bkt, nil, log.NewNopLogger())

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
	require.NoError(t, mockStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
				require.NoError(t, mockStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
			silence := types.Silence{
				Matchers: labels.Matchers{
					{Name: "instance", Value: "prometheus-one"},
				},
				Comment:  "Created for a test case.",
				StartsAt: time.Now(),
				EndsAt:   time.Now().Add(time.Hour),
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
				require.NoError(t, mockStore.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
				silence := types.Silence{
					Matchers: labels.Matchers{
						{Name: "instance", Value: "prometheus-one"},
					},
					Comment:  "Created for a test case.",
					StartsAt: time.Now(),
					EndsAt:   time.Now().Add(time.Hour),
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
	cfg := bucketclient.BucketAlertStoreConfig{FetchGrafanaConfig: true}
	return bucketclient.NewBucketAlertStore(cfg, objstore.NewInMemBucket(), nil, log.NewNopLogger())
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
	require.NoError(t, store.SetAlertConfig(ctx, alertspb.AlertConfigDesc{
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
	ctx = notify.WithGroupKey(ctx, "key")
	ctx = notify.WithRepeatInterval(ctx, time.Minute)
	ctx = notify.WithNow(ctx, time.Now())

	// Verify that rate-limiter is in place for email notifier.
	_, _, err = uam.lastPipeline.Exec(ctx, log.NewNopLogger(), &types.Alert{})
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

func TestComputeConfig(t *testing.T) {
	store := prepareInMemoryAlertStore()
	reg := prometheus.NewPedanticRegistry()
	cfg := mockAlertmanagerConfig(t)
	am := setupSingleMultitenantAlertmanager(t, cfg, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg)

	reg2 := prometheus.NewPedanticRegistry()
	cfg2 := mockAlertmanagerConfig(t)
	amWithSuffix := setupSingleMultitenantAlertmanager(t, cfg2, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg2)

	reg3 := prometheus.NewPedanticRegistry()
	cfg3 := mockAlertmanagerConfig(t)
	cfg3.StrictInitializationEnabled = true
	amWithStrictInit := setupSingleMultitenantAlertmanager(t, cfg3, store, nil, featurecontrol.NoopFlags{}, log.NewNopLogger(), reg3)

	testTenant := "test-tenant"

	tenantReceivingRequests := "test-tenant-receiving"
	amWithStrictInit.lastRequestTime.Store(tenantReceivingRequests, time.Now().Unix())
	amWithSuffix.lastRequestTime.Store(tenantReceivingRequests+"-grafana", time.Now().Unix())

	tenantReceivingRequestsExpired := "test-tenant-idle"
	amWithStrictInit.lastRequestTime.Store(tenantReceivingRequestsExpired, time.Now().Add(-time.Hour).Unix())
	amWithSuffix.lastRequestTime.Store(tenantReceivingRequestsExpired+"-grafana", time.Now().Add(-time.Hour).Unix())

	var grafanaCfg GrafanaAlertmanagerConfig
	require.NoError(t, json.Unmarshal([]byte(grafanaConfig), &grafanaCfg))

	grafanaExternalURL := "https://grafana.com"
	grafanaExternalURLParsed, err := url.Parse("https://grafana.com")
	require.NoError(t, err)

	fallbackCfg, err := definition.LoadCompat([]byte(am.fallbackConfig))
	require.NoError(t, err)

	testFromAddress := "test-instance@grafana.com"
	testHeaders := map[string]string{"Test-Header-1": "test-value-1", "Test-Header-2": "test-value-2"}
	smtpConfig := &alertspb.SmtpConfig{
		FromAddress:   testFromAddress,
		StaticHeaders: testHeaders,
	}
	grafanaCfg.AlertmanagerConfig.Global = fallbackCfg.Global
	combinedCfg, err := json.Marshal(grafanaCfg.AlertmanagerConfig)
	require.NoError(t, err)

	baseEmailCfg := alertingReceivers.EmailSenderConfig{
		FromName:     "Grafana",
		EhloIdentity: "localhost",
		ExternalURL:  "https://grafana.com",
		ContentTypes: []string{"text/html"},
		SentBy:       "Mimir vunknown", // no 'version' flag passed in tests.
	}

	patchedEmailCfg := alertingReceivers.EmailSenderConfig{
		FromName:      "Grafana",
		EhloIdentity:  "localhost",
		ExternalURL:   "https://grafana.com",
		ContentTypes:  []string{"text/html"},
		FromAddress:   testFromAddress,
		StaticHeaders: testHeaders,
		SentBy:        "Mimir vunknown", // no 'version' flag passed in tests.
	}

	mimirExternalURL, err := url.Parse(am.cfg.ExternalURL.String())
	require.NoError(t, err)
	tests := []struct {
		name       string
		cfg        alertspb.AlertConfigDescs
		expStartAM bool
		expErr     string
		expCfg     amConfig
	}{
		{
			name: "no grafana configuration, custom mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: simpleConfigOne,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            testTenant,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, default mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: am.fallbackConfig,
				},
			},
			expCfg: amConfig{
				User:            testTenant,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, empty mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: testTenant,
				},
			},
			expCfg: amConfig{
				User:            testTenant,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, custom mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: simpleConfigOne,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, default mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: am.fallbackConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, empty mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequests,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, custom mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: simpleConfigOne,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, default mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: am.fallbackConfig,
				},
			},
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "no grafana configuration, empty mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequestsExpired,
				},
			},
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, custom mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            testTenant,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, default mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            testTenant,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, empty mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: testTenant,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            testTenant,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, custom mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, default mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, empty mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequests,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, custom mimir config, idle tenant",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, default mimir config, idle tenant",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "empty grafana configuration, empty mimir config, idle tenant",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequestsExpired,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, custom mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            testTenant,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, default mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            testTenant,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, empty mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: testTenant,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            testTenant,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, custom mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, default mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, empty mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequests,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, custom mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, default mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				RawConfig:       am.fallbackConfig,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "unpromoted grafana configuration, empty mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequestsExpired,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Promoted:    false,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "default grafana configuration, custom mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            testTenant,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "default grafana configuration, default mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      testTenant,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:               testTenant,
				RawConfig:          string(combinedCfg),
				TmplExternalURL:    grafanaExternalURLParsed,
				EmailConfig:        baseEmailCfg,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "default grafana configuration, empty mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: testTenant,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        testTenant,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:               testTenant,
				RawConfig:          string(combinedCfg),
				TmplExternalURL:    grafanaExternalURLParsed,
				EmailConfig:        baseEmailCfg,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "default grafana configuration, custom mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "default grafana configuration, default mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               tenantReceivingRequests,
				RawConfig:          string(combinedCfg),
				TmplExternalURL:    grafanaExternalURLParsed,
				EmailConfig:        baseEmailCfg,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "default grafana configuration, empty mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequests,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               tenantReceivingRequests,
				RawConfig:          string(combinedCfg),
				TmplExternalURL:    grafanaExternalURLParsed,
				EmailConfig:        baseEmailCfg,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "default grafana configuration, custom mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequestsExpired,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			name: "default grafana configuration, default mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:               tenantReceivingRequestsExpired,
				RawConfig:          string(combinedCfg),
				TmplExternalURL:    grafanaExternalURLParsed,
				EmailConfig:        baseEmailCfg,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "default grafana configuration, empty mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequestsExpired,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Default:     true,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
				},
			},
			expCfg: amConfig{
				User:               tenantReceivingRequestsExpired,
				RawConfig:          string(combinedCfg),
				EmailConfig:        baseEmailCfg,
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "usable grafana configuration, default mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      "user-grafana",
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        "user-grafana",
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               "user-grafana",
				RawConfig:          string(combinedCfg),
				EmailConfig:        patchedEmailCfg,
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "usable grafana configuration, empty mimir config",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: "user-grafana",
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        "user-grafana",
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               "user-grafana",
				RawConfig:          string(combinedCfg),
				EmailConfig:        patchedEmailCfg,
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "usable grafana configuration, default mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               tenantReceivingRequests,
				RawConfig:          string(combinedCfg),
				EmailConfig:        patchedEmailCfg,
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "usable grafana configuration, empty mimir config, receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequests,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               tenantReceivingRequests,
				RawConfig:          string(combinedCfg),
				EmailConfig:        patchedEmailCfg,
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "usable grafana configuration, default mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequestsExpired,
					RawConfig: am.fallbackConfig,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               tenantReceivingRequestsExpired,
				RawConfig:          string(combinedCfg),
				EmailConfig:        patchedEmailCfg,
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "usable grafana configuration, empty mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequestsExpired,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:               tenantReceivingRequestsExpired,
				RawConfig:          string(combinedCfg),
				EmailConfig:        patchedEmailCfg,
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			name: "usable grafana configuration with custom SMTP configs, empty mimir config, idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User: tenantReceivingRequestsExpired,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequestsExpired,
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig: &alertspb.SmtpConfig{
						EhloIdentity:   "test-identity",
						FromAddress:    "test@test.com",
						FromName:       "Test From Name",
						Host:           "http://test.com",
						Password:       "test-password",
						SkipVerify:     true,
						StartTlsPolicy: "test-policy",
						StaticHeaders:  nil,
						User:           "test-user",
					},
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:      tenantReceivingRequestsExpired,
				RawConfig: string(combinedCfg),
				EmailConfig: alertingReceivers.EmailSenderConfig{
					AuthPassword:   "test-password",
					AuthUser:       "test-user",
					ContentTypes:   []string{"text/html"},
					EhloIdentity:   "test-identity",
					ExternalURL:    grafanaExternalURL,
					FromName:       "Test From Name",
					FromAddress:    "test@test.com",
					Host:           "http://test.com",
					SkipVerify:     true,
					StartTLSPolicy: "test-policy",
					StaticHeaders:  nil,
					SentBy:         "Mimir vunknown",
				},
				TmplExternalURL:    grafanaExternalURLParsed,
				UsingGrafanaConfig: true,
			},
		},
		{
			// TODO: change once merging configs is implemented.
			name: "both mimir and grafana configurations (merging not implemented)",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      "user-grafana",
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        "user-grafana",
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            "user-grafana",
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			// TODO: change once merging configs is implemented.
			name: "both mimir and grafana configurations (merging not implemented), receiving requests",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
		{
			// TODO: change once merging configs is implemented.
			name: "both mimir and grafana configurations (merging not implemented), idle Alertmanager",
			cfg: alertspb.AlertConfigDescs{
				Mimir: alertspb.AlertConfigDesc{
					User:      tenantReceivingRequests,
					RawConfig: simpleConfigOne,
				},
				Grafana: alertspb.GrafanaAlertConfigDesc{
					User:        tenantReceivingRequests,
					RawConfig:   grafanaConfig,
					Promoted:    true,
					ExternalUrl: grafanaExternalURL,
					SmtpConfig:  smtpConfig,
				},
			},
			expStartAM: true,
			expCfg: amConfig{
				User:            tenantReceivingRequests,
				RawConfig:       simpleConfigOne,
				Templates:       []definition.PostableApiTemplate{},
				TmplExternalURL: mimirExternalURL,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, startAM, err := am.computeConfig(test.cfg)
			if test.expErr != "" {
				require.EqualError(t, err, test.expErr)
				return
			}
			require.NoError(t, err)

			require.True(t, startAM)
			require.Equal(t, test.expCfg, cfg)
		})

		t.Run(fmt.Sprintf("%s with strict initialization", test.name), func(t *testing.T) {
			// Set a recent last request time for the tenant receiving requests.
			amWithStrictInit.lastRequestTime.Store(tenantReceivingRequests, time.Now().Unix())
			amWithSuffix.lastRequestTime.Store(tenantReceivingRequests+"-grafana", time.Now().Unix())

			cfg, startAM, err := amWithStrictInit.computeConfig(test.cfg)
			if test.expErr != "" {
				require.EqualError(t, err, test.expErr)
				return
			}
			require.NoError(t, err)

			require.Equal(t, test.expStartAM, startAM)
			if startAM {
				require.Equal(t, test.expCfg, cfg)
			}
		})
	}
}

func Test_amConfigFingerprint(t *testing.T) {
	const expectedTotalFields = 23 // Total fields: 3 (PostableApiTemplate) + 14 (EmailSenderConfig) + 6 (amConfig)
	t.Run("ensure all fields in the fingerprint", func(t *testing.T) {
		// Helper function to get field count of a struct
		getFieldCount := func(v interface{}) int {
			t := reflect.TypeOf(v)
			if t.Kind() == reflect.Ptr {
				t = t.Elem()
			}
			return t.NumField()
		}

		// Calculate total fields across all structs
		totalFields := 0
		totalFields += getFieldCount(definition.PostableApiTemplate{})
		totalFields += getFieldCount(alertingReceivers.EmailSenderConfig{})
		totalFields += getFieldCount(amConfig{})

		require.Equalf(t, expectedTotalFields, totalFields, "Total fields across structs is %d, expected %d; new fields may require updating fingerprint method", totalFields, expectedTotalFields)
	})

	url, err := url.Parse("http://localhost")
	require.NoError(t, err)

	fullConfig := amConfig{
		User:      "user-grafana",
		RawConfig: simpleConfigOne,
		Templates: []definition.PostableApiTemplate{
			{
				Name:    "test",
				Content: "test",
				Kind:    definition.MimirTemplateKind,
			},
			{
				Name:    "test2",
				Content: "test2",
				Kind:    definition.GrafanaTemplateKind,
			},
			{
				Name:    "test3",
				Content: "test3",
				Kind:    definition.GrafanaTemplateKind,
			},
		},
		TmplExternalURL: url,
		EmailConfig: alertingReceivers.EmailSenderConfig{
			AuthPassword:   "custom-password",
			AuthUser:       "custom-user",
			ContentTypes:   []string{"text/html", "text/plain"},
			EhloIdentity:   "custom-identity",
			ExternalURL:    "http://custom-url",
			FromAddress:    "custom@address.com",
			FromName:       "Custom From Name",
			Host:           "custom-host",
			SentBy:         "Mimir vunknown",
			SkipVerify:     true,
			StartTLSPolicy: "custom-policy",
			StaticHeaders:  map[string]string{"test": "test", "test2": "test2", "test3": "test3"},
		},
	}

	jsonCfg, err := json.Marshal(fullConfig)
	require.NoError(t, err)

	t.Run("fingerprint should be stable", func(t *testing.T) {
		expected := fullConfig.fingerprint()

		// do it many times to make sure order of elements in the map does not affect fingerprint
		for i := 0; i < 100; i++ {
			cfg2 := amConfig{}
			require.NoError(t, json.Unmarshal(jsonCfg, &cfg2)) // copy structure
			assert.Empty(t, cmp.Diff(fullConfig, cfg2, cmp.AllowUnexported(amConfig{})))
			rand.Shuffle(len(cfg2.Templates), func(i, j int) {
				cfg2.Templates[i], cfg2.Templates[j] = cfg2.Templates[j], cfg2.Templates[i]
			})
			// copy map to shuffle elements
			cp := map[string]string{}
			maps.Copy(cp, cfg2.EmailConfig.StaticHeaders)
			cfg2.EmailConfig.StaticHeaders = cp

			rand.Shuffle(len(cfg2.EmailConfig.ContentTypes), func(i, j int) {
				cfg2.EmailConfig.ContentTypes[i], cfg2.EmailConfig.ContentTypes[j] = cfg2.EmailConfig.ContentTypes[j], cfg2.EmailConfig.ContentTypes[i]
			})

			require.Equal(t, expected, cfg2.fingerprint())
		}
	})

	t.Run("fingerprint should change", func(t *testing.T) {
		cfg := amConfig{}
		require.NoError(t, json.Unmarshal(jsonCfg, &cfg)) // copy structure
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

		lastFingerprint := cfg.fingerprint()
		assertField := func(prefix string) func(fieldName string) {
			return func(fieldName string) {
				newFP := cfg.fingerprint()
				assert.NotEqualf(t, lastFingerprint, newFP, "Changes in fields [%s%s] did not cause fingerprint to change", prefix, fieldName)
				lastFingerprint = newFP
			}
		}

		setStringFieldsWithRandomValue(reflect.ValueOf(&cfg).Elem(), assertField(""))
		setStringFieldsWithRandomValue(reflect.ValueOf(&cfg.EmailConfig).Elem(), assertField("EmailConfig."))
		setStringFieldsWithRandomValue(reflect.ValueOf(&cfg.Templates[1]).Elem(), assertField("Templates[1]."))
		cfg.Templates = append(cfg.Templates, definition.PostableApiTemplate{
			Name:    "test3",
			Content: "test3",
			Kind:    definition.GrafanaTemplateKind,
		})
		assertField("")("Templates")
		notChecked--

		cfg.TmplExternalURL = nil
		assertField("")("TmplExternalURL")
		cfg.TmplExternalURL, err = url.Parse("http://new-url")
		require.NoError(t, err)
		assertField("")("TmplExternalURL")
		notChecked--

		cfg.EmailConfig.ContentTypes = []string{"text/plain"}
		assertField("EmailConfig.")("ContentTypes")
		notChecked--

		cfg.EmailConfig.StaticHeaders = map[string]string{"test2": "test", "test": "test2", "test3": "test3"}
		assertField("EmailConfig.")("StaticHeaders")
		notChecked--

		cfg.EmailConfig = alertingReceivers.EmailSenderConfig{}
		assertField("")("EmailConfig")
		notChecked--

		require.Equal(t, 0, notChecked)
	})
}

func TestSyncStates(t *testing.T) {
	user := "test-user"
	externalURL, err := url.Parse("http://test.com")
	require.NoError(t, err)

	// Create test Grafana state.
	var buf bytes.Buffer
	_, err = pbutil.WriteDelimited(&buf, &pb.MeshEntry{
		Entry: &pb.Entry{
			Receiver:     &pb.Receiver{GroupName: `Grafana`, Integration: "grafanaIntegration", Idx: 0},
			GroupKey:     []byte(`{}/{grafana="true"}/{receiver="grafana webhook"}:{alertname="grafana test"}`),
			Timestamp:    time.Now(),
			FiringAlerts: []uint64{14588439739663070854},
		},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	grafanaNflog := make([]byte, buf.Len())
	copy(grafanaNflog, buf.Bytes())
	buf.Reset()

	_, err = pbutil.WriteDelimited(&buf, &silencepb.MeshSilence{
		Silence: &silencepb.Silence{
			Id: "grafana silence",
		},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	grafanaSilences := make([]byte, buf.Len())
	copy(grafanaSilences, buf.Bytes())
	buf.Reset()

	// Create test Mimir state.
	_, err = pbutil.WriteDelimited(&buf, &pb.MeshEntry{
		Entry: &pb.Entry{
			Receiver:     &pb.Receiver{GroupName: `Mimir`, Integration: "mimirIntegration", Idx: 0},
			GroupKey:     []byte(`{}/{mimir="true"}/{receiver="mimir webhook"}:{alertname="mimir test"}`),
			Timestamp:    time.Now(),
			FiringAlerts: []uint64{14588439739663070854},
		},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	mimirNflog := make([]byte, buf.Len())
	copy(mimirNflog, buf.Bytes())
	buf.Reset()

	_, err = pbutil.WriteDelimited(&buf, &silencepb.MeshSilence{
		Silence: &silencepb.Silence{
			Id: "mimir silence",
		},
		ExpiresAt: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	mimirSilences := make([]byte, buf.Len())
	copy(mimirSilences, buf.Bytes())
	buf.Reset()

	testMimirState := alertspb.FullStateDesc{
		State: &clusterpb.FullState{
			Parts: []clusterpb.Part{
				{
					Key:  nflogStateKeyPrefix + user,
					Data: mimirNflog,
				},
				{
					Key:  silencesStateKeyPrefix + user,
					Data: mimirSilences,
				},
			},
		},
	}

	ctx := context.Background()
	tests := []struct {
		name                 string
		cfg                  amConfig
		mimirState           *alertspb.FullStateDesc
		parts                map[string][]byte
		expNoNewAlertmanager bool
		expErr               string
	}{
		{
			name: "not using grafana config should be a no-op",
			cfg: amConfig{
				User: user,
			},
			expNoNewAlertmanager: true,
		},
		{
			name: "no grafana state should be a no-op",
			cfg: amConfig{
				User:               user,
				UsingGrafanaConfig: true,
			},
			expNoNewAlertmanager: true,
		},
		{
			name: "invalid alertmanager configuration should cause an error",
			cfg: amConfig{
				User:               user,
				RawConfig:          "invalid",
				TmplExternalURL:    externalURL,
				UsingGrafanaConfig: true,
			},
			parts:  map[string][]byte{"notifications": grafanaNflog},
			expErr: fmt.Sprintf("error creating new Alertmanager for user %[1]s: no usable Alertmanager configuration for %[1]s", user),
		},
		{
			name: "invalid part key",
			cfg: amConfig{
				User:               user,
				RawConfig:          simpleConfigOne,
				TmplExternalURL:    externalURL,
				UsingGrafanaConfig: true,
			},
			parts:  map[string][]byte{"invalid": []byte("invalid")},
			expErr: "unknown part key \"invalid\"",
		},
		{
			name: "valid alertmanager configuration should cause alertmanager creation and state promotion",
			cfg: amConfig{
				User:               user,
				RawConfig:          simpleConfigOne,
				TmplExternalURL:    externalURL,
				UsingGrafanaConfig: true,
			},
			parts: map[string][]byte{"notifications": grafanaNflog},
		},
		{
			name: "starting with existing mimir state should merge states",
			cfg: amConfig{
				User:               user,
				RawConfig:          simpleConfigOne,
				TmplExternalURL:    externalURL,
				UsingGrafanaConfig: true,
			},
			mimirState: &testMimirState,
			parts:      map[string][]byte{"notifications": grafanaNflog, "silences": grafanaSilences},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := prepareInMemoryAlertStore()
			am := setupSingleMultitenantAlertmanager(t,
				mockAlertmanagerConfig(t),
				store,
				nil,
				featurecontrol.NoopFlags{},
				log.NewNopLogger(),
				prometheus.NewPedanticRegistry(),
			)

			if test.mimirState != nil {
				require.NoError(t, store.SetFullState(ctx, user, *test.mimirState))
				require.NoError(t, am.setConfig(test.cfg))
				require.NotNil(t, am.alertmanagers[user])
				require.NoError(t, am.alertmanagers[user].WaitInitialStateSync(ctx))
				// Make broadcast a no-op for tests, otherwise it will block indefinitely.
				am.alertmanagers[user].silences.SetBroadcast(func(_ []byte) {})
			}
			if test.parts != nil {
				var s clusterpb.FullState
				for k, v := range test.parts {
					s.Parts = append(s.Parts, clusterpb.Part{
						Key:  k,
						Data: v,
					})
				}
				require.NoError(t, store.SetFullGrafanaState(ctx, user, alertspb.FullStateDesc{State: &s}))
			}

			if test.expErr != "" {
				err := am.syncStates(ctx, test.cfg)
				require.Error(t, err)
				require.Equal(t, test.expErr, err.Error())
				require.Nil(t, am.alertmanagers[user])
				return
			}

			// Make broadcast a no-op for tests.
			if am, ok := am.alertmanagers[user]; ok {
				am.silences.SetBroadcast(func(_ []byte) {})
			}

			require.NoError(t, am.syncStates(ctx, test.cfg))
			if test.expNoNewAlertmanager {
				require.Nil(t, am.alertmanagers[user])
				return
			}
			require.NotNil(t, am.alertmanagers[user])
			require.True(t, am.alertmanagers[user].usingGrafanaState.Load())

			// Grafana state should be deleted after merging.
			_, err = store.GetFullGrafanaState(ctx, user)
			require.Error(t, err)
			require.Equal(t, "alertmanager storage object not found", err.Error())

			// States should be merged.
			require.NotNil(t, am.alertmanagers[user])
			s, err := am.alertmanagers[user].getFullState()
			require.NoError(t, err)

			// One part for notification log, another one for silences.
			require.Len(t, s.Parts, 2)
			require.True(t, s.Parts[0].Key == nflogStateKeyPrefix+user || s.Parts[1].Key == nflogStateKeyPrefix+user)
			require.True(t, s.Parts[0].Key == silencesStateKeyPrefix+user || s.Parts[1].Key == silencesStateKeyPrefix+user)

			var silencesPart, nflogPart string
			for _, p := range s.Parts {
				require.True(t, p.Key == silencesStateKeyPrefix+user || p.Key == nflogStateKeyPrefix+user)
				if p.Key == silencesStateKeyPrefix+user {
					silencesPart = p.String()
				} else {
					nflogPart = p.String()
				}
			}

			// We don't need to check the exact content of the state, the merging logic is tested in the state replication tests.
			if _, ok := test.parts["notifications"]; ok {
				require.True(t, strings.Contains(nflogPart, "grafana webhook"))
			}
			if _, ok := test.parts["silences"]; ok {
				require.True(t, strings.Contains(silencesPart, "grafana silence"))
			}
			if test.mimirState != nil {
				require.True(t, strings.Contains(nflogPart, "mimir webhook"))
				require.True(t, strings.Contains(silencesPart, "mimir silence"))
			}
		})
	}

	preExistingAlertmanagerTests := []struct {
		name            string
		cfg             amConfig
		initialPromoted bool
		expPromoted     bool
	}{
		{
			name: "not using grafana config should toggle the promoted flag off",
			cfg: amConfig{
				User:            user,
				RawConfig:       simpleConfigOne,
				TmplExternalURL: externalURL,
			},
			initialPromoted: true,
			expPromoted:     false,
		},
		{
			name: "not using grafana config should be a no-op if the alertmanager is not promoted",
			cfg: amConfig{
				User:            user,
				RawConfig:       simpleConfigOne,
				TmplExternalURL: externalURL,
			},
			initialPromoted: false,
			expPromoted:     false,
		},
		{
			name: "attempting to promote an already promoted alertmanager should not change the flag",
			cfg: amConfig{
				User:               user,
				RawConfig:          simpleConfigOne,
				TmplExternalURL:    externalURL,
				UsingGrafanaConfig: true,
			},
			initialPromoted: true,
			expPromoted:     true,
		},
	}

	for _, test := range preExistingAlertmanagerTests {
		t.Run(test.name, func(t *testing.T) {
			store := prepareInMemoryAlertStore()
			am := setupSingleMultitenantAlertmanager(t,
				mockAlertmanagerConfig(t),
				store,
				nil,
				featurecontrol.NoopFlags{},
				log.NewNopLogger(),
				prometheus.NewPedanticRegistry(),
			)

			require.NoError(t, store.SetFullGrafanaState(ctx, test.cfg.User, alertspb.FullStateDesc{}))

			require.NoError(t, am.setConfig(amConfig{
				User:            test.cfg.User,
				RawConfig:       simpleConfigOne,
				TmplExternalURL: externalURL,
			}))
			require.NotNil(t, am.alertmanagers[test.cfg.User])
			am.alertmanagers[test.cfg.User].usingGrafanaState.Store(test.initialPromoted)

			require.NoError(t, am.syncStates(ctx, test.cfg))
			require.NotNil(t, am.alertmanagers[test.cfg.User])
			require.Equal(t, test.expPromoted, am.alertmanagers[test.cfg.User].usingGrafanaState.Load())
		})
	}
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
	maxGrafanaConfigSize           int
	maxGrafanaStateSize            int
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

func (m *mockAlertManagerLimits) AlertmanagerMaxGrafanaConfigSize(string) int {
	return m.maxGrafanaConfigSize
}

func (m *mockAlertManagerLimits) AlertmanagerMaxGrafanaStateSize(string) int {
	return m.maxGrafanaStateSize
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
