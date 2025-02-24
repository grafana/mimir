// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/alertmanager_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
//go:build requires_docker

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/go-openapi/strfmt"
	alertingmodels "github.com/grafana/alerting/models"
	alertingNotify "github.com/grafana/alerting/notify"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	v2_models "github.com/prometheus/alertmanager/api/v2/models"
	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/storage/bucket/s3"
)

const simpleAlertmanagerConfig = `
global:
  smtp_smarthost: 'localhost:25'
  smtp_from: 'youraddress@example.org'
route:
  receiver: dummy
  group_by: [group]
receivers:
  - name: dummy
    email_configs:
    - to: 'youraddress@example.org'
`

// uploadAlertmanagerConfig uploads the provided config to the minio bucket for the specified user.
// Uses default test minio credentials.
func uploadAlertmanagerConfig(minio *e2e.HTTPService, bucket, user, config string) error {
	client, err := s3.NewBucketClient(s3.Config{
		Endpoint:        minio.HTTPEndpoint(),
		Insecure:        true,
		BucketName:      bucket,
		AccessKeyID:     e2edb.MinioAccessKey,
		SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
	}, "test", log.NewNopLogger())
	if err != nil {
		return err
	}

	desc := alertspb.AlertConfigDesc{
		RawConfig: config,
		User:      user,
		Templates: []*alertspb.TemplateDesc{},
	}

	d, err := desc.Marshal()
	if err != nil {
		return err
	}

	return client.Upload(context.Background(), fmt.Sprintf("/alerts/%s", user), bytes.NewReader(d))
}

func TestAlertmanager(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	require.NoError(t, uploadAlertmanagerConfig(minio, alertsBucketName, "user-1", mimirAlertmanagerUserConfigYaml))

	alertmanager := e2emimir.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags(),
			AlertmanagerS3Flags(),
			AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
		),
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_config_last_reload_successful"))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Greater(0), "cortex_alertmanager_config_hash"))

	c, err := e2emimir.NewClient("", "", alertmanager.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches alertmanager_test_fixtures/user-1.yaml
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)

	// Ensure no service-specific metrics prefix is used by the wrong service.
	assertServiceMetricsPrefixes(t, AlertManager, alertmanager)

	// Test compression by inspecting the response Headers
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/api/v1/alerts", alertmanager.HTTPEndpoint()), nil)
	require.NoError(t, err)

	req.Header.Set("X-Scope-OrgID", "user-1")
	req.Header.Set("Accept-Encoding", "gzip")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Execute HTTP request
	res, err := http.DefaultClient.Do(req.WithContext(ctx))
	require.NoError(t, err)

	defer res.Body.Close()
	// We assert on the Vary header as the minimum response size for enabling compression is 1500 bytes.
	// This is enough to know whenever the handler for compression is enabled or not.
	require.Equal(t, "Accept-Encoding", res.Header.Get("Vary"))
}

// This test asserts that in classic mode it is not possible to upload configurations,
// create silences, or post alerts that contain UTF-8 on the left hand side of label
// matchers or label names. It can be deleted when the -alertmanager.utf8-strict-mode-enabled
// flag is removed.
func TestAlertmanagerClassicMode(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	require.NoError(t, uploadAlertmanagerConfig(minio, alertsBucketName, "user-1", mimirAlertmanagerUserConfigYaml))

	alertmanager := e2emimir.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags(),
			AlertmanagerS3Flags(),
			AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
			map[string]string{"-alertmanager.utf8-strict-mode-enabled": "false"},
		),
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_config_last_reload_successful"))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Greater(0), "cortex_alertmanager_config_hash"))

	c, err := e2emimir.NewClient("", "", alertmanager.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelFunc()

	// Should be able to use classic config, but not UTF-8 configuration.
	require.NoError(t, c.SetAlertmanagerConfig(ctx, mimirAlertmanagerUserClassicConfigYaml, nil))
	require.EqualError(t, c.SetAlertmanagerConfig(ctx, mimirAlertmanagerUserUTF8ConfigYaml, nil), "setting config failed with status 400 and error error validating Alertmanager config: bad matcher format: bar🙂=baz\n")

	// Should be able to create a silence with classic matchers, but not UTF-8 matchers.
	silenceID, err := c.CreateSilence(ctx, types.Silence{
		Matchers: amlabels.Matchers{
			{Name: "foo", Value: "bar"},
		},
		Comment:  "This is a test silence.",
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	})
	require.NoError(t, err)
	require.NotEmpty(t, silenceID)

	silenceID, err = c.CreateSilence(ctx, types.Silence{
		Matchers: amlabels.Matchers{
			{Name: "bar🙂", Value: "baz"},
		},
		Comment:  "This is a test silence.",
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	})
	require.EqualError(t, err, "creating the silence failed with status 400 and error \"invalid silence: invalid label matcher 0: invalid label name \\\"bar🙂\\\"\"\n")
	require.Empty(t, silenceID)

	// Should be able to post alerts with classic labels but not UTF-8 labels.
	require.NoError(t, c.SendAlertToAlermanager(ctx, &model.Alert{
		Labels: model.LabelSet{
			"foo": "bar",
		},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	}))
	require.EqualError(t, c.SendAlertToAlermanager(ctx, &model.Alert{
		Labels: model.LabelSet{
			"bar🙂": "baz",
		},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	}), "sending alert failed with status 400 and error \"invalid label set: invalid name \\\"bar🙂\\\"\"\n")
}

// This test asserts that in UTF-8 strict mode it is possible to upload configurations,
// create silences, and post alerts that contain UTF-8 on the left hand side of label
// matchers and label names. It is the opposite of TestAlertmanagerClassicMode. It should
// be merged with the TestAlertmanager test when the -alertmanager.utf8-strict-mode-enabled flag
// is removed.
func TestAlertmanagerUTF8StrictMode(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	require.NoError(t, uploadAlertmanagerConfig(minio, alertsBucketName, "user-1", mimirAlertmanagerUserConfigYaml))

	alertmanager := e2emimir.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags(),
			AlertmanagerS3Flags(),
			AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
			map[string]string{"-alertmanager.utf8-strict-mode-enabled": "true"},
		),
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_config_last_reload_successful"))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Greater(0), "cortex_alertmanager_config_hash"))

	c, err := e2emimir.NewClient("", "", alertmanager.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelFunc()

	// Should be able to use classic and UTF-8 configurations without error.
	require.NoError(t, c.SetAlertmanagerConfig(ctx, mimirAlertmanagerUserClassicConfigYaml, nil))
	require.NoError(t, c.SetAlertmanagerConfig(ctx, mimirAlertmanagerUserUTF8ConfigYaml, nil))

	// Should be able to create a silence with both classic matchers and UTF-8 matchers.
	silenceID, err := c.CreateSilence(ctx, types.Silence{
		Matchers: amlabels.Matchers{
			{Name: "foo", Value: "bar"},
		},
		Comment:  "This is a test silence.",
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	})
	require.NoError(t, err)
	require.NotEmpty(t, silenceID)

	silenceID, err = c.CreateSilence(ctx, types.Silence{
		Matchers: amlabels.Matchers{
			{Name: "bar🙂", Value: "baz"},
		},
		Comment:  "This is a test silence.",
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	})
	require.NoError(t, err)
	require.NotEmpty(t, silenceID)

	// Should be able to post alerts with both classic labels and UTF-8 labels.
	require.NoError(t, c.SendAlertToAlermanager(ctx, &model.Alert{
		Labels: model.LabelSet{
			"foo": "bar",
		},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	}))
	require.NoError(t, c.SendAlertToAlermanager(ctx, &model.Alert{
		Labels: model.LabelSet{
			"bar🙂": "baz",
		},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(time.Minute),
	}))
}

func TestAlertmanagerV1Deprecated(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(mimirAlertmanagerUserConfigYaml)))

	alertmanager := e2emimir.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags(),
			AlertmanagerLocalFlags(),
			AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
		),
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))

	endpoints := []string{
		"alerts",
		"receivers",
		"silence/id",
		"silences",
		"status",
	}
	for _, endpoint := range endpoints {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/alertmanager/api/v1/%s", alertmanager.HTTPEndpoint(), endpoint), nil)
		require.NoError(t, err)
		req.Header.Set("X-Scope-OrgID", "user-1")
		req.Header.Set("Accept-Encoding", "gzip")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Execute HTTP request
		res, err := http.DefaultClient.Do(req.WithContext(ctx))
		require.NoError(t, err)
		require.Equal(t, http.StatusGone, res.StatusCode)

		var response = struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}{}
		require.NoError(t, json.NewDecoder(res.Body).Decode(&response))
		require.Equal(t, "deprecated", response.Status)
		require.Equal(t, "The Alertmanager v1 API was deprecated in version 0.16.0 and is removed as of version 0.28.0 - please use the equivalent route in the v2 API", response.Error)
	}
}

func TestAlertmanagerLocalStore(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	require.NoError(t, s.StartAndWaitReady(consul))

	require.NoError(t, writeFileToSharedDir(s, "alertmanager_configs/user-1.yaml", []byte(mimirAlertmanagerUserConfigYaml)))

	alertmanager := e2emimir.NewAlertmanager(
		"alertmanager",
		mergeFlags(
			AlertmanagerFlags(),
			AlertmanagerLocalFlags(),
			AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
		),
	)
	require.NoError(t, s.StartAndWaitReady(alertmanager))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Equals(1), "cortex_alertmanager_config_last_reload_successful"))
	require.NoError(t, alertmanager.WaitSumMetrics(e2e.Greater(0), "cortex_alertmanager_config_hash"))

	c, err := e2emimir.NewClient("", "", alertmanager.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches alertmanager_test_fixtures/user-1.yaml
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)
}

func TestAlertmanagerStoreAPI(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(AlertmanagerFlags(),
		AlertmanagerS3Flags(),
		AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1))

	am := e2emimir.NewAlertmanager(
		"alertmanager",
		flags,
	)

	require.NoError(t, s.StartAndWaitReady(am))

	c, err := e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	// When no config is set yet it should use the default fallback config.
	cfg, err := c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "empty-receiver", cfg.Route.Receiver)
	require.Equal(t, "empty-receiver", cfg.Receivers[0].Name)

	err = c.SetAlertmanagerConfig(context.Background(), mimirAlertmanagerUserConfigYaml, map[string]string{})
	require.NoError(t, err)

	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_alertmanager_config_last_reload_successful"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))
	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Greater(0), []string{"cortex_alertmanager_config_last_reload_successful_seconds"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))

	cfg, err = c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// Ensure the returned status config matches the loaded config
	require.NotNil(t, cfg)
	require.Equal(t, "example_receiver", cfg.Route.Receiver)
	require.Len(t, cfg.Route.GroupByStr, 1)
	require.Equal(t, "example_groupby", cfg.Route.GroupByStr[0])
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "example_receiver", cfg.Receivers[0].Name)

	err = c.SendAlertToAlermanager(context.Background(), &model.Alert{Labels: model.LabelSet{"foo": "bar"}})
	require.NoError(t, err)

	require.NoError(t, am.WaitSumMetricsWithOptions(e2e.Equals(1), []string{"cortex_alertmanager_alerts_received_total"},
		e2e.WithLabelMatchers(labels.MustNewMatcher(labels.MatchEqual, "user", "user-1")),
		e2e.WaitMissingMetrics))

	err = c.DeleteAlertmanagerConfig(context.Background())
	require.NoError(t, err)

	// The deleted config is applied asynchronously, so we should wait until the metric
	// disappear for the specific user.
	require.NoError(t, am.WaitRemovedMetric("cortex_alertmanager_config_last_reload_successful", e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))
	require.NoError(t, am.WaitRemovedMetric("cortex_alertmanager_config_last_reload_successful_seconds", e2e.WithLabelMatchers(
		labels.MustNewMatcher(labels.MatchEqual, "user", "user-1"))))

	// Getting the config after the delete call should return the fallback config.
	cfg, err = c.GetAlertmanagerConfig(context.Background())
	require.NoError(t, err)
	require.Len(t, cfg.Receivers, 1)
	require.Equal(t, "empty-receiver", cfg.Route.Receiver)
	require.Equal(t, "empty-receiver", cfg.Receivers[0].Name)
}

func TestAlertmanagerSharding(t *testing.T) {
	tests := map[string]struct {
		replicationFactor int
	}{
		"RF = 2": {replicationFactor: 2},
		"RF = 3": {replicationFactor: 3},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			flags := mergeFlags(AlertmanagerFlags(),
				AlertmanagerS3Flags(),
				AlertmanagerGrafanaCompatibilityFlags())

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, alertsBucketName)
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			client, err := s3.NewBucketClient(s3.Config{
				Endpoint:        minio.HTTPEndpoint(),
				Insecure:        true,
				BucketName:      alertsBucketName,
				AccessKeyID:     e2edb.MinioAccessKey,
				SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
			}, "test", log.NewNopLogger())
			require.NoError(t, err)

			// Create and upload Alertmanager configurations.
			for i := 1; i <= 30; i++ {
				user := fmt.Sprintf("user-%d", i)
				desc := alertspb.AlertConfigDesc{
					RawConfig: simpleAlertmanagerConfig,
					User:      user,
					Templates: []*alertspb.TemplateDesc{},
				}

				d, err := desc.Marshal()
				require.NoError(t, err)
				err = client.Upload(context.Background(), fmt.Sprintf("/alerts/%s", user), bytes.NewReader(d))
				require.NoError(t, err)
			}

			// 3 instances, 30 configurations and a replication factor of 2 or 3.
			flags = mergeFlags(flags, AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), testCfg.replicationFactor))

			// Wait for the Alertmanagers to start.
			alertmanager1 := e2emimir.NewAlertmanager("alertmanager-1", flags)
			alertmanager2 := e2emimir.NewAlertmanager("alertmanager-2", flags)
			alertmanager3 := e2emimir.NewAlertmanager("alertmanager-3", flags)

			alertmanagers := e2emimir.NewCompositeMimirService(alertmanager1, alertmanager2, alertmanager3)

			// Start Alertmanager instances.
			for _, am := range alertmanagers.Instances() {
				require.NoError(t, s.StartAndWaitReady(am))
			}

			require.NoError(t, alertmanagers.WaitSumMetricsWithOptions(e2e.Equals(9), []string{"cortex_ring_members"}, e2e.WithLabelMatchers(
				labels.MustNewMatcher(labels.MatchEqual, "name", "alertmanager"),
				labels.MustNewMatcher(labels.MatchEqual, "state", "ACTIVE"),
			)))

			// We expect every instance to discover every configuration but only own a subset of them.
			require.NoError(t, alertmanagers.WaitSumMetrics(e2e.Equals(90), "cortex_alertmanager_tenants_discovered"))
			// We know that the ring has settled when every instance has some tenants and the total number of tokens have been assigned.
			// The total number of tenants across all instances is: total alertmanager configs * replication factor.
			require.NoError(t, alertmanagers.WaitSumMetricsWithOptions(e2e.Equals(float64(30*testCfg.replicationFactor)), []string{"cortex_alertmanager_config_last_reload_successful"}, e2e.SkipMissingMetrics))
			require.NoError(t, alertmanagers.WaitSumMetrics(e2e.Equals(float64(1152)), "cortex_ring_tokens_total"))

			// Now, let's make sure state is replicated across instances.
			// 1. Let's select a random tenant
			userID := "user-5"

			// 2. Let's create a silence
			comment := func(i int) string {
				return fmt.Sprintf("Silence Comment #%d", i)
			}
			silence := func(i int) types.Silence {
				return types.Silence{
					Matchers: amlabels.Matchers{
						{Name: "instance", Value: "prometheus-one"},
					},
					Comment:  comment(i),
					StartsAt: time.Now(),
					EndsAt:   time.Now().Add(time.Hour),
				}
			}

			// 2b. For each tenant, with a replication factor of 2 and 3 instances,
			// the user will not be present in one of the instances.
			// However, the distributor should route us to a correct instance.
			c1, err := e2emimir.NewClient("", "", alertmanager1.HTTPEndpoint(), "", userID)
			require.NoError(t, err)
			c2, err := e2emimir.NewClient("", "", alertmanager2.HTTPEndpoint(), "", userID)
			require.NoError(t, err)
			c3, err := e2emimir.NewClient("", "", alertmanager3.HTTPEndpoint(), "", userID)
			require.NoError(t, err)

			clients := []*e2emimir.Client{c1, c2, c3}

			waitForSilences := func(state string, amount int) error {
				return alertmanagers.WaitSumMetricsWithOptions(
					e2e.Equals(float64(amount)),
					[]string{"cortex_alertmanager_silences"},
					e2e.SkipMissingMetrics,
					e2e.WithLabelMatchers(
						labels.MustNewMatcher(labels.MatchEqual, "state", state),
					),
				)
			}

			var id1, id2, id3 string

			// Endpoint: POST /silences
			{
				id1, err = c1.CreateSilence(context.Background(), silence(1))
				assert.NoError(t, err)
				id2, err = c2.CreateSilence(context.Background(), silence(2))
				assert.NoError(t, err)
				id3, err = c3.CreateSilence(context.Background(), silence(3))
				assert.NoError(t, err)

				// Reading silences do not currently read from all replicas. We have to wait for
				// the silence to be replicated asynchronously, before we can reliably read them.
				require.NoError(t, waitForSilences("active", 3*testCfg.replicationFactor))
			}

			assertSilences := func(list []types.Silence, s1, s2, s3 types.SilenceState) {
				assert.Equal(t, 3, len(list))

				ids := make(map[string]types.Silence, len(list))
				for _, s := range list {
					ids[s.ID] = s
				}

				require.Contains(t, ids, id1)
				assert.Equal(t, comment(1), ids[id1].Comment)
				assert.Equal(t, s1, ids[id1].Status.State)
				require.Contains(t, ids, id2)
				assert.Equal(t, comment(2), ids[id2].Comment)
				assert.Equal(t, s2, ids[id2].Status.State)
				require.Contains(t, ids, id3)
				assert.Equal(t, comment(3), ids[id3].Comment)
				assert.Equal(t, s3, ids[id3].Status.State)
			}

			// Endpoint: GET /v2/silences
			{
				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateActive, types.SilenceStateActive, types.SilenceStateActive)
				}
			}

			// Endpoint: GET /v2/silence/{id}
			{
				for _, c := range clients {
					sil1, err := c.GetSilence(context.Background(), id1)
					require.NoError(t, err)
					assert.Equal(t, comment(1), sil1.Comment)
					assert.Equal(t, types.SilenceStateActive, sil1.Status.State)

					sil2, err := c.GetSilence(context.Background(), id2)
					require.NoError(t, err)
					assert.Equal(t, comment(2), sil2.Comment)
					assert.Equal(t, types.SilenceStateActive, sil2.Status.State)

					sil3, err := c.GetSilence(context.Background(), id3)
					require.NoError(t, err)
					assert.Equal(t, comment(3), sil3.Comment)
					assert.Equal(t, types.SilenceStateActive, sil3.Status.State)
				}
			}

			// Endpoint: GET /receivers
			{
				for _, c := range clients {
					list, err := c.GetReceivers(context.Background())
					assert.NoError(t, err)
					assert.ElementsMatch(t, list, []string{"dummy"})
				}
			}

			// Endpoint: GET /api/v1/grafana/receivers
			{
				for _, c := range clients {
					list, err := c.GetReceiversExperimental(context.Background())
					assert.NoError(t, err)
					assert.ElementsMatch(t, list, []alertingmodels.Receiver{
						{
							Name:   "dummy",
							Active: true,
							Integrations: []alertingmodels.Integration{
								{
									LastNotifyAttemptDuration: "0s",
									Name:                      "email",
								},
							},
						},
					})
				}
			}

			// Endpoint: GET /multitenant_alertmanager/status
			{
				for _, c := range clients {
					_, err := c.GetAlertmanagerStatusPage(context.Background())
					assert.NoError(t, err)
				}
			}

			// Endpoint: GET /status
			{
				for _, c := range clients {
					_, err := c.GetAlertmanagerConfig(context.Background())
					assert.NoError(t, err)
				}
			}

			// Endpoint: DELETE /silence/{id}
			{
				// Delete one silence via each instance. Listing the silences on
				// all other instances should yield the silence being expired.
				err = c1.DeleteSilence(context.Background(), id2)
				assert.NoError(t, err)

				// These waits are required as deletion replication is currently
				// asynchronous, and silence reading is not consistent. Once
				// merging is implemented on the read path, this is not needed.
				require.NoError(t, waitForSilences("expired", 1*testCfg.replicationFactor))

				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateActive, types.SilenceStateExpired, types.SilenceStateActive)
				}

				err = c2.DeleteSilence(context.Background(), id3)
				assert.NoError(t, err)
				require.NoError(t, waitForSilences("expired", 2*testCfg.replicationFactor))

				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateActive, types.SilenceStateExpired, types.SilenceStateExpired)
				}

				err = c3.DeleteSilence(context.Background(), id1)
				assert.NoError(t, err)
				require.NoError(t, waitForSilences("expired", 3*testCfg.replicationFactor))

				for _, c := range clients {
					list, err := c.GetSilences(context.Background())
					require.NoError(t, err)
					assertSilences(list, types.SilenceStateExpired, types.SilenceStateExpired, types.SilenceStateExpired)
				}
			}

			alert := func(i, g int) *model.Alert {
				return &model.Alert{
					Labels: model.LabelSet{
						"name":  model.LabelValue(fmt.Sprintf("alert_%d", i)),
						"group": model.LabelValue(fmt.Sprintf("group_%d", g)),
					},
					StartsAt: time.Now(),
					EndsAt:   time.Now().Add(time.Hour),
				}
			}

			alertNames := func(list []model.Alert) (r []string) {
				for _, a := range list {
					r = append(r, string(a.Labels["name"]))
				}
				return
			}

			// Endpoint: POST /alerts
			{
				err = c1.SendAlertToAlermanager(context.Background(), alert(1, 1))
				require.NoError(t, err)
				err = c2.SendAlertToAlermanager(context.Background(), alert(2, 1))
				require.NoError(t, err)
				err = c3.SendAlertToAlermanager(context.Background(), alert(3, 2))
				require.NoError(t, err)

				// Wait for the alerts to be received by every replica.
				require.NoError(t, alertmanagers.WaitSumMetricsWithOptions(
					e2e.Equals(float64(3*testCfg.replicationFactor)),
					[]string{"cortex_alertmanager_alerts_received_total"},
					e2e.SkipMissingMetrics))
			}

			// Endpoint: GET /v2/alerts
			{
				for _, c := range clients {
					list, err := c.GetAlerts(context.Background())
					require.NoError(t, err)
					assert.ElementsMatch(t, []string{"alert_1", "alert_2", "alert_3"}, alertNames(list))
				}
			}

			// Endpoint: GET /v2/alerts/groups
			{
				for _, c := range clients {
					list, err := c.GetAlertGroups(context.Background())
					require.NoError(t, err)

					assert.Equal(t, 2, len(list))
					groups := make(map[string][]model.Alert)
					for _, g := range list {
						groups[string(g.Labels["group"])] = g.Alerts
					}

					require.Contains(t, groups, "group_1")
					assert.ElementsMatch(t, []string{"alert_1", "alert_2"}, alertNames(groups["group_1"]))
					require.Contains(t, groups, "group_2")
					assert.ElementsMatch(t, []string{"alert_3"}, alertNames(groups["group_2"]))
				}
			}

			// Check the alerts were eventually written to every replica.
			require.NoError(t, alertmanagers.WaitSumMetricsWithOptions(
				e2e.Equals(float64(3*testCfg.replicationFactor)),
				[]string{"cortex_alertmanager_alerts_received_total"},
				e2e.SkipMissingMetrics))

			// Endpoint: Non-API endpoints such as the web user interface
			{
				for _, c := range clients {
					// Static paths - just check route, not content.
					_, err := c.GetAlertmanager(context.Background(), "/script.js")
					assert.NoError(t, err)
					_, err = c.GetAlertmanager(context.Background(), "/favicon.ico")
					assert.NoError(t, err)

					// Status paths - fixed response.
					body, err := c.GetAlertmanager(context.Background(), "/-/healthy")
					assert.NoError(t, err)
					assert.Equal(t, "OK", body)
					body, err = c.GetAlertmanager(context.Background(), "/-/ready")
					assert.NoError(t, err)
					assert.Equal(t, "OK", body)

					// Disabled paths - should 404.
					_, err = c.GetAlertmanager(context.Background(), "/metrics")
					assert.EqualError(t, err, e2emimir.ErrNotFound.Error())
					_, err = c.GetAlertmanager(context.Background(), "/debug/pprof")
					assert.EqualError(t, err, e2emimir.ErrNotFound.Error())
					err = c.PostAlertmanager(context.Background(), "/-/reload")
					assert.EqualError(t, err, e2emimir.ErrNotFound.Error())
				}
			}
		})
	}
}

func TestAlertmanagerShardingScaling(t *testing.T) {
	// Note that we run the test with the persister interval reduced in
	// order to speed up the testing. However, this could mask issues with
	// the syncing state from replicas. Therefore, we also run the tests
	// with the sync interval increased (with the caveat that we cannot
	// test the all-replica shutdown/restart).
	tests := map[string]struct {
		replicationFactor int
		withPersister     bool
	}{
		"RF = 2 with persister":    {replicationFactor: 2, withPersister: true},
		"RF = 3 with persister":    {replicationFactor: 3, withPersister: true},
		"RF = 2 without persister": {replicationFactor: 2, withPersister: false},
		"RF = 3 without persister": {replicationFactor: 3, withPersister: false},
	}

	for testName, testCfg := range tests {
		t.Run(testName, func(t *testing.T) {
			s, err := e2e.NewScenario(networkName)
			require.NoError(t, err)
			defer s.Close()

			// Start dependencies.
			consul := e2edb.NewConsul()
			minio := e2edb.NewMinio(9000, alertsBucketName)
			require.NoError(t, s.StartAndWaitReady(consul, minio))

			client, err := s3.NewBucketClient(s3.Config{
				Endpoint:        minio.HTTPEndpoint(),
				Insecure:        true,
				BucketName:      alertsBucketName,
				AccessKeyID:     e2edb.MinioAccessKey,
				SecretAccessKey: flagext.SecretWithValue(e2edb.MinioSecretKey),
			}, "test", log.NewNopLogger())
			require.NoError(t, err)

			// Create and upload Alertmanager configurations.
			numUsers := 20
			for i := 1; i <= numUsers; i++ {
				user := fmt.Sprintf("user-%d", i)
				desc := alertspb.AlertConfigDesc{
					RawConfig: simpleAlertmanagerConfig,
					User:      user,
					Templates: []*alertspb.TemplateDesc{},
				}

				d, err := desc.Marshal()
				require.NoError(t, err)
				err = client.Upload(context.Background(), fmt.Sprintf("/alerts/%s", user), bytes.NewReader(d))
				require.NoError(t, err)
			}

			persistInterval := "5h"
			if testCfg.withPersister {
				persistInterval = "5s"
			}

			flags := mergeFlags(AlertmanagerFlags(),
				AlertmanagerS3Flags(),
				AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), testCfg.replicationFactor),
				AlertmanagerPersisterFlags(persistInterval))

			instances := make([]*e2emimir.MimirService, 0)

			// Helper to start an instance.
			startInstance := func() *e2emimir.MimirService {
				i := len(instances) + 1
				am := e2emimir.NewAlertmanager(fmt.Sprintf("alertmanager-%d", i), flags)
				require.NoError(t, s.StartAndWaitReady(am))
				instances = append(instances, am)
				return am
			}

			// Helper to stop the most recently started instance.
			popInstance := func() {
				require.Greater(t, len(instances), 0)
				last := len(instances) - 1
				require.NoError(t, s.Stop(instances[last]))
				instances = instances[:last]
			}

			// Helper to validate the system wide metrics as we add and remove instances.
			validateMetrics := func(expectedSilences int) {
				// Check aggregate metrics across all instances.
				ams := e2emimir.NewCompositeMimirService(instances...)

				// All instances should discover all tenants.
				require.NoError(t, ams.WaitSumMetrics(
					e2e.Equals(float64(numUsers*len(instances))),
					"cortex_alertmanager_tenants_discovered"))

				// If the number of instances has not yet reached the replication
				// factor, then effective replication will be reduced.
				var expectedReplication int
				if len(instances) <= testCfg.replicationFactor {
					expectedReplication = len(instances)
				} else {
					expectedReplication = testCfg.replicationFactor
				}

				require.NoError(t, ams.WaitSumMetrics(
					e2e.Equals(float64(numUsers*expectedReplication)),
					"cortex_alertmanager_tenants_owned"))

				require.NoError(t, ams.WaitSumMetrics(
					e2e.Equals(float64(numUsers*expectedReplication)),
					"cortex_alertmanager_config_last_reload_successful"))

				require.NoError(t, ams.WaitSumMetricsWithOptions(
					e2e.Equals(float64(expectedSilences*expectedReplication)),
					[]string{"cortex_alertmanager_silences"},
					e2e.SkipMissingMetrics))
			}

			// Start up the first instance and use it to create some silences.
			numSilences := 0
			{
				am1 := startInstance()

				// Validate metrics with only the first instance running, before creating silences.
				validateMetrics(0)

				// Only create silences for every other user. It will be common that some users
				// have no silences, so some irregularity in the test is beneficial.
				for i := 1; i <= numUsers; i += 2 {
					user := fmt.Sprintf("user-%d", i)
					client, err := e2emimir.NewClient("", "", am1.HTTPEndpoint(), "", user)
					require.NoError(t, err)

					for j := 1; j <= 10; j++ {
						silence := types.Silence{
							Matchers: amlabels.Matchers{
								{Name: "instance", Value: "prometheus-one"},
							},
							Comment:  fmt.Sprintf("Silence Comment #%d", j),
							StartsAt: time.Now(),
							EndsAt:   time.Now().Add(time.Hour),
						}

						_, err = client.CreateSilence(context.Background(), silence)
						assert.NoError(t, err)
						numSilences++
					}
				}

				// Validate metrics after creating silences.
				validateMetrics(numSilences)

				// If we are testing with persistence, then check the persister actually activated.
				// It's unlikely that nothing has been persisted by now if correctly configured.
				if testCfg.withPersister {
					require.NoError(t, instances[0].WaitSumMetrics(
						e2e.Greater(0),
						"cortex_alertmanager_state_persist_total"))
				}
			}

			// Scale up by adding some number of new instances. We don't go too high to
			// keep the test run-time low (RF+2) - going higher has diminishing returns.
			{
				scale := (testCfg.replicationFactor + 2)
				for i := 2; i <= scale; i++ {
					_ = startInstance()
					validateMetrics(numSilences)
				}
			}

			// Scale down to a single instance. Note that typically scaling down would be performed
			// carefully, one instance at a time. For this test, the act of waiting for metrics
			// to reach expected values, essentially inhibits the scale down sufficiently.
			{
				for len(instances) >= 2 {
					popInstance()
					validateMetrics(numSilences)
				}
			}

			// Stop the last instance.
			popInstance()

			// Restart the first instance.
			_ = startInstance()

			// With persistence, then the silences will not be lost. Otherwise, they will.
			if testCfg.withPersister {
				validateMetrics(numSilences)
			} else {
				validateMetrics(0)
			}
		})
	}
}

func TestAlertmanagerGrafanaAlertmanagerAPI(t *testing.T) {
	testGrafanaConfig := `{
		"template_files": {},
		"alertmanager_config": {
			"route": {
				"receiver": "test_receiver",
				"group_by": ["alertname"]
			},
			"receivers": [{
				"name": "test_receiver",
				"grafana_managed_receiver_configs": [{
					"uid": "",
					"name": "email test",
					"type": "email",
					"disableResolveMessage": true,
					"settings": {
						"addresses": "test@test.com"
					},
					"secureSettings": null
				}]
			}],
			"templates": null
		}
	}`

	staticHeaders := map[string]string{
		"Header-1": "Value-1",
		"Header-2": "Value-2",
	}
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(AlertmanagerFlags(),
		AlertmanagerS3Flags(),
		AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
		AlertmanagerGrafanaCompatibilityFlags())

	am := e2emimir.NewAlertmanager(
		"alertmanager",
		flags,
	)
	require.NoError(t, s.StartAndWaitReady(am))

	// For Grafana Alertmanager configuration.
	{
		c, err := e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
		require.NoError(t, err)
		{
			var cfg *alertmanager.UserGrafanaConfig
			// When no config is set yet, it should not return anything.
			cfg, err = c.GetGrafanaAlertmanagerConfig(context.Background())
			require.EqualError(t, err, e2emimir.ErrNotFound.Error())
			require.Nil(t, cfg)

			// Now, let's set a config.
			now := time.Now().UnixMilli()
			err = c.SetGrafanaAlertmanagerConfig(context.Background(), now, testGrafanaConfig, "bb788eaa294c05ec556c1ed87546b7a9", "http://test.com", false, true, staticHeaders)
			require.NoError(t, err)

			// With that set, let's get it back.
			cfg, err = c.GetGrafanaAlertmanagerConfig(context.Background())
			require.NoError(t, err)
			require.Equal(t, now, cfg.CreatedAt)
		}

		// Let's store config for a different user as well.
		c, err = e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-5")
		require.NoError(t, err)
		{
			var cfg *alertmanager.UserGrafanaConfig
			// When no config is set yet, it should not return anything.
			cfg, err = c.GetGrafanaAlertmanagerConfig(context.Background())
			require.EqualError(t, err, e2emimir.ErrNotFound.Error())
			require.Nil(t, cfg)

			// Now, let's set a config.
			now := time.Now().UnixMilli()
			err = c.SetGrafanaAlertmanagerConfig(context.Background(), now, testGrafanaConfig, "bb788eaa294c05ec556c1ed87546b7a9", "http://test.com", false, true, staticHeaders)
			require.NoError(t, err)

			// With that set, let's get it back.
			cfg, err = c.GetGrafanaAlertmanagerConfig(context.Background())
			require.NoError(t, err)
			require.Equal(t, now, cfg.CreatedAt)

			// Now, let's delete it.
			err = c.DeleteGrafanaAlertmanagerConfig(context.Background())
			require.NoError(t, err)

			// Now that the config is deleted, it should not return anything again.
			cfg, err = c.GetGrafanaAlertmanagerConfig(context.Background())
			require.EqualError(t, err, e2emimir.ErrNotFound.Error())
			require.Nil(t, cfg)
		}
	}

	// For Grafana Alertmanager state.
	{
		c, err := e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
		require.NoError(t, err)
		{
			var state *alertmanager.UserGrafanaState
			// When no state is set yet, it should not return anything.
			state, err = c.GetGrafanaAlertmanagerState(context.Background())
			require.EqualError(t, err, e2emimir.ErrNotFound.Error())
			require.Nil(t, state)

			// Now, let's set the state.
			err = c.SetGrafanaAlertmanagerState(context.Background(), "ChEKBW5mbG9nEghzb21lZGF0YQ==")
			require.NoError(t, err)

			// With a state now set, let's get it back.
			state, err = c.GetGrafanaAlertmanagerState(context.Background())
			require.NoError(t, err)
			require.Equal(t, "ChEKBW5mbG9nEghzb21lZGF0YQ==", state.State)
		}

		// Let's store state for a different user as well.
		c, err = e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-5")
		require.NoError(t, err)
		{
			var state *alertmanager.UserGrafanaState
			// When no state is set yet, it should not return anything.
			state, err = c.GetGrafanaAlertmanagerState(context.Background())
			require.EqualError(t, err, e2emimir.ErrNotFound.Error())
			require.Nil(t, state)

			// Now, let's set the state.
			err = c.SetGrafanaAlertmanagerState(context.Background(), "ChEKBW5mbG9nEghzb21lZGF0YQ==")
			require.NoError(t, err)

			// With a state now set, let's get it back.
			state, err = c.GetGrafanaAlertmanagerState(context.Background())
			require.NoError(t, err)
			require.Equal(t, "ChEKBW5mbG9nEghzb21lZGF0YQ==", state.State)

			// Now, let's delete it.
			err = c.DeleteGrafanaAlertmanagerState(context.Background())
			require.NoError(t, err)

			// Now that the state is deleted, it should not return anything again.
			state, err = c.GetGrafanaAlertmanagerState(context.Background())
			require.EqualError(t, err, e2emimir.ErrNotFound.Error())
			require.Nil(t, state)
		}

	}
}

func TestAlertmanagerTestTemplates(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(AlertmanagerFlags(),
		AlertmanagerS3Flags(),
		AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
		AlertmanagerGrafanaCompatibilityFlags())

	am := e2emimir.NewAlertmanager(
		"alertmanager",
		flags,
	)
	require.NoError(t, s.StartAndWaitReady(am))

	c, err := e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	startTime, err := time.Parse(time.RFC3339, "2024-01-01T00:00:00Z")
	require.NoError(t, err)
	endTime, err := time.Parse(time.RFC3339, "2024-01-01T02:00:00Z")
	require.NoError(t, err)

	// Endpoint: POST /api/v1/grafana/templates/test
	ttConfig := alertingNotify.TestTemplatesConfigBodyParams{
		Alerts: []*alertingNotify.PostableAlert{
			{
				StartsAt:    strfmt.DateTime(startTime),
				EndsAt:      strfmt.DateTime(endTime),
				Annotations: v2_models.LabelSet{"annotation": "test annotation"},
				Alert: v2_models.Alert{
					GeneratorURL: strfmt.URI("http://www.grafana.com"),
					Labels:       v2_models.LabelSet{"label": "test label"},
				},
			},
		},
		Template: `{{ define "Testing123" }}\n  This is a test template\n{{ end }}`,
		Name:     "Testing123",
	}

	res, err := c.TestTemplatesExperimental(context.Background(), ttConfig)
	require.NoError(t, err)

	require.Len(t, res.Results, 1)
	require.Len(t, res.Errors, 0)

	tmplResult := res.Results[0]
	require.Equal(t, tmplResult.Name, "Testing123")
	require.Equal(t, tmplResult.Text, `\n  This is a test template\n`)
}

func TestAlertmanagerTestReceivers(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, alertsBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(AlertmanagerFlags(),
		AlertmanagerS3Flags(),
		AlertmanagerShardingFlags(consul.NetworkHTTPEndpoint(), 1),
		AlertmanagerGrafanaCompatibilityFlags())

	am := e2emimir.NewAlertmanager(
		"alertmanager",
		flags,
	)
	require.NoError(t, s.StartAndWaitReady(am))

	c, err := e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
	require.NoError(t, err)

	// Endpoint: POST /api/v1/grafana/receivers/test
	trConfig := alertingNotify.TestReceiversConfigBodyParams{
		Alert: &alertingNotify.TestReceiversConfigAlertParams{
			Annotations: model.LabelSet{"annotation": "test annotation"},
			Labels:      model.LabelSet{"label": "test label"},
		},
		Receivers: []*alertingNotify.APIReceiver{
			{
				GrafanaIntegrations: alertingNotify.GrafanaIntegrations{
					Integrations: []*alertingNotify.GrafanaIntegrationConfig{
						{
							UID:                   "uid",
							Name:                  "test integration",
							Type:                  "oncall",
							DisableResolveMessage: false,
							Settings:              json.RawMessage(`{ "url" : "http://www.grafana.com" }`),
						},
					},
				},
			},
		},
	}

	res, err := c.TestReceiversExperimental(context.Background(), trConfig)
	require.NoError(t, err)

	// Default annotations from Test Alert
	// "summary":          "Notification test",
	// "__value_string__": "[ metric='foo' labels={instance=bar} value=10 ]"
	expectedAnnotations := model.LabelSet{
		"annotation":       "test annotation",
		"summary":          "Notification test",
		"__value_string__": "[ metric='foo' labels={instance=bar} value=10 ]",
	}
	require.Equal(t, expectedAnnotations, res.Alert.Annotations)

	// Default labels from Test Alert
	// "alertname": "TestAlert"
	// "instance":  "Grafana"
	expectedLabels := model.LabelSet{
		"label":     "test label",
		"alertname": "TestAlert",
		"instance":  "Grafana",
	}
	require.Equal(t, expectedLabels, res.Alert.Labels)

	require.Len(t, res.Receivers, 1)
	require.Len(t, res.Receivers[0].Configs, 1)
	require.Equal(t, trConfig.Receivers[0].GrafanaIntegrations.Integrations[0].UID, res.Receivers[0].Configs[0].UID)
	require.Equal(t, trConfig.Receivers[0].GrafanaIntegrations.Integrations[0].Name, res.Receivers[0].Configs[0].Name)
	require.Equal(t, "", res.Receivers[0].Configs[0].Error)
}
