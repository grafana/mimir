// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/grafana/alerting/definition"
	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/grafana/mimir/integration/e2emimir"
	"github.com/grafana/mimir/pkg/alertmanager"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestGrafanaAlertmanagerWithNestedRoutes(t *testing.T) {
	s, err := newGrafanaAMScenario(t, grafanaAlertmanagerUserConfig, 3, 3)
	require.NoError(t, err)
	defer s.close()

	s.runAMTest()
}

type grafanaAMScenario struct {
	*e2e.Scenario
	t *testing.T

	config alertmanager.GrafanaAlertmanagerConfig
	ms     *e2e.HTTPService
	ams    []e2e.Service
	amCs   []*e2emimir.Client
	whs    []e2e.Service
	whCs   []*WebhookClient
}

func newGrafanaAMScenario(t *testing.T, defaultConfig string, nAMs, nWebhooks int) (*grafanaAMScenario, error) {
	e2es, err := e2e.NewScenario(networkName)
	require.NoError(t, err)

	s := grafanaAMScenario{Scenario: e2es, t: t}
	t.Logf("Shared dir: %s", s.SharedDir())

	require.NoError(t, copyFileToSharedDir(s.Scenario, "development/grafana-alertmanager/config/alertmanager.yaml", "alertmanager.yaml"))

	var grafanaConfig alertmanager.GrafanaAlertmanagerConfig
	require.NoError(t, json.Unmarshal([]byte(defaultConfig), &grafanaConfig))
	s.config = grafanaConfig

	minio := e2edb.NewMinio(9000, grafanaAlertmanagerBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))
	s.ms = minio

	s.setupWHs(nWebhooks)
	require.NoError(t, copyFileToSharedDir(s.Scenario, "development/grafana-alertmanager/config/alertmanager-fallback-config.yaml", alertmanagerFallbackConfigFile))
	s.setupAMs(nAMs)

	return &s, nil
}

func (s *grafanaAMScenario) setupWHs(n int) {
	for i := range n {
		wh := NewWebhookService(fmt.Sprintf("wh-%d", i), map[string]string{
			"-results-output-path": e2e.ContainerSharedDir,
		}, nil)
		s.whs = append(s.whs, wh)
	}
	require.NoError(s.t, s.StartAndWaitReady(s.whs...))

	// same as for AM clients - we need to initialize them after the services are started, so that we know the actual port
	for _, service := range s.whs {
		wh, _ := service.(*WebhookService)
		whC, err := NewWebhookClient("http://" + wh.HTTPEndpoint())
		require.NoError(s.t, err)

		s.whCs = append(s.whCs, whC)
	}
}

func (s *grafanaAMScenario) setupAMs(n int) {
	flags := mergeFlags(
		AlertmanagerMemberlistFlags(networkName+"-am-0:7946"),
		AlertmanagerS3Flags(),
		AlertmanagerGrafanaCompatibilityFlags(),
		map[string]string{
			// Override alerts bucket
			"-alertmanager-storage.s3.bucket-name": grafanaAlertmanagerBucketName,
			"-log.level":                           "debug",
		},
	)

	// Start Mimir
	for i := range n {
		am := e2emimir.NewAlertmanager(fmt.Sprintf("am-%d", i), flags, e2emimir.WithConfigFile("alertmanager.yaml"))
		s.ams = append(s.ams, am)
	}
	require.NoError(s.t, s.StartAndWaitReady(s.ams...))

	// we need to initialize the clients after the services are started, so that we know the actual port
	for _, service := range s.ams {
		am, _ := service.(*e2emimir.MimirService)
		c, err := e2emimir.NewClient("", "", am.HTTPEndpoint(), "", "user-1")
		require.NoError(s.t, err)
		s.amCs = append(s.amCs, c)
	}
}

func (s *grafanaAMScenario) addMockReceiver() {
	// NOTE: the only reason to do this is to change something in the config to trigger update
	// it might make more sense to change something simpler and not parsing the whole config
	s.config.AlertmanagerConfig.Receivers = append(
		s.config.AlertmanagerConfig.Receivers,
		&definition.PostableApiReceiver{
			PostableGrafanaReceivers: definition.PostableGrafanaReceivers{
				GrafanaManagedReceivers: []*definition.PostableGrafanaReceiver{
					{
						Name:                  "mock-webhook",
						UID:                   "mock-webhook",
						Type:                  "webhook",
						DisableResolveMessage: true,
						Settings: definition.RawMessage([]byte(`{
							"url": "http://invalid.url"
						}`)),
					},
				},
			},
		},
	)
}

func (s *grafanaAMScenario) removeMockReceiver() {
	// NOTE: same as `addMockReceiver`
	rs := make([]*definition.PostableApiReceiver, 0, len(s.config.AlertmanagerConfig.Receivers)-1)
	for _, r := range s.config.AlertmanagerConfig.Receivers {
		if rcv := r.PostableGrafanaReceivers.GrafanaManagedReceivers[0]; rcv != nil && rcv.Name == "mock-webhook" {
			continue
		}
		rs = append(rs, r)
	}

	s.config.AlertmanagerConfig.Receivers = rs
}

func (s *grafanaAMScenario) setCurrentConfig() {
	cfg, err := json.Marshal(s.config)
	require.NoError(s.t, err)

	c := s.amCs[0]
	createdAt := time.Now().Unix()

	s.logf("Setting Grafana Alertmanager config with ts %d...", createdAt)
	require.NoError(s.t, c.SetGrafanaAlertmanagerConfig(s.t.Context(), createdAt, string(cfg), "test", "http://localhost:8080", false, true, nil, nil))
}

func (s *grafanaAMScenario) logf(msg string, args ...any) {
	ts := time.Now().Format(time.RFC3339)
	s.t.Logf("%s: "+msg, append([]any{ts}, args...)...)
}

func (s *grafanaAMScenario) dumpFullState(id string) {
	c := s.amCs[0]

	st, err := c.GetFullState(s.t.Context())
	require.NoError(s.t, err)

	d, err := json.MarshalIndent(st, "", "  ")
	require.NoError(s.t, err)
	dumpFile := fmt.Sprintf("state_%s.json", id)
	s.logf("Dumping full Alertmanager state to %q... state size: %d", dumpFile, len(d))

	dst := filepath.Join(s.SharedDir(), dumpFile)
	require.NoError(s.t, os.WriteFile(dst, d, fs.ModePerm))
}

func (s *grafanaAMScenario) sendAlerts(alerts ...*model.Alert) {
	c := s.amCs[0]

	for _, a := range alerts {
		require.NoError(s.t, c.SendAlertToAlermanager(
			s.t.Context(),
			a,
		))
	}
}

func (s *grafanaAMScenario) waitForNotificationsParallel(areExpected ...func(res *GetNotificationsResponse) bool) {
	if len(areExpected) != len(s.whCs) {
		s.t.Fatalf("areExpected functions must match the number of webhook clients (%d != %d)", len(areExpected), len(s.whCs))
	}

	var wg sync.WaitGroup

	for i, c := range s.whCs {
		wg.Add(1)
		go func(i int, c *WebhookClient) {
			defer wg.Done()
			require.NoError(s.t, c.WaitForNotifications(areExpected[i]))
		}(i, c)
	}

	wg.Wait()
}

func (s *grafanaAMScenario) waitForNotifications(areExpected ...func(res *GetNotificationsResponse) bool) {
	if len(areExpected) != len(s.whCs) {
		s.t.Fatalf("areExpected functions must match the number of webhook clients (%d != %d)", len(areExpected), len(s.whCs))
	}

	for i, c := range s.whCs {
		require.NoError(s.t, c.WaitForNotifications(areExpected[i]))
	}
}

func (s *grafanaAMScenario) waitForNotificationsAll(isExpected func(res *GetNotificationsResponse) bool) {
	expected := make([]func(res *GetNotificationsResponse) bool, len(s.whCs))
	for i := range expected {
		expected[i] = isExpected
	}
	s.waitForNotifications(expected...)
}

func (s *grafanaAMScenario) waitForNotificationsAllParallel(isExpected func(res *GetNotificationsResponse) bool) {
	expected := make([]func(res *GetNotificationsResponse) bool, len(s.whCs))
	for i := range expected {
		expected[i] = isExpected
	}

	s.waitForNotificationsParallel(expected...)
}

func (s *grafanaAMScenario) close() {
	// stop to trigger dump
	for _, wh := range s.whs {
		require.NoError(s.t, wh.Stop())
	}

	s.dumpFullState("4_close") // dump AM state before stopping
	for _, am := range s.ams {
		require.NoError(s.t, am.Stop())
	}
	// NOTE: On close, the shared directory gets cleaned up
	// comment the line below to inspect the dumps
	s.Scenario.Close()
}

func (s *grafanaAMScenario) runAMTest() {
	// Set user config
	s.setCurrentConfig()

	var (
		// alert timestamps
		startsAt = time.Now().Add(-24 * time.Hour)
		endsAt   = startsAt.Add(30 * time.Second)
	)

	// 1. send first alert - all receivers should get it
	s.t.Log("Sending first alert...")
	s.sendAlerts(
		testAlertWithOpts(withStartsAt(startsAt)),
	)

	expectOneFire := func(res *GetNotificationsResponse) bool {
		return res.Stats["firing"] == 1
	}

	// wait for all receivers to get the notification
	s.waitForNotificationsAll(expectOneFire)

	s.dumpFullState("1_after_first_alert")

	// simulating prod timeline
	time.Sleep(2 * time.Second)

	// 2. send resolved alert - only wh1 should get it
	// the only reason wh-0 gets a notification is because we trigger a config update right after sending the alert
	// this restarts the dispatcher, setting the `hasFlushed` flag to false. The dedup stage works for repeated notifications
	// but if there was a state change it will trigger a notification
	s.t.Log("Resolving alert...")
	s.sendAlerts(
		testAlertWithOpts(withStartsAt(startsAt), withEndsAt(endsAt)),
	)

	time.Sleep(5 * time.Second)

	// 2.1 trigger config update
	s.addMockReceiver()
	s.setCurrentConfig()

	expectNoResolved := func(res *GetNotificationsResponse) bool {
		r, ok := res.Stats["resolved"]
		return !ok || r == 0
	}

	// 2.2. check if we got resolved
	s.waitForNotifications(
		func(res *GetNotificationsResponse) bool {
			return res.Stats["resolved"] == 1
		},
		expectNoResolved,
		expectNoResolved,
	)
	s.dumpFullState("2_after_resolve")

	// 2.3. wait
	time.Sleep(5 * time.Second)

	// 3. send 3 alerts at once, each with a different "id" label.
	s.t.Log("Sending 3 alerts at once...")

	startsAt = endsAt.Add(2 * time.Minute)
	as := make([]*model.Alert, 3)
	for i := range as {
		as[i] = testAlertWithOpts(
			withStartsAt(startsAt),
			withIDLabel(fmt.Sprint(2+i)),
		)
	}

	s.sendAlerts(as...)

	// NOTE: leaving it here for context
	// Testing the reset to make sure they're the cause for notifications before group interval
	// if no reset is done, no notifications should be sent
	// uncommenting that triggers notifications on wh-1 and wh-2
	// time.Sleep(1 * time.Second)
	// s.removeMockReceiver()
	// s.setCurrentConfig()

	s.waitForNotificationsParallel(func(res *GetNotificationsResponse) bool {
		return res.Stats["firing"] == 4
	}, expectOneFire, expectOneFire)

	s.dumpFullState("3_after_2nd_fire")
}

func withEndsAt(endsAt time.Time) func(*model.Alert) {
	return func(a *model.Alert) {
		a.EndsAt = endsAt
	}
}

func withStartsAt(startsAt time.Time) func(*model.Alert) {
	return func(a *model.Alert) {
		a.StartsAt = startsAt
	}
}

func withIDLabel(id string) func(*model.Alert) {
	return func(a *model.Alert) {
		a.Labels["id"] = model.LabelValue(id)
	}
}

func testAlertWithOpts(opts ...func(*model.Alert)) *model.Alert {
	a := &model.Alert{
		Labels: model.LabelSet{
			"grafana_folder": "integration",
			"alertname":      "test",
			"id":             "1",
			"nest":           "true",
			"further":        "true",
		},
		StartsAt: time.Now(),
	}

	for _, o := range opts {
		o(a)
	}

	return a
}
