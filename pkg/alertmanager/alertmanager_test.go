// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertmanager_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/alerting/definition"
	alertingmodels "github.com/grafana/alerting/models"
	alertingTemplates "github.com/grafana/alerting/templates"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDispatcherGroupLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		groups           int
		groupsLimit      int
		expectedFailures int
	}{
		"no limit":   {groups: 5, groupsLimit: 0, expectedFailures: 0},
		"high limit": {groups: 5, groupsLimit: 10, expectedFailures: 0},
		"low limit":  {groups: 5, groupsLimit: 3, expectedFailures: 4}, // 2 groups that fail, 2 alerts per group = 4 failures
	} {
		t.Run(name, func(t *testing.T) {
			createAlertmanagerAndSendAlerts(t, tc.groups, tc.groupsLimit, tc.expectedFailures)
		})
	}
}

type stubReplicator struct{}

func (*stubReplicator) ReplicateStateForUser(context.Context, string, *clusterpb.Part) error {
	return nil
}

func (*stubReplicator) GetPositionForUser(string) int {
	return 0
}

func (*stubReplicator) ReadFullStateForUser(context.Context, string) ([]*clusterpb.FullState, error) {
	return nil, nil
}

func createAlertmanagerAndSendAlerts(t *testing.T, alertGroups, groupsLimit, expectedFailures int) {
	user := "test"

	reg := prometheus.NewPedanticRegistry()
	am, err := New(&Config{
		UserID:            user,
		Logger:            log.NewNopLogger(),
		Limits:            &mockAlertManagerLimits{maxDispatcherAggregationGroups: groupsLimit},
		Features:          featurecontrol.NoopFlags{},
		TenantDataDir:     t.TempDir(),
		ExternalURL:       &url.URL{Path: "/am"},
		ShardingEnabled:   true,
		Store:             prepareInMemoryAlertStore(),
		Replicator:        &stubReplicator{},
		ReplicationFactor: 1,
		// We have to set this interval non-zero, though we don't need the persister to do anything.
		PersisterConfig: PersisterConfig{Interval: time.Hour},
	}, reg)
	require.NoError(t, err)
	defer am.StopAndWait()

	cfgRaw := `receivers:
- name: 'prod'

route:
  group_by: ['alertname']
  group_wait: 10ms
  group_interval: 10ms
  receiver: 'prod'`

	cfg, err := definition.LoadCompat([]byte(cfgRaw))
	require.NoError(t, err)
	tmpls := make([]alertingTemplates.TemplateDefinition, 0)
	require.NoError(t, am.ApplyConfig(cfg, tmpls, cfgRaw, &url.URL{}, nil))

	now := time.Now()

	for i := 0; i < alertGroups; i++ {
		alertName := model.LabelValue(fmt.Sprintf("Alert-%d", i))

		inputAlerts := []*types.Alert{
			{
				Alert: model.Alert{
					Labels: model.LabelSet{
						"alertname": alertName,
						"a":         "b",
					},
					Annotations:  model.LabelSet{"foo": "bar"},
					StartsAt:     now,
					EndsAt:       now.Add(5 * time.Minute),
					GeneratorURL: "http://example.com/prometheus",
				},
				UpdatedAt: now,
				Timeout:   false,
			},

			{
				Alert: model.Alert{
					Labels: model.LabelSet{
						"alertname": alertName,
						"z":         "y",
					},
					Annotations:  model.LabelSet{"foo": "bar"},
					StartsAt:     now,
					EndsAt:       now.Add(5 * time.Minute),
					GeneratorURL: "http://example.com/prometheus",
				},
				UpdatedAt: now,
				Timeout:   false,
			},
		}
		require.NoError(t, am.alerts.Put(inputAlerts...))
	}

	// Give it some time, as alerts are sent to dispatcher asynchronously.
	test.Poll(t, 3*time.Second, nil, func() interface{} {
		return testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP alertmanager_dispatcher_aggregation_group_limit_reached_total Number of times when dispatcher failed to create new aggregation group due to limit.
		# TYPE alertmanager_dispatcher_aggregation_group_limit_reached_total counter
		alertmanager_dispatcher_aggregation_group_limit_reached_total %d
	`, expectedFailures)), "alertmanager_dispatcher_aggregation_group_limit_reached_total")
	})
}

func TestDispatcherLoggerInsightKey(t *testing.T) {
	var buf concurrency.SyncBuffer
	logger := log.NewLogfmtLogger(&buf)

	user := "test"
	reg := prometheus.NewPedanticRegistry()
	am, err := New(&Config{
		UserID:            user,
		Logger:            logger,
		Limits:            &mockAlertManagerLimits{maxDispatcherAggregationGroups: 10},
		Features:          featurecontrol.NoopFlags{},
		TenantDataDir:     t.TempDir(),
		ExternalURL:       &url.URL{Path: "/am"},
		ShardingEnabled:   true,
		Store:             prepareInMemoryAlertStore(),
		Replicator:        &stubReplicator{},
		ReplicationFactor: 1,
		PersisterConfig:   PersisterConfig{Interval: time.Hour},
	}, reg)
	require.NoError(t, err)
	defer am.StopAndWait()

	cfgRaw := `receivers:
- name: 'prod'

route:
  group_by: ['alertname']
  group_wait: 10ms
  group_interval: 10ms
  receiver: 'prod'`

	cfg, err := definition.LoadCompat([]byte(cfgRaw))
	require.NoError(t, err)
	tmpls := make([]alertingTemplates.TemplateDefinition, 0)
	require.NoError(t, am.ApplyConfig(cfg, tmpls, cfgRaw, &url.URL{}, nil))

	now := time.Now()
	inputAlerts := []*types.Alert{
		{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"alertname": model.LabelValue("Alert-1"),
					"a":         "b",
				},
				Annotations:  model.LabelSet{"foo": "bar"},
				StartsAt:     now,
				EndsAt:       now.Add(5 * time.Minute),
				GeneratorURL: "http://example.com/prometheus",
			},
			UpdatedAt: now,
			Timeout:   false,
		},
	}
	require.NoError(t, am.alerts.Put(inputAlerts...))

	test.Poll(t, 3*time.Second, true, func() interface{} {
		logs := buf.String()
		return strings.Contains(logs, "insight=true")
		// Ensure that the dispatcher component emits logs with a "true" insight key,
		// identifying these logs to be exposed to end users via the usage insights system.
	})
}

var (
	alert1 = model.Alert{
		Labels:       model.LabelSet{"alert": "first"},
		Annotations:  model.LabelSet{"job": "test"},
		StartsAt:     time.Now(),
		EndsAt:       time.Now(),
		GeneratorURL: "some URL",
	}
	alert1Size = alertSize(alert1)

	alert2 = model.Alert{
		Labels:       model.LabelSet{"alert": "second"},
		Annotations:  model.LabelSet{"job": "test", "cluster": "prod"},
		StartsAt:     time.Now(),
		EndsAt:       time.Now(),
		GeneratorURL: "some URL",
	}
	alert2Size = alertSize(alert2)
)

type callbackOp struct {
	alert               *types.Alert
	existing            bool
	delete              bool // true=delete, false=insert.
	expectedInsertError error

	// expected values after operation.
	expectedCount     int
	expectedTotalSize int
}

func TestAlertsLimiterWithNoLimits(t *testing.T) {
	ops := []callbackOp{
		{alert: &types.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 2, expectedTotalSize: alert1Size + alert2Size},
		{alert: &types.Alert{Alert: alert2}, delete: true, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	testLimiter(t, &mockAlertManagerLimits{}, ops)
}

func TestAlertsLimiterWithCountLimit(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}
	alert2WithMoreAnnotationsSize := alertSize(alert2WithMoreAnnotations)

	ops := []callbackOp{
		{alert: &types.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedInsertError: fmt.Errorf(errTooManyAlerts, 1), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},

		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		// Update of existing alert works -- doesn't change count.
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedCount: 1, expectedTotalSize: alert2WithMoreAnnotationsSize},
		{alert: &types.Alert{Alert: alert2}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	testLimiter(t, &mockAlertManagerLimits{maxAlertsCount: 1}, ops)
}

func TestAlertsLimiterWithSizeLimit(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}

	ops := []callbackOp{
		{alert: &types.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: false, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &types.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},

		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &types.Alert{Alert: alert2}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	// Prerequisite for this test. We set size limit to alert2Size, but inserting alert1 first will prevent insertion of alert2.
	require.True(t, alert2Size > alert1Size)

	testLimiter(t, &mockAlertManagerLimits{maxAlertsSizeBytes: alert2Size}, ops)
}

func TestAlertsLimiterWithSizeLimitAndAnnotationUpdate(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}
	alert2WithMoreAnnotationsSize := alertSize(alert2WithMoreAnnotations)

	// Updating alert with larger annotation that goes over the size limit fails.
	testLimiter(t, &mockAlertManagerLimits{maxAlertsSizeBytes: alert2Size}, []callbackOp{
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert2Size},
	})

	// Updating alert with larger annotations in the limit works fine.
	testLimiter(t, &mockAlertManagerLimits{maxAlertsSizeBytes: alert2WithMoreAnnotationsSize}, []callbackOp{
		{alert: &types.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &types.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedCount: 1, expectedTotalSize: alert2WithMoreAnnotationsSize},
		{alert: &types.Alert{Alert: alert2}, existing: true, expectedCount: 1, expectedTotalSize: alert2Size},
	})
}

// testLimiter sends sequence of alerts to limiter, and checks if limiter updated reacted correctly.
func testLimiter(t *testing.T, limits Limits, ops []callbackOp) {
	reg := prometheus.NewPedanticRegistry()

	limiter := newAlertsLimiter("test", limits, reg)

	for ix, op := range ops {
		if op.delete {
			limiter.PostDelete(op.alert)
		} else {
			err := limiter.PreStore(op.alert, op.existing)
			require.Equal(t, op.expectedInsertError, err, "op %d", ix)
			if err == nil {
				limiter.PostStore(op.alert, op.existing)
			}
		}

		count, totalSize := limiter.currentStats()

		assert.Equal(t, op.expectedCount, count, "wrong count, op %d", ix)
		assert.Equal(t, op.expectedTotalSize, totalSize, "wrong total size, op %d", ix)
	}
}

// cloneSilence returns a shallow copy of a silence. It is used in tests.
func cloneSilence(t *testing.T, sil *silencepb.Silence) *silencepb.Silence {
	t.Helper()
	s := *sil
	return &s
}

func toMeshSilence(t *testing.T, sil *silencepb.Silence, retention time.Duration) *silencepb.MeshSilence {
	t.Helper()
	return &silencepb.MeshSilence{
		Silence:   sil,
		ExpiresAt: sil.EndsAt.Add(retention),
	}
}

func TestSilenceLimits(t *testing.T) {
	user := "test"

	r := prometheus.NewPedanticRegistry()
	limits := mockAlertManagerLimits{
		maxSilencesCount:    1,
		maxSilenceSizeBytes: 2 << 11, // 4KB,
	}
	am, err := New(&Config{
		UserID:            user,
		Logger:            log.NewNopLogger(),
		Limits:            &limits,
		Features:          featurecontrol.NoopFlags{},
		TenantDataDir:     t.TempDir(),
		ExternalURL:       &url.URL{Path: "/am"},
		ShardingEnabled:   true,
		Store:             prepareInMemoryAlertStore(),
		Replicator:        &stubReplicator{},
		ReplicationFactor: 1,
		// We have set this to 1 hour, but we don't use it in this
		// test as we override the broadcast function with SetBroadcast.
		PersisterConfig: PersisterConfig{Interval: time.Hour},
	}, r)
	require.NoError(t, err)
	defer am.StopAndWait()

	// Override SetBroadcast as we just want to test limits.
	am.silences.SetBroadcast(func(_ []byte) {})

	// Insert sil1 should succeed without error.
	sil1 := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{{Name: "a", Pattern: "b"}},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(5 * time.Minute),
	}
	require.NoError(t, am.silences.Set(sil1))

	// Insert sil2 should fail because maximum number of silences has been
	// exceeded.
	sil2 := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{{Name: "c", Pattern: "d"}},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(5 * time.Minute),
	}
	require.EqualError(t, am.silences.Set(sil2), "exceeded maximum number of silences: 1 (limit: 1)")

	// Expire sil1 and run the GC. This should allow sil2 to be inserted.
	require.NoError(t, am.silences.Expire(sil1.Id))
	n, err := am.silences.GC()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, am.silences.Set(sil2))

	// Expire sil2 and run the GC.
	require.NoError(t, am.silences.Expire(sil2.Id))
	n, err = am.silences.GC()
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Insert sil3 should fail because it exceeds maximum size.
	sil3 := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{
			{
				Name:    strings.Repeat("e", 2<<9),
				Pattern: strings.Repeat("f", 2<<9),
			},
			{
				Name:    strings.Repeat("g", 2<<9),
				Pattern: strings.Repeat("h", 2<<9),
			},
		},
		CreatedBy: strings.Repeat("i", 2<<9),
		Comment:   strings.Repeat("j", 2<<9),
		StartsAt:  time.Now(),
		EndsAt:    time.Now().Add(5 * time.Minute),
	}
	require.EqualError(t, am.silences.Set(sil3), fmt.Sprintf("silence exceeded maximum size: %d bytes (limit: 4096 bytes)", toMeshSilence(t, sil3, 0).Size()))

	// Should be able to insert sil4.
	sil4 := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{{Name: "k", Pattern: "l"}},
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(5 * time.Minute),
	}
	require.NoError(t, am.silences.Set(sil4))

	// Should be able to update sil4 without modifications. It is expected to
	// keep the same ID.
	sil5 := cloneSilence(t, sil4)
	require.NoError(t, am.silences.Set(sil5))
	require.Equal(t, sil4.Id, sil5.Id)

	// Should be able to update the comment. It is also expected to keep the
	// same ID.
	sil6 := cloneSilence(t, sil5)
	sil6.Comment = "m"
	require.NoError(t, am.silences.Set(sil6))
	require.Equal(t, sil5.Id, sil6.Id)

	// Should not be able to update the start and end time as this requires
	// sil6 to be expired and a new silence to be created. However, this would
	// exceed the maximum number of silences, which counts both active and
	// expired silences.
	sil7 := cloneSilence(t, sil6)
	sil7.StartsAt = time.Now().Add(5 * time.Minute)
	sil7.EndsAt = time.Now().Add(10 * time.Minute)
	require.EqualError(t, am.silences.Set(sil7), "exceeded maximum number of silences: 1 (limit: 1)")

	// sil6 should not be expired because the update failed.
	sils, _, err := am.silences.Query(silence.QState(types.SilenceStateExpired))
	require.NoError(t, err)
	require.Len(t, sils, 0)

	// Should not be able to update with a comment that exceeds maximum size.
	// Need to increase the maximum number of silences to test this.
	limits.maxSilencesCount = 2
	sil8 := cloneSilence(t, sil6)
	sil8.Comment = strings.Repeat("m", 2<<11)
	require.EqualError(t, am.silences.Set(sil8), fmt.Sprintf("silence exceeded maximum size: %d bytes (limit: 4096 bytes)", toMeshSilence(t, sil8, 0).Size()))

	// sil6 should not be expired because the update failed.
	sils, _, err = am.silences.Query(silence.QState(types.SilenceStateExpired))
	require.NoError(t, err)
	require.Len(t, sils, 0)

	// Should not be able to replace with a silence that exceeds maximum size.
	// This is different from the previous assertion as unlike when adding or
	// updating a comment, changing the matchers for a silence should expire
	// the existing silence, unless the silence that is replacing it exceeds
	// limits, in which case the operation should fail and the existing silence
	// should still be active.
	sil9 := cloneSilence(t, sil8)
	sil9.Matchers = []*silencepb.Matcher{{Name: "n", Pattern: "o"}}
	require.EqualError(t, am.silences.Set(sil9), fmt.Sprintf("silence exceeded maximum size: %d bytes (limit: 4096 bytes)", toMeshSilence(t, sil9, 0).Size()))

	// sil6 should not be expired because the update failed.
	sils, _, err = am.silences.Query(silence.QState(types.SilenceStateExpired))
	require.NoError(t, err)
	require.Len(t, sils, 0)
}

func TestExperimentalReceiversAPI(t *testing.T) {
	var buf concurrency.SyncBuffer
	logger := log.NewLogfmtLogger(&buf)

	user := "test"
	reg := prometheus.NewPedanticRegistry()
	am, err := New(&Config{
		UserID:            user,
		Logger:            logger,
		Limits:            &mockAlertManagerLimits{maxDispatcherAggregationGroups: 10},
		Features:          featurecontrol.NoopFlags{},
		TenantDataDir:     t.TempDir(),
		ExternalURL:       &url.URL{Path: "/am"},
		ShardingEnabled:   true,
		Store:             prepareInMemoryAlertStore(),
		Replicator:        &stubReplicator{},
		ReplicationFactor: 1,
		PersisterConfig:   PersisterConfig{Interval: time.Hour},
	}, reg)
	require.NoError(t, err)
	defer am.StopAndWait()

	cfgRaw := `receivers:
- name: 'recv-1'
  webhook_configs:
  - url: http://example.com/
    send_resolved: true

- name: 'recv-2'
  webhook_configs:
  - url: http://example.com/
    send_resolved: false

route:
  group_by: ['alertname']
  group_wait: 10ms
  group_interval: 10ms
  receiver: 'recv-1'`

	cfg, err := definition.LoadCompat([]byte(cfgRaw))
	require.NoError(t, err)
	tmpls := make([]alertingTemplates.TemplateDefinition, 0)
	require.NoError(t, am.ApplyConfig(cfg, tmpls, cfgRaw, &url.URL{}, nil))

	doGetReceivers := func() []alertingmodels.Receiver {
		rr := httptest.NewRecorder()
		am.GetReceiversHandler(rr, nil)
		require.Equal(t, http.StatusOK, rr.Code)
		result := []alertingmodels.Receiver{}
		err = json.Unmarshal(rr.Body.Bytes(), &result)
		assert.NoError(t, err)
		sort.Slice(result, func(i, j int) bool {
			return result[i].Name < result[j].Name
		})
		return result
	}

	// Check the API returns all receivers but without any notification status.

	result := doGetReceivers()
	assert.Equal(t, []alertingmodels.Receiver{
		{
			Name:   "recv-1",
			Active: true,
			Integrations: []alertingmodels.Integration{
				{
					Name:                      "webhook",
					LastNotifyAttemptDuration: "0s",
					SendResolved:              true,
				},
			},
		},
		{
			Name: "recv-2",
			// Receiver not used in a route.
			Active: false,
			Integrations: []alertingmodels.Integration{
				{
					Name:                      "webhook",
					LastNotifyAttemptDuration: "0s",
					// We configure send_resolved to false.
					SendResolved: false,
				},
			},
		},
	}, result)

	// Send an alert to cause a notification attempt.

	now := time.Now()
	inputAlerts := []*types.Alert{
		{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"alertname": model.LabelValue("Alert-1"),
					"a":         "b",
				},
				Annotations:  model.LabelSet{"templates": `{{ template "test" . }}`},
				StartsAt:     now,
				EndsAt:       now.Add(5 * time.Minute),
				GeneratorURL: "http://example.com/prometheus",
			},
			UpdatedAt: now,
			Timeout:   false,
		},
	}
	require.NoError(t, am.alerts.Put(inputAlerts...))

	// Wait for the API to tell us there was a notification attempt.

	result = []alertingmodels.Receiver{}
	require.Eventually(t, func() bool {
		result = doGetReceivers()
		return len(result) == 2 &&
			len(result[0].Integrations) == 1 &&
			len(result[1].Integrations) == 1 &&
			!result[0].Integrations[0].LastNotifyAttempt.IsZero()
	}, 5*time.Second, 100*time.Millisecond)

	assert.Equal(t, "webhook", result[0].Integrations[0].Name)
	assert.NotZero(t, result[0].Integrations[0].LastNotifyAttempt)
	assert.Equal(t, errRateLimited.Error(), result[0].Integrations[0].LastNotifyAttemptError)

	// Check the status of the other integration is not changed.

	assert.Equal(t, "webhook", result[1].Integrations[0].Name)
	assert.Zero(t, result[1].Integrations[0].LastNotifyAttempt)
	assert.Equal(t, "", result[1].Integrations[0].LastNotifyAttemptError)
}

func TestGrafanaAlertmanagerTemplates(t *testing.T) {
	am, err := New(&Config{
		UserID:            "test",
		Logger:            log.NewNopLogger(),
		Limits:            &mockAlertManagerLimits{},
		Features:          featurecontrol.NoopFlags{},
		TenantDataDir:     t.TempDir(),
		ExternalURL:       &url.URL{Path: "/am"},
		ShardingEnabled:   true,
		Store:             prepareInMemoryAlertStore(),
		Replicator:        &stubReplicator{},
		ReplicationFactor: 1,
		// We have to set this interval non-zero, though we don't need the persister to do anything.
		PersisterConfig: PersisterConfig{Interval: time.Hour},
	}, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	defer am.StopAndWait()

	// The webhook message should contain the executed Grafana template.
	var got struct {
		Message string `json:"message"`
	}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&got)
		defer func() {
			require.NoError(t, r.Body.Close())
		}()
	}))
	defer s.Close()

	cfgRaw := fmt.Sprintf(`{
            "route": {
                "receiver": "test_receiver",
                "group_by": ["alertname"]
            },
            "receivers": [{
                "name": "test_receiver",
                "grafana_managed_receiver_configs": [{
                    "uid": "",
                    "name": "webhook test",
                    "type": "webhook",
                    "disableResolveMessage": true,
                    "settings": {
                        "url": %q,
                        "message": %q
                    },
                }]
            }],
        }`, s.URL, `{{ template "test" . }}`)

	cfg, err := definition.LoadCompat([]byte(cfgRaw))
	require.NoError(t, err)
	expMessage := "This is a test template"
	testTemplate := alertingTemplates.TemplateDefinition{
		Name:     "test",
		Template: fmt.Sprintf(`{{ define "test" -}} %s {{- end }}`, expMessage),
	}
	require.NoError(t, am.ApplyConfig(cfg, []alertingTemplates.TemplateDefinition{testTemplate}, cfgRaw, &url.URL{}, nil))

	now := time.Now()
	alert := types.Alert{
		Alert: model.Alert{
			Labels: model.LabelSet{
				"alertname": model.LabelValue("test-alert"),
			},
			StartsAt: now.Add(-5 * time.Minute),
			EndsAt:   now.Add(5 * time.Minute),
		},
		UpdatedAt: now,
	}
	require.NoError(t, am.alerts.Put(&alert))
	require.Equal(t, am.templates[0], testTemplate)
	require.Eventually(t, func() bool {
		return got.Message == expMessage
	}, 5*time.Second, 100*time.Millisecond)
}
