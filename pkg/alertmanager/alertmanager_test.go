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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/alertmanager/alert"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/featurecontrol"
	"github.com/prometheus/alertmanager/silence"
	"github.com/prometheus/alertmanager/silence/silencepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	googleproto "google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	utiltest "github.com/grafana/mimir/pkg/util/test"
)

func TestDispatcherGroupLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		groups        int
		groupsLimit   int
		expectFailure bool
	}{
		"no limit":   {groups: 5, groupsLimit: 0, expectFailure: false},
		"high limit": {groups: 5, groupsLimit: 10, expectFailure: false},
		// 5 groups vs. limit of 3 — at least some alerts should hit the limit.
		// The exact count is non-deterministic because v0.32.0 ingests alerts
		// concurrently and the limit check is racy upstream.
		"low limit": {groups: 5, groupsLimit: 3, expectFailure: true},
	} {
		t.Run(name, func(t *testing.T) {
			createAlertmanagerAndSendAlerts(t, tc.groups, tc.groupsLimit, tc.expectFailure)
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

func createAlertmanagerAndSendAlerts(t *testing.T, alertGroups, groupsLimit int, expectFailure bool) {
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

	cfg, err := config.Load(cfgRaw)
	require.NoError(t, err)
	tmpls := make([]*alertspb.TemplateDesc, 0)
	require.NoError(t, am.ApplyConfig(cfg, tmpls, cfgRaw))

	now := time.Now()

	for i := 0; i < alertGroups; i++ {
		alertName := model.LabelValue(fmt.Sprintf("Alert-%d", i))

		inputAlerts := []*alert.Alert{
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
		require.NoError(t, am.alerts.Put(context.Background(), inputAlerts...))
	}

	// Give it some time, as alerts are sent to dispatcher asynchronously.
	// Note: the dispatcher's group-limit check is a racy check-then-act under concurrent
	// alert ingestion (worker goroutines spawn in d.run after WaitForLoading completes),
	// so the exact counter value isn't deterministic. We assert the qualitative outcome
	// instead: no failures when no limit is exceeded, at least one failure otherwise.
	test.Poll(t, 3*time.Second, true, func() interface{} {
		got := readCounter(t, reg, "alertmanager_dispatcher_aggregation_group_limit_reached_total")
		if !expectFailure {
			return got == 0
		}
		return got >= 1
	})
}

// readCounter returns the current value of the named unlabeled counter metric in reg.
// It fails the test if the metric is not registered, has more than one sample (i.e.
// has labels — this helper can't disambiguate), or is not a counter. Failing loud on
// "not registered" guards against silent zeros caused by a typo in the metric name.
func readCounter(t *testing.T, reg prometheus.Gatherer, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.Len(t, mf.GetMetric(), 1, "readCounter expects exactly one sample for %q", name)
		c := mf.GetMetric()[0].GetCounter()
		require.NotNil(t, c, "metric %q is not a counter", name)
		return c.GetValue()
	}
	require.Failf(t, "metric not found", "metric %q is not registered", name)
	return 0
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

	cfg, err := config.Load(cfgRaw)
	require.NoError(t, err)
	tmpls := make([]*alertspb.TemplateDesc, 0)
	require.NoError(t, am.ApplyConfig(cfg, tmpls, cfgRaw))

	now := time.Now()
	inputAlerts := []*alert.Alert{
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
	require.NoError(t, am.alerts.Put(context.Background(), inputAlerts...))

	test.Poll(t, 3*time.Second, true, func() interface{} {
		logs := buf.String()
		return strings.Contains(logs, "insight=true")
		// Ensure that the dispatcher component emits logs with a "true" insight key,
		// identifying these logs to be exposed to end users via the usage insights system.
	})
}

// TestApplyConfigStopRace_NoInhibitorLeak guards against a regression where
// ApplyConfig's dispatcher-load barrier could fall through to launching the
// inhibitor goroutine even when a concurrent Stop had already torn the
// alertmanager down — leaving the inhibitor with no live cancel hook and
// leaking it forever.
//
// The race window is microseconds wide, so we run the ApplyConfig+Stop pair
// many times to give the scheduler a realistic chance of exercising it.
// Crucially, we do NOT add `goleak.IgnoreTopFunction(".../inhibit.(*Inhibitor).run")`
// here (unlike TestMultitenantAlertmanager_loadAndSyncConfigs), because that
// is exactly the goroutine class this test must catch when leaked.
func TestApplyConfigStopRace_NoInhibitorLeak(t *testing.T) {
	utiltest.VerifyNoLeak(t,
		// Dispatcher.Stop signals cancellation but doesn't synchronously wait for
		// every spawned goroutine to return; in addition, this test deliberately
		// races ApplyConfig with Stop, and a Stop that wins the race against the
		// FIRST ApplyConfig sees am.dispatcher==nil and never calls dispatcher.Stop
		// on the dispatcher that ApplyConfig subsequently launches — that is a
		// separate, pre-existing leak (not the one this test is guarding against).
		// We deliberately do NOT ignore inhibit.(*Inhibitor).run here — that IS
		// the goroutine class this test must catch when leaked.
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/dispatch.(*Dispatcher).run"),
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/dispatch.(*Dispatcher).run.func1"),
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/dispatch.(*Dispatcher).run.func2"),
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/dispatch.(*Dispatcher).run.func3"),
		goleak.IgnoreTopFunction("github.com/prometheus/alertmanager/dispatch.(*aggrGroup).run"),
	)

	cfgRaw := `receivers:
- name: 'prod'

route:
  group_by: ['alertname']
  group_wait: 10ms
  group_interval: 10ms
  receiver: 'prod'`

	cfg, err := config.Load(cfgRaw)
	require.NoError(t, err)

	const iterations = 50
	for i := 0; i < iterations; i++ {
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
			PersisterConfig:   PersisterConfig{Interval: time.Hour},
		}, prometheus.NewPedanticRegistry())
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			// ApplyConfig can return either nil (the happy path) or nil after
			// bailing out on a concurrent Stop. Both are fine; we only care
			// that no inhibitor goroutine is left running.
			_ = am.ApplyConfig(cfg, nil, cfgRaw)
		}()
		go func() {
			defer wg.Done()
			am.StopAndWait()
		}()
		wg.Wait()
	}
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
	alert               *alert.Alert
	existing            bool
	delete              bool // true=delete, false=insert.
	expectedInsertError error

	// expected values after operation.
	expectedCount     int
	expectedTotalSize int
}

func TestAlertsLimiterWithNoLimits(t *testing.T) {
	ops := []callbackOp{
		{alert: &alert.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &alert.Alert{Alert: alert2}, existing: false, expectedCount: 2, expectedTotalSize: alert1Size + alert2Size},
		{alert: &alert.Alert{Alert: alert2}, delete: true, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &alert.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	testLimiter(t, &mockAlertManagerLimits{}, ops)
}

func TestAlertsLimiterWithCountLimit(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}
	alert2WithMoreAnnotationsSize := alertSize(alert2WithMoreAnnotations)

	ops := []callbackOp{
		{alert: &alert.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &alert.Alert{Alert: alert2}, existing: false, expectedInsertError: fmt.Errorf(errTooManyAlerts, 1), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &alert.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},

		{alert: &alert.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		// Update of existing alert works -- doesn't change count.
		{alert: &alert.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedCount: 1, expectedTotalSize: alert2WithMoreAnnotationsSize},
		{alert: &alert.Alert{Alert: alert2}, delete: true, expectedCount: 0, expectedTotalSize: 0},
	}

	testLimiter(t, &mockAlertManagerLimits{maxAlertsCount: 1}, ops)
}

func TestAlertsLimiterWithSizeLimit(t *testing.T) {
	alert2WithMoreAnnotations := alert2
	alert2WithMoreAnnotations.Annotations = model.LabelSet{"job": "test", "cluster": "prod", "new": "super-long-annotation"}

	ops := []callbackOp{
		{alert: &alert.Alert{Alert: alert1}, existing: false, expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &alert.Alert{Alert: alert2}, existing: false, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &alert.Alert{Alert: alert2WithMoreAnnotations}, existing: false, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert1Size},
		{alert: &alert.Alert{Alert: alert1}, delete: true, expectedCount: 0, expectedTotalSize: 0},

		{alert: &alert.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &alert.Alert{Alert: alert2}, delete: true, expectedCount: 0, expectedTotalSize: 0},
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
		{alert: &alert.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &alert.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedInsertError: fmt.Errorf(errAlertsTooBig, alert2Size), expectedCount: 1, expectedTotalSize: alert2Size},
	})

	// Updating alert with larger annotations in the limit works fine.
	testLimiter(t, &mockAlertManagerLimits{maxAlertsSizeBytes: alert2WithMoreAnnotationsSize}, []callbackOp{
		{alert: &alert.Alert{Alert: alert2}, existing: false, expectedCount: 1, expectedTotalSize: alert2Size},
		{alert: &alert.Alert{Alert: alert2WithMoreAnnotations}, existing: true, expectedCount: 1, expectedTotalSize: alert2WithMoreAnnotationsSize},
		{alert: &alert.Alert{Alert: alert2}, existing: true, expectedCount: 1, expectedTotalSize: alert2Size},
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

// cloneSilence returns a deep copy of a silence. It is used in tests.
func cloneSilence(t *testing.T, sil *silencepb.Silence) *silencepb.Silence {
	t.Helper()
	return googleproto.Clone(sil).(*silencepb.Silence)
}

func toMeshSilence(t *testing.T, sil *silencepb.Silence, retention time.Duration) *silencepb.MeshSilence {
	t.Helper()
	return &silencepb.MeshSilence{
		Silence:   sil,
		ExpiresAt: timestamppb.New(sil.EndsAt.AsTime().Add(retention)),
	}
}

func TestSilenceLimits(t *testing.T) {
	ctx := context.Background()
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
		StartsAt: timestamppb.Now(),
		EndsAt:   timestamppb.New(time.Now().Add(5 * time.Minute)),
	}
	require.NoError(t, am.silences.Set(ctx, sil1))

	// Insert sil2 should fail because maximum number of silences has been
	// exceeded.
	sil2 := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{{Name: "c", Pattern: "d"}},
		StartsAt: timestamppb.Now(),
		EndsAt:   timestamppb.New(time.Now().Add(5 * time.Minute)),
	}
	require.EqualError(t, am.silences.Set(ctx, sil2), "exceeded maximum number of silences: 1 (limit: 1)")

	// Expire sil1 and run the GC. This should allow sil2 to be inserted.
	require.NoError(t, am.silences.Expire(ctx, sil1.Id))
	n, err := am.silences.GC()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.NoError(t, am.silences.Set(ctx, sil2))

	// Expire sil2 and run the GC.
	require.NoError(t, am.silences.Expire(ctx, sil2.Id))
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
		StartsAt:  timestamppb.Now(),
		EndsAt:    timestamppb.New(time.Now().Add(5 * time.Minute)),
	}
	require.EqualError(t, am.silences.Set(ctx, sil3), fmt.Sprintf("silence exceeded maximum size: %d bytes (limit: 4096 bytes)", googleproto.Size(toMeshSilence(t, sil3, 0))))

	// Should be able to insert sil4.
	sil4 := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{{Name: "k", Pattern: "l"}},
		StartsAt: timestamppb.Now(),
		EndsAt:   timestamppb.New(time.Now().Add(5 * time.Minute)),
	}
	require.NoError(t, am.silences.Set(ctx, sil4))

	// Should be able to update sil4 without modifications. It is expected to
	// keep the same ID.
	sil5 := cloneSilence(t, sil4)
	require.NoError(t, am.silences.Set(ctx, sil5))
	require.Equal(t, sil4.Id, sil5.Id)

	// Should be able to update the comment. It is also expected to keep the
	// same ID.
	sil6 := cloneSilence(t, sil5)
	sil6.Comment = "m"
	require.NoError(t, am.silences.Set(ctx, sil6))
	require.Equal(t, sil5.Id, sil6.Id)

	// Should not be able to update the start and end time as this requires
	// sil6 to be expired and a new silence to be created. However, this would
	// exceed the maximum number of silences, which counts both active and
	// expired silences.
	sil7 := cloneSilence(t, sil6)
	sil7.StartsAt = timestamppb.New(time.Now().Add(5 * time.Minute))
	sil7.EndsAt = timestamppb.New(time.Now().Add(10 * time.Minute))
	require.EqualError(t, am.silences.Set(ctx, sil7), "exceeded maximum number of silences: 1 (limit: 1)")

	// sil6 should not be expired because the update failed.
	sils, _, err := am.silences.Query(ctx, silence.QState(silence.SilenceStateExpired))
	require.NoError(t, err)
	require.Len(t, sils, 0)

	// Should not be able to update with a comment that exceeds maximum size.
	// Need to increase the maximum number of silences to test this.
	limits.maxSilencesCount = 2
	sil8 := cloneSilence(t, sil6)
	sil8.Comment = strings.Repeat("m", 2<<11)
	require.EqualError(t, am.silences.Set(ctx, sil8), fmt.Sprintf("silence exceeded maximum size: %d bytes (limit: 4096 bytes)", googleproto.Size(toMeshSilence(t, sil8, 0))))

	// sil6 should not be expired because the update failed.
	sils, _, err = am.silences.Query(ctx, silence.QState(silence.SilenceStateExpired))
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
	require.EqualError(t, am.silences.Set(ctx, sil9), fmt.Sprintf("silence exceeded maximum size: %d bytes (limit: 4096 bytes)", googleproto.Size(toMeshSilence(t, sil9, 0))))

	// sil6 should not be expired because the update failed.
	sils, _, err = am.silences.Query(ctx, silence.QState(silence.SilenceStateExpired))
	require.NoError(t, err)
	require.Len(t, sils, 0)
}

// TestAlertmanagerAPIReportsSilencedAlerts is a regression test for the API alert-status
// callback wiring. From the alertmanager v0.33 bump the API computes silenced/inhibited status
// on demand via the setAlertStatus callback registered through api.Update; if that callback does
// not run the silencer/inhibitor, the /alerts endpoint reports every alert as active and ignores
// the silenced/inhibited query filters.
func TestAlertmanagerAPIReportsSilencedAlerts(t *testing.T) {
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
		PersisterConfig:   PersisterConfig{Interval: time.Hour},
	}, prometheus.NewPedanticRegistry())
	require.NoError(t, err)
	defer am.StopAndWait()

	cfgRaw := `receivers:
- name: 'prod'

route:
  receiver: 'prod'`
	cfg, err := config.Load(cfgRaw)
	require.NoError(t, err)
	require.NoError(t, am.ApplyConfig(cfg, nil, cfgRaw))

	now := time.Now()
	require.NoError(t, am.alerts.Put(context.Background(), &alert.Alert{
		Alert: model.Alert{
			Labels:   model.LabelSet{"alertname": "SilencedAlert"},
			StartsAt: now.Add(-time.Minute),
			EndsAt:   now.Add(time.Hour),
		},
		UpdatedAt: now,
	}))

	sil := &silencepb.Silence{
		Matchers: []*silencepb.Matcher{{Name: "alertname", Pattern: "SilencedAlert"}},
		StartsAt: timestamppb.New(now.Add(-time.Minute)),
		EndsAt:   timestamppb.New(now.Add(time.Hour)),
	}
	require.NoError(t, am.silences.Set(context.Background(), sil))

	// The alert should be reported as suppressed (silenced) by the silence we created.
	alerts := getAPIAlerts(t, am, "/am/api/v2/alerts")
	require.Len(t, alerts, 1)
	require.Equal(t, string(alert.AlertStateSuppressed), alerts[0].Status.State)
	require.Equal(t, []string{sil.Id}, alerts[0].Status.SilencedBy)

	// The silenced=false filter should now exclude it.
	alerts = getAPIAlerts(t, am, "/am/api/v2/alerts?silenced=false")
	require.Empty(t, alerts)
}

type apiAlertResponse struct {
	Status struct {
		State      string   `json:"state"`
		SilencedBy []string `json:"silencedBy"`
	} `json:"status"`
}

func getAPIAlerts(t *testing.T, am *Alertmanager, path string) []apiAlertResponse {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "http://alertmanager"+path, nil)
	rec := httptest.NewRecorder()
	am.mux.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var alerts []apiAlertResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&alerts))
	return alerts
}
