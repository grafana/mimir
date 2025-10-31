// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/manager_metrics_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"bytes"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	managerMetrics := NewManagerMetrics(log.NewNopLogger())
	mainReg.MustRegister(managerMetrics)
	managerMetrics.AddUserRegistry("user1", populateManager(1))
	managerMetrics.AddUserRegistry("user2", populateManager(10))
	managerMetrics.AddUserRegistry("user3", populateManager(100))

	managerMetrics.AddUserRegistry("user4", populateManager(1000))
	managerMetrics.RemoveUserRegistry("user4")

	//noinspection ALL
	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
# HELP cortex_prometheus_last_evaluation_samples The number of samples returned during the last rule group evaluation.
# TYPE cortex_prometheus_last_evaluation_samples gauge
cortex_prometheus_last_evaluation_samples{rule_group="group_one",user="user1"} 1000
cortex_prometheus_last_evaluation_samples{rule_group="group_one",user="user2"} 10000
cortex_prometheus_last_evaluation_samples{rule_group="group_one",user="user3"} 100000
cortex_prometheus_last_evaluation_samples{rule_group="group_two",user="user1"} 1000
cortex_prometheus_last_evaluation_samples{rule_group="group_two",user="user2"} 10000
cortex_prometheus_last_evaluation_samples{rule_group="group_two",user="user3"} 100000
# HELP cortex_prometheus_rule_evaluation_duration_seconds The duration for a rule to execute.
# TYPE cortex_prometheus_rule_evaluation_duration_seconds summary
cortex_prometheus_rule_evaluation_duration_seconds{user="user1",quantile="0.5"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user1",quantile="0.9"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user1",quantile="0.99"} 1
cortex_prometheus_rule_evaluation_duration_seconds_sum{user="user1"} 1
cortex_prometheus_rule_evaluation_duration_seconds_count{user="user1"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user2",quantile="0.5"} 10
cortex_prometheus_rule_evaluation_duration_seconds{user="user2",quantile="0.9"} 10
cortex_prometheus_rule_evaluation_duration_seconds{user="user2",quantile="0.99"} 10
cortex_prometheus_rule_evaluation_duration_seconds_sum{user="user2"} 10
cortex_prometheus_rule_evaluation_duration_seconds_count{user="user2"} 1
cortex_prometheus_rule_evaluation_duration_seconds{user="user3",quantile="0.5"} 100
cortex_prometheus_rule_evaluation_duration_seconds{user="user3",quantile="0.9"} 100
cortex_prometheus_rule_evaluation_duration_seconds{user="user3",quantile="0.99"} 100
cortex_prometheus_rule_evaluation_duration_seconds_sum{user="user3"} 100
cortex_prometheus_rule_evaluation_duration_seconds_count{user="user3"} 1
# HELP cortex_prometheus_rule_evaluation_failures_total The total number of rule evaluation failures.
# TYPE cortex_prometheus_rule_evaluation_failures_total counter
cortex_prometheus_rule_evaluation_failures_total{reason="user",rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_evaluation_failures_total{reason="user",rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_evaluation_failures_total{reason="user",rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_evaluation_failures_total{reason="user",rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_evaluation_failures_total{reason="user",rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_evaluation_failures_total{reason="user",rule_group="group_two",user="user3"} 100
# HELP cortex_prometheus_rule_evaluations_total The total number of rule evaluations.
# TYPE cortex_prometheus_rule_evaluations_total counter
cortex_prometheus_rule_evaluations_total{rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_evaluations_total{rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_evaluations_total{rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_evaluations_total{rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_evaluations_total{rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_evaluations_total{rule_group="group_two",user="user3"} 100
# HELP cortex_prometheus_rule_group_duration_seconds The duration of rule group evaluations.
# TYPE cortex_prometheus_rule_group_duration_seconds summary
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.01"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.05"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.5"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.9"} 1
cortex_prometheus_rule_group_duration_seconds{user="user1",quantile="0.99"} 1
cortex_prometheus_rule_group_duration_seconds_sum{user="user1"} 1
cortex_prometheus_rule_group_duration_seconds_count{user="user1"} 1
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.01"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.05"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.5"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.9"} 10
cortex_prometheus_rule_group_duration_seconds{user="user2",quantile="0.99"} 10
cortex_prometheus_rule_group_duration_seconds_sum{user="user2"} 10
cortex_prometheus_rule_group_duration_seconds_count{user="user2"} 1
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.01"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.05"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.5"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.9"} 100
cortex_prometheus_rule_group_duration_seconds{user="user3",quantile="0.99"} 100
cortex_prometheus_rule_group_duration_seconds_sum{user="user3"} 100
cortex_prometheus_rule_group_duration_seconds_count{user="user3"} 1
# HELP cortex_prometheus_rule_group_iterations_missed_total The total number of rule group evaluations missed due to slow rule group evaluation.
# TYPE cortex_prometheus_rule_group_iterations_missed_total counter
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_group_iterations_missed_total{rule_group="group_two",user="user3"} 100
# HELP cortex_prometheus_rule_group_iterations_total The total number of scheduled rule group evaluations, whether executed or missed.
# TYPE cortex_prometheus_rule_group_iterations_total counter
cortex_prometheus_rule_group_iterations_total{rule_group="group_one",user="user1"} 1
cortex_prometheus_rule_group_iterations_total{rule_group="group_one",user="user2"} 10
cortex_prometheus_rule_group_iterations_total{rule_group="group_one",user="user3"} 100
cortex_prometheus_rule_group_iterations_total{rule_group="group_two",user="user1"} 1
cortex_prometheus_rule_group_iterations_total{rule_group="group_two",user="user2"} 10
cortex_prometheus_rule_group_iterations_total{rule_group="group_two",user="user3"} 100
# HELP cortex_prometheus_rule_group_last_duration_seconds The duration of the last rule group evaluation.
# TYPE cortex_prometheus_rule_group_last_duration_seconds gauge
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_one",user="user1"} 1000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_one",user="user2"} 10000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_one",user="user3"} 100000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_two",user="user1"} 1000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_two",user="user2"} 10000
cortex_prometheus_rule_group_last_duration_seconds{rule_group="group_two",user="user3"} 100000
# HELP cortex_prometheus_rule_group_last_evaluation_timestamp_seconds The timestamp of the last rule group evaluation in seconds.
# TYPE cortex_prometheus_rule_group_last_evaluation_timestamp_seconds gauge
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_one",user="user1"} 1000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_one",user="user2"} 10000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_one",user="user3"} 100000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_two",user="user1"} 1000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_two",user="user2"} 10000
cortex_prometheus_rule_group_last_evaluation_timestamp_seconds{rule_group="group_two",user="user3"} 100000
# HELP cortex_prometheus_rule_group_rules The number of rules.
# TYPE cortex_prometheus_rule_group_rules gauge
cortex_prometheus_rule_group_rules{rule_group="group_one",user="user1"} 1000
cortex_prometheus_rule_group_rules{rule_group="group_one",user="user2"} 10000
cortex_prometheus_rule_group_rules{rule_group="group_one",user="user3"} 100000
cortex_prometheus_rule_group_rules{rule_group="group_two",user="user1"} 1000
cortex_prometheus_rule_group_rules{rule_group="group_two",user="user2"} 10000
cortex_prometheus_rule_group_rules{rule_group="group_two",user="user3"} 100000
# HELP cortex_prometheus_rule_group_last_restore_duration_seconds The duration of the last alert rules alerts restoration using the `+"`ALERTS_FOR_STATE`"+` series across all rule groups.
# TYPE cortex_prometheus_rule_group_last_restore_duration_seconds gauge
cortex_prometheus_rule_group_last_restore_duration_seconds{user="user1"} 20
cortex_prometheus_rule_group_last_restore_duration_seconds{user="user2"} 200
cortex_prometheus_rule_group_last_restore_duration_seconds{user="user3"} 2000
`))
	require.NoError(t, err)
}

func populateManager(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()

	metrics := rules.NewGroupMetrics(r)

	metrics.EvalDuration.Observe(base)
	metrics.IterationDuration.Observe(base)

	metrics.IterationsScheduled.WithLabelValues("group_one").Add(base)
	metrics.IterationsScheduled.WithLabelValues("group_two").Add(base)
	metrics.IterationsMissed.WithLabelValues("group_one").Add(base)
	metrics.IterationsMissed.WithLabelValues("group_two").Add(base)
	metrics.EvalTotal.WithLabelValues("group_one").Add(base)
	metrics.EvalTotal.WithLabelValues("group_two").Add(base)
	metrics.EvalFailures.WithLabelValues("group_one", "user").Add(base)
	metrics.EvalFailures.WithLabelValues("group_two", "user").Add(base)

	metrics.GroupLastEvalTime.WithLabelValues("group_one").Add(base * 1000)
	metrics.GroupLastEvalTime.WithLabelValues("group_two").Add(base * 1000)

	metrics.GroupLastDuration.WithLabelValues("group_one").Add(base * 1000)
	metrics.GroupLastDuration.WithLabelValues("group_two").Add(base * 1000)

	metrics.GroupRules.WithLabelValues("group_one").Add(base * 1000)
	metrics.GroupRules.WithLabelValues("group_two").Add(base * 1000)

	metrics.GroupSamples.WithLabelValues("group_one").Add(base * 1000)
	metrics.GroupSamples.WithLabelValues("group_two").Add(base * 1000)

	metrics.GroupLastRestoreDuration.WithLabelValues("group_one").Add(base * 10)
	metrics.GroupLastRestoreDuration.WithLabelValues("group_two").Add(base * 10)

	return r
}

func TestMetricsArePerUser(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	managerMetrics := NewManagerMetrics(log.NewNopLogger())
	mainReg.MustRegister(managerMetrics)
	managerMetrics.AddUserRegistry("user1", populateManager(1))
	managerMetrics.AddUserRegistry("user2", populateManager(10))
	managerMetrics.AddUserRegistry("user3", populateManager(100))

	ch := make(chan prometheus.Metric)

	defer func() {
		// drain the channel, so that collecting goroutine can stop.
		// This is useful if test fails.
		// nolint:revive // We want to drain the channel.
		for range ch {
		}
	}()

	go func() {
		managerMetrics.Collect(ch)
		close(ch)
	}()

	for m := range ch {
		desc := m.Desc()

		dtoM := &dto.Metric{}
		err := m.Write(dtoM)

		require.NoError(t, err)

		foundUserLabel := false
		for _, l := range dtoM.Label {
			if l.GetName() == "user" {
				foundUserLabel = true
				break
			}
		}

		assert.True(t, foundUserLabel, "user label not found for metric %s", desc.String())
	}
}

func TestNotifierMetrics(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	notifierMetrics := NewNotifierMetrics(log.NewNopLogger())
	mainReg.MustRegister(notifierMetrics)
	notifierMetrics.AddUserRegistry("user1", populateNotifier(1))
	notifierMetrics.AddUserRegistry("user2", populateNotifier(10))
	notifierMetrics.AddUserRegistry("user3", populateNotifier(100))

	err := testutil.GatherAndCompare(mainReg, bytes.NewBufferString(`
# HELP cortex_prometheus_notifications_errors_sum The sum of notification errors for all users.
# TYPE cortex_prometheus_notifications_errors_sum counter
cortex_prometheus_notifications_errors_sum{alertmanager="alertmanager1"} 111
cortex_prometheus_notifications_errors_sum{alertmanager="alertmanager2"} 111
cortex_prometheus_notifications_errors_sum{alertmanager="alertmanager3"} 111
# HELP cortex_prometheus_notifications_sent_sum The sum of notifications sent for all users.
# TYPE cortex_prometheus_notifications_sent_sum counter
cortex_prometheus_notifications_sent_sum{alertmanager="alertmanager1"} 111
cortex_prometheus_notifications_sent_sum{alertmanager="alertmanager2"} 111
cortex_prometheus_notifications_sent_sum{alertmanager="alertmanager3"} 111
`))
	require.NoError(t, err)
}

type mockNotifierMetrics struct {
	Sent   *prometheus.CounterVec
	Errors *prometheus.CounterVec
}

func newMockNotifierMetrics(reg prometheus.Registerer) *mockNotifierMetrics {
	return &mockNotifierMetrics{
		Sent: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "prometheus_notifications_sent_total",
			Help: "The total number of notifications sent.",
		}, []string{"alertmanager"}),
		Errors: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "prometheus_notifications_errors_total",
			Help: "The total number of notifications failed.",
		}, []string{"alertmanager"}),
	}
}

func populateNotifier(base float64) *prometheus.Registry {
	r := prometheus.NewRegistry()

	metrics := newMockNotifierMetrics(r)

	metrics.Sent.WithLabelValues("alertmanager1").Add(base)
	metrics.Sent.WithLabelValues("alertmanager2").Add(base)
	metrics.Sent.WithLabelValues("alertmanager3").Add(base)

	metrics.Errors.WithLabelValues("alertmanager1").Add(base)
	metrics.Errors.WithLabelValues("alertmanager2").Add(base)
	metrics.Errors.WithLabelValues("alertmanager3").Add(base)

	return r
}

func TestNotifierMetricsAreAggregated(t *testing.T) {
	mainReg := prometheus.NewPedanticRegistry()

	notifierMetrics := NewNotifierMetrics(log.NewNopLogger())
	mainReg.MustRegister(notifierMetrics)
	notifierMetrics.AddUserRegistry("user1", populateNotifier(1))
	notifierMetrics.AddUserRegistry("user2", populateNotifier(10))
	notifierMetrics.AddUserRegistry("user3", populateNotifier(100))

	ch := make(chan prometheus.Metric)

	defer func() {
		// drain the channel, so that collecting goroutine can stop.
		// This is useful if test fails.
		// nolint:revive // We want to drain the channel.
		for range ch {
		}
	}()

	go func() {
		notifierMetrics.Collect(ch)
		close(ch)
	}()

	for m := range ch {
		desc := m.Desc()

		dtoM := &dto.Metric{}
		err := m.Write(dtoM)

		require.NoError(t, err)

		foundUserLabel := false
		foundAlertmanagerLabel := false
		for _, l := range dtoM.Label {
			if l.GetName() == "user" {
				foundUserLabel = true
				break
			}
			if l.GetName() == "alertmanager" {
				foundAlertmanagerLabel = true
				break
			}
		}

		assert.False(t, foundUserLabel, "user label found for metric %s", desc.String())
		assert.True(t, foundAlertmanagerLabel, "alertmanager label not found for metric %s", desc.String())
	}
}
