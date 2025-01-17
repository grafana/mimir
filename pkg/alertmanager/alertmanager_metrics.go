// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertmanager_metrics.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertmanager

import (
	"github.com/go-kit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// This struct aggregates metrics exported by Alertmanager
// and re-exports those aggregates as Mimir metrics.
type alertmanagerMetrics struct {
	regs *dskit_metrics.TenantRegistries

	// exported metrics, gathered from Alertmanager API
	alertsReceived *prometheus.Desc
	alertsInvalid  *prometheus.Desc

	// exported metrics, gathered from Alertmanager PipelineBuilder
	numNotifications                   *prometheus.Desc
	numFailedNotifications             *prometheus.Desc
	numNotificationRequestsTotal       *prometheus.Desc
	numNotificationRequestsFailedTotal *prometheus.Desc
	numNotificationSuppressedTotal     *prometheus.Desc
	notificationLatencySeconds         *prometheus.Desc

	// exported metrics, gathered from Alertmanager nflog
	nflogGCDuration              *prometheus.Desc
	nflogSnapshotDuration        *prometheus.Desc
	nflogSnapshotSize            *prometheus.Desc
	nflogMaintenanceTotal        *prometheus.Desc
	nflogMaintenanceErrorsTotal  *prometheus.Desc
	nflogQueriesTotal            *prometheus.Desc
	nflogQueryErrorsTotal        *prometheus.Desc
	nflogQueryDuration           *prometheus.Desc
	nflogPropagatedMessagesTotal *prometheus.Desc

	// exported metrics, gathered from Alertmanager Marker
	markerAlerts *prometheus.Desc

	// exported metrics, gathered from Alertmanager Silences
	silencesGCDuration              *prometheus.Desc
	silencesMaintenanceTotal        *prometheus.Desc
	silencesMaintenanceErrorsTotal  *prometheus.Desc
	silencesSnapshotDuration        *prometheus.Desc
	silencesSnapshotSize            *prometheus.Desc
	silencesQueriesTotal            *prometheus.Desc
	silencesQueryErrorsTotal        *prometheus.Desc
	silencesQueryDuration           *prometheus.Desc
	silences                        *prometheus.Desc
	silencesPropagatedMessagesTotal *prometheus.Desc

	// The alertmanager config hash.
	configHashValue *prometheus.Desc

	partialMerges           *prometheus.Desc
	partialMergesFailed     *prometheus.Desc
	replicationTotal        *prometheus.Desc
	replicationFailed       *prometheus.Desc
	fetchReplicaStateTotal  *prometheus.Desc
	fetchReplicaStateFailed *prometheus.Desc
	initialSyncTotal        *prometheus.Desc
	initialSyncCompleted    *prometheus.Desc
	initialSyncDuration     *prometheus.Desc
	persistTotal            *prometheus.Desc
	persistFailed           *prometheus.Desc

	// exported metrics, gathered from Alertmanager Dispatcher
	dispatcherAggrGroups                    *prometheus.Desc
	dispatcherProcessingDuration            *prometheus.Desc
	dispatcherAggregationGroupsLimitReached *prometheus.Desc

	notificationRateLimited  *prometheus.Desc
	insertAlertFailures      *prometheus.Desc
	alertsLimiterAlertsCount *prometheus.Desc
	alertsLimiterAlertsSize  *prometheus.Desc
}

func newAlertmanagerMetrics(logger log.Logger) *alertmanagerMetrics {
	return &alertmanagerMetrics{
		regs: dskit_metrics.NewTenantRegistries(logger),
		alertsReceived: prometheus.NewDesc(
			"cortex_alertmanager_alerts_received_total",
			"The total number of received alerts.",
			[]string{"user"}, nil),
		alertsInvalid: prometheus.NewDesc(
			"cortex_alertmanager_alerts_invalid_total",
			"The total number of received alerts that were invalid.",
			[]string{"user"}, nil),
		numNotifications: prometheus.NewDesc(
			"cortex_alertmanager_notifications_total",
			"The total number of attempted notifications.",
			[]string{"user", "integration"}, nil),
		numFailedNotifications: prometheus.NewDesc(
			"cortex_alertmanager_notifications_failed_total",
			"The total number of failed notifications.",
			[]string{"user", "integration", "reason"}, nil),
		numNotificationRequestsTotal: prometheus.NewDesc(
			"cortex_alertmanager_notification_requests_total",
			"The total number of attempted notification requests.",
			[]string{"user", "integration"}, nil),
		numNotificationRequestsFailedTotal: prometheus.NewDesc(
			"cortex_alertmanager_notification_requests_failed_total",
			"The total number of failed notification requests.",
			[]string{"user", "integration"}, nil),
		numNotificationSuppressedTotal: prometheus.NewDesc(
			"cortex_alertmanager_notifications_suppressed_total",
			"The total number of notifications suppressed for being silenced, inhibited, outside of active time intervals or within muted time intervals.",
			[]string{"user", "reason"}, nil),
		notificationLatencySeconds: prometheus.NewDesc(
			"cortex_alertmanager_notification_latency_seconds",
			"The latency of notifications in seconds.",
			nil, nil),
		nflogGCDuration: prometheus.NewDesc(
			"cortex_alertmanager_nflog_gc_duration_seconds",
			"Duration of the last notification log garbage collection cycle.",
			nil, nil),
		nflogSnapshotDuration: prometheus.NewDesc(
			"cortex_alertmanager_nflog_snapshot_duration_seconds",
			"Duration of the last notification log snapshot.",
			nil, nil),
		nflogSnapshotSize: prometheus.NewDesc(
			"cortex_alertmanager_nflog_snapshot_size_bytes",
			"Size of the last notification log snapshot in bytes.",
			nil, nil),
		nflogMaintenanceTotal: prometheus.NewDesc(
			"cortex_alertmanager_nflog_maintenance_total",
			"How many maintenances were executed for the notification log.",
			nil, nil),
		nflogMaintenanceErrorsTotal: prometheus.NewDesc(
			"cortex_alertmanager_nflog_maintenance_errors_total",
			"How many maintenances were executed for the notification log that failed.",
			nil, nil),
		nflogQueriesTotal: prometheus.NewDesc(
			"cortex_alertmanager_nflog_queries_total",
			"Number of notification log queries were received.",
			nil, nil),
		nflogQueryErrorsTotal: prometheus.NewDesc(
			"cortex_alertmanager_nflog_query_errors_total",
			"Number notification log received queries that failed.",
			nil, nil),
		nflogQueryDuration: prometheus.NewDesc(
			"cortex_alertmanager_nflog_query_duration_seconds",
			"Duration of notification log query evaluation.",
			nil, nil),
		nflogPropagatedMessagesTotal: prometheus.NewDesc(
			"cortex_alertmanager_nflog_gossip_messages_propagated_total",
			"Number of received gossip messages that have been further gossiped.",
			nil, nil),
		markerAlerts: prometheus.NewDesc(
			"cortex_alertmanager_alerts",
			"How many alerts by state.",
			[]string{"user", "state"}, nil),
		silencesGCDuration: prometheus.NewDesc(
			"cortex_alertmanager_silences_gc_duration_seconds",
			"Duration of the last silence garbage collection cycle.",
			nil, nil),
		silencesMaintenanceTotal: prometheus.NewDesc(
			"cortex_alertmanager_silences_maintenance_total",
			"How many maintenances were executed for silences.",
			nil, nil),
		silencesMaintenanceErrorsTotal: prometheus.NewDesc(
			"cortex_alertmanager_silences_maintenance_errors_total",
			"How many maintenances were executed for silences that failed.",
			nil, nil),
		silencesSnapshotDuration: prometheus.NewDesc(
			"cortex_alertmanager_silences_snapshot_duration_seconds",
			"Duration of the last silence snapshot.",
			nil, nil),
		silencesSnapshotSize: prometheus.NewDesc(
			"cortex_alertmanager_silences_snapshot_size_bytes",
			"Size of the last silence snapshot in bytes.",
			nil, nil),
		silencesQueriesTotal: prometheus.NewDesc(
			"cortex_alertmanager_silences_queries_total",
			"How many silence queries were received.",
			nil, nil),
		silencesQueryErrorsTotal: prometheus.NewDesc(
			"cortex_alertmanager_silences_query_errors_total",
			"How many silence received queries did not succeed.",
			nil, nil),
		silencesQueryDuration: prometheus.NewDesc(
			"cortex_alertmanager_silences_query_duration_seconds",
			"Duration of silence query evaluation.",
			nil, nil),
		silencesPropagatedMessagesTotal: prometheus.NewDesc(
			"cortex_alertmanager_silences_gossip_messages_propagated_total",
			"Number of received gossip messages that have been further gossiped.",
			nil, nil),
		silences: prometheus.NewDesc(
			"cortex_alertmanager_silences",
			"How many silences by state.",
			[]string{"user", "state"}, nil),
		configHashValue: prometheus.NewDesc(
			"cortex_alertmanager_config_hash",
			"Hash of the currently loaded alertmanager configuration. Note that this is not a cryptographically strong hash.",
			[]string{"user"}, nil),
		partialMerges: prometheus.NewDesc(
			"cortex_alertmanager_partial_state_merges_total",
			"Number of times we have received a partial state to merge for a key.",
			[]string{"user"}, nil),
		partialMergesFailed: prometheus.NewDesc(
			"cortex_alertmanager_partial_state_merges_failed_total",
			"Number of times we have failed to merge a partial state received for a key.",
			[]string{"user"}, nil),
		replicationTotal: prometheus.NewDesc(
			"cortex_alertmanager_state_replication_total",
			"Number of times we have tried to replicate a state to other alertmanagers",
			[]string{"user"}, nil),
		replicationFailed: prometheus.NewDesc(
			"cortex_alertmanager_state_replication_failed_total",
			"Number of times we have failed to replicate a state to other alertmanagers",
			[]string{"user"}, nil),
		fetchReplicaStateTotal: prometheus.NewDesc(
			"cortex_alertmanager_state_fetch_replica_state_total",
			"Number of times we have tried to read and merge the full state from another replica.",
			nil, nil),
		fetchReplicaStateFailed: prometheus.NewDesc(
			"cortex_alertmanager_state_fetch_replica_state_failed_total",
			"Number of times we have failed to read and merge the full state from another replica.",
			nil, nil),
		initialSyncTotal: prometheus.NewDesc(
			"cortex_alertmanager_state_initial_sync_total",
			"Number of times we have tried to sync initial state from peers or storage.",
			nil, nil),
		initialSyncCompleted: prometheus.NewDesc(
			"cortex_alertmanager_state_initial_sync_completed_total",
			"Number of times we have completed syncing initial state for each possible outcome.",
			[]string{"outcome"}, nil),
		initialSyncDuration: prometheus.NewDesc(
			"cortex_alertmanager_state_initial_sync_duration_seconds",
			"Time spent syncing initial state from peers or storage.",
			nil, nil),
		persistTotal: prometheus.NewDesc(
			"cortex_alertmanager_state_persist_total",
			"Number of times we have tried to persist the running state to storage.",
			nil, nil),
		persistFailed: prometheus.NewDesc(
			"cortex_alertmanager_state_persist_failed_total",
			"Number of times we have failed to persist the running state to storage.",
			nil, nil),
		dispatcherAggrGroups: prometheus.NewDesc(
			"cortex_alertmanager_dispatcher_aggregation_groups",
			"Number of active aggregation groups",
			nil, nil),
		dispatcherProcessingDuration: prometheus.NewDesc(
			"cortex_alertmanager_dispatcher_alert_processing_duration_seconds",
			"Summary of latencies for the processing of alerts.",
			nil, nil),
		dispatcherAggregationGroupsLimitReached: prometheus.NewDesc(
			"cortex_alertmanager_dispatcher_aggregation_group_limit_reached_total",
			"Number of times when dispatcher failed to create new aggregation group due to limit.",
			[]string{"user"}, nil),
		notificationRateLimited: prometheus.NewDesc(
			"cortex_alertmanager_notification_rate_limited_total",
			"Total number of rate-limited notifications per integration.",
			[]string{"user", "integration"}, nil),
		insertAlertFailures: prometheus.NewDesc(
			"cortex_alertmanager_alerts_insert_limited_total",
			"Total number of failures to store alert due to hitting alertmanager limits.",
			[]string{"user"}, nil),
		alertsLimiterAlertsCount: prometheus.NewDesc(
			"cortex_alertmanager_alerts_limiter_current_alerts",
			"Number of alerts tracked by alerts limiter.",
			[]string{"user"}, nil),
		alertsLimiterAlertsSize: prometheus.NewDesc(
			"cortex_alertmanager_alerts_limiter_current_alerts_size_bytes",
			"Total size of alerts tracked by alerts limiter.",
			[]string{"user"}, nil),
	}
}

func (m *alertmanagerMetrics) addUserRegistry(user string, reg *prometheus.Registry) {
	m.regs.AddTenantRegistry(user, reg)
}

func (m *alertmanagerMetrics) removeUserRegistry(user string) {
	// We need to go for a soft deletion here, as hard deletion requires
	// that _all_ metrics except gauges are per-user.
	m.regs.RemoveTenantRegistry(user, false)
}

func (m *alertmanagerMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- m.alertsReceived
	out <- m.alertsInvalid
	out <- m.numNotifications
	out <- m.numFailedNotifications
	out <- m.numNotificationRequestsTotal
	out <- m.numNotificationRequestsFailedTotal
	out <- m.numNotificationSuppressedTotal
	out <- m.notificationLatencySeconds
	out <- m.markerAlerts
	out <- m.nflogGCDuration
	out <- m.nflogSnapshotDuration
	out <- m.nflogSnapshotSize
	out <- m.nflogMaintenanceTotal
	out <- m.nflogMaintenanceErrorsTotal
	out <- m.nflogQueriesTotal
	out <- m.nflogQueryErrorsTotal
	out <- m.nflogQueryDuration
	out <- m.nflogPropagatedMessagesTotal
	out <- m.silencesGCDuration
	out <- m.silencesMaintenanceTotal
	out <- m.silencesMaintenanceErrorsTotal
	out <- m.silencesSnapshotDuration
	out <- m.silencesSnapshotSize
	out <- m.silencesQueriesTotal
	out <- m.silencesQueryErrorsTotal
	out <- m.silencesQueryDuration
	out <- m.silencesPropagatedMessagesTotal
	out <- m.silences
	out <- m.configHashValue
	out <- m.partialMerges
	out <- m.partialMergesFailed
	out <- m.replicationTotal
	out <- m.replicationFailed
	out <- m.fetchReplicaStateTotal
	out <- m.fetchReplicaStateFailed
	out <- m.initialSyncTotal
	out <- m.initialSyncCompleted
	out <- m.initialSyncDuration
	out <- m.persistTotal
	out <- m.persistFailed
	out <- m.dispatcherAggrGroups
	out <- m.dispatcherProcessingDuration
	out <- m.dispatcherAggregationGroupsLimitReached
	out <- m.notificationRateLimited
	out <- m.insertAlertFailures
	out <- m.alertsLimiterAlertsCount
	out <- m.alertsLimiterAlertsSize
}

func (m *alertmanagerMetrics) Collect(out chan<- prometheus.Metric) {
	data := m.regs.BuildMetricFamiliesPerTenant()

	data.SendSumOfCountersPerTenant(out, m.alertsReceived, "alertmanager_alerts_received_total", dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.alertsInvalid, "alertmanager_alerts_invalid_total", dskit_metrics.WithSkipZeroValueMetrics)

	data.SendSumOfCountersPerTenant(out, m.numNotifications, "alertmanager_notifications_total", dskit_metrics.WithLabels("integration"), dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.numFailedNotifications, "alertmanager_notifications_failed_total", dskit_metrics.WithLabels("integration", "reason"), dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.numNotificationRequestsTotal, "alertmanager_notification_requests_total", dskit_metrics.WithLabels("integration"), dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.numNotificationRequestsFailedTotal, "alertmanager_notification_requests_failed_total", dskit_metrics.WithLabels("integration"), dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.numNotificationSuppressedTotal, "alertmanager_notifications_suppressed_total", dskit_metrics.WithLabels("reason"), dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfHistograms(out, m.notificationLatencySeconds, "alertmanager_notification_latency_seconds")
	data.SendSumOfGaugesPerTenant(out, m.markerAlerts, "alertmanager_alerts", dskit_metrics.WithLabels("state"), dskit_metrics.WithSkipZeroValueMetrics)

	data.SendSumOfSummaries(out, m.nflogGCDuration, "alertmanager_nflog_gc_duration_seconds")
	data.SendSumOfSummaries(out, m.nflogSnapshotDuration, "alertmanager_nflog_snapshot_duration_seconds")
	data.SendSumOfGauges(out, m.nflogSnapshotSize, "alertmanager_nflog_snapshot_size_bytes")
	data.SendSumOfCounters(out, m.nflogMaintenanceTotal, "alertmanager_nflog_maintenance_total")
	data.SendSumOfCounters(out, m.nflogMaintenanceErrorsTotal, "alertmanager_nflog_maintenance_errors_total")
	data.SendSumOfCounters(out, m.nflogQueriesTotal, "alertmanager_nflog_queries_total")
	data.SendSumOfCounters(out, m.nflogQueryErrorsTotal, "alertmanager_nflog_query_errors_total")
	data.SendSumOfHistograms(out, m.nflogQueryDuration, "alertmanager_nflog_query_duration_seconds")
	data.SendSumOfCounters(out, m.nflogPropagatedMessagesTotal, "alertmanager_nflog_gossip_messages_propagated_total")

	data.SendSumOfSummaries(out, m.silencesGCDuration, "alertmanager_silences_gc_duration_seconds")
	data.SendSumOfCounters(out, m.silencesMaintenanceTotal, "alertmanager_silences_maintenance_total")
	data.SendSumOfCounters(out, m.silencesMaintenanceErrorsTotal, "alertmanager_silences_maintenance_errors_total")
	data.SendSumOfSummaries(out, m.silencesSnapshotDuration, "alertmanager_silences_snapshot_duration_seconds")
	data.SendSumOfGauges(out, m.silencesSnapshotSize, "alertmanager_silences_snapshot_size_bytes")
	data.SendSumOfCounters(out, m.silencesQueriesTotal, "alertmanager_silences_queries_total")
	data.SendSumOfCounters(out, m.silencesQueryErrorsTotal, "alertmanager_silences_query_errors_total")
	data.SendSumOfHistograms(out, m.silencesQueryDuration, "alertmanager_silences_query_duration_seconds")
	data.SendSumOfCounters(out, m.silencesPropagatedMessagesTotal, "alertmanager_silences_gossip_messages_propagated_total")
	data.SendSumOfGaugesPerTenant(out, m.silences, "alertmanager_silences", dskit_metrics.WithLabels("state"), dskit_metrics.WithSkipZeroValueMetrics)

	data.SendMaxOfGaugesPerTenant(out, m.configHashValue, "alertmanager_config_hash")

	data.SendSumOfCountersPerTenant(out, m.partialMerges, "alertmanager_partial_state_merges_total", dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.partialMergesFailed, "alertmanager_partial_state_merges_failed_total", dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.replicationTotal, "alertmanager_state_replication_total", dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.replicationFailed, "alertmanager_state_replication_failed_total", dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCounters(out, m.fetchReplicaStateTotal, "alertmanager_state_fetch_replica_state_total")
	data.SendSumOfCounters(out, m.fetchReplicaStateFailed, "alertmanager_state_fetch_replica_state_failed_total")
	data.SendSumOfCounters(out, m.initialSyncTotal, "alertmanager_state_initial_sync_total")
	data.SendSumOfCountersWithLabels(out, m.initialSyncCompleted, "alertmanager_state_initial_sync_completed_total", "outcome")
	data.SendSumOfHistograms(out, m.initialSyncDuration, "alertmanager_state_initial_sync_duration_seconds")
	data.SendSumOfCounters(out, m.persistTotal, "alertmanager_state_persist_total")
	data.SendSumOfCounters(out, m.persistFailed, "alertmanager_state_persist_failed_total")

	data.SendSumOfGauges(out, m.dispatcherAggrGroups, "alertmanager_dispatcher_aggregation_groups")
	data.SendSumOfSummaries(out, m.dispatcherProcessingDuration, "alertmanager_dispatcher_alert_processing_duration_seconds")
	data.SendSumOfCountersPerTenant(out, m.dispatcherAggregationGroupsLimitReached, "alertmanager_dispatcher_aggregation_group_limit_reached_total")

	data.SendSumOfCountersPerTenant(out, m.notificationRateLimited, "alertmanager_notification_rate_limited_total", dskit_metrics.WithLabels("integration"), dskit_metrics.WithSkipZeroValueMetrics)
	data.SendSumOfCountersPerTenant(out, m.insertAlertFailures, "alertmanager_alerts_insert_limited_total")
	data.SendSumOfGaugesPerTenant(out, m.alertsLimiterAlertsCount, "alertmanager_alerts_limiter_current_alerts")
	data.SendSumOfGaugesPerTenant(out, m.alertsLimiterAlertsSize, "alertmanager_alerts_limiter_current_alerts_size_bytes")
}
