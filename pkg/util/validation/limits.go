// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"bytes"
	"encoding/json"
	"flag"
	"math"
	"strings"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/ingester/activeseries"
)

const (
	maxLabelNamesPerSeriesFlag = "validation.max-label-names-per-series"
	maxLabelNameLengthFlag     = "validation.max-length-label-name"
	maxLabelValueLengthFlag    = "validation.max-length-label-value"
	creationGracePeriodFlag    = "validation.create-grace-period"
)

// LimitError are errors that do not comply with the limits specified.
type LimitError string

func (e LimitError) Error() string {
	return string(e)
}

type ForwardingRule struct {
	// Ingest defines whether a metric should still be pushed to the Ingesters despite it being forwarded.
	Ingest bool `yaml:"ingest" json:"ingest"`

	// Endpoint is the URL of the remote_write endpoint to which a metric should be forwarded.
	Endpoint string `yaml:"endpoint" json:"endpoint"`
}

// ForwardingRules are keyed by metric names, excluding labels.
type ForwardingRules map[string]ForwardingRule

// Limits describe all the limits for users; can be used to describe global default
// limits via flags, or per-user limits via yaml config.
type Limits struct {
	// Distributor enforced limits.
	IngestionRate             float64             `yaml:"ingestion_rate" json:"ingestion_rate"`
	IngestionBurstSize        int                 `yaml:"ingestion_burst_size" json:"ingestion_burst_size"`
	AcceptHASamples           bool                `yaml:"accept_ha_samples" json:"accept_ha_samples"`
	HAClusterLabel            string              `yaml:"ha_cluster_label" json:"ha_cluster_label"`
	HAReplicaLabel            string              `yaml:"ha_replica_label" json:"ha_replica_label"`
	HAMaxClusters             int                 `yaml:"ha_max_clusters" json:"ha_max_clusters"`
	DropLabels                flagext.StringSlice `yaml:"drop_labels" json:"drop_labels" category:"advanced"`
	MaxLabelNameLength        int                 `yaml:"max_label_name_length" json:"max_label_name_length"`
	MaxLabelValueLength       int                 `yaml:"max_label_value_length" json:"max_label_value_length"`
	MaxLabelNamesPerSeries    int                 `yaml:"max_label_names_per_series" json:"max_label_names_per_series"`
	MaxMetadataLength         int                 `yaml:"max_metadata_length" json:"max_metadata_length"`
	CreationGracePeriod       model.Duration      `yaml:"creation_grace_period" json:"creation_grace_period" category:"advanced"`
	EnforceMetadataMetricName bool                `yaml:"enforce_metadata_metric_name" json:"enforce_metadata_metric_name" category:"advanced"`
	IngestionTenantShardSize  int                 `yaml:"ingestion_tenant_shard_size" json:"ingestion_tenant_shard_size"`
	MetricRelabelConfigs      []*relabel.Config   `yaml:"metric_relabel_configs,omitempty" json:"metric_relabel_configs,omitempty" doc:"nocli|description=List of metric relabel configurations. Note that in most situations, it is more effective to use metrics relabeling directly in the Prometheus server, e.g. remote_write.write_relabel_configs." category:"experimental"`

	// Ingester enforced limits.
	// Series
	MaxGlobalSeriesPerUser   int `yaml:"max_global_series_per_user" json:"max_global_series_per_user"`
	MaxGlobalSeriesPerMetric int `yaml:"max_global_series_per_metric" json:"max_global_series_per_metric"`
	// Metadata
	MaxGlobalMetricsWithMetadataPerUser int `yaml:"max_global_metadata_per_user" json:"max_global_metadata_per_user"`
	MaxGlobalMetadataPerMetric          int `yaml:"max_global_metadata_per_metric" json:"max_global_metadata_per_metric"`
	// Exemplars
	MaxGlobalExemplarsPerUser int `yaml:"max_global_exemplars_per_user" json:"max_global_exemplars_per_user" category:"experimental"`
	// Active series custom trackers
	ActiveSeriesCustomTrackersConfig activeseries.CustomTrackersConfig `yaml:"active_series_custom_trackers_config" json:"active_series_custom_trackers_config" doc:"description=Additional custom trackers for active metrics. If there are active series matching a provided matcher (map value), the count will be exposed in the custom trackers metric labeled using the tracker name (map key). Zero valued counts are not exposed (and removed when they go back to zero)." category:"advanced"`

	// Querier enforced limits.
	MaxChunksPerQuery              int            `yaml:"max_fetched_chunks_per_query" json:"max_fetched_chunks_per_query"`
	MaxFetchedSeriesPerQuery       int            `yaml:"max_fetched_series_per_query" json:"max_fetched_series_per_query"`
	MaxFetchedChunkBytesPerQuery   int            `yaml:"max_fetched_chunk_bytes_per_query" json:"max_fetched_chunk_bytes_per_query"`
	MaxQueryLookback               model.Duration `yaml:"max_query_lookback" json:"max_query_lookback"`
	MaxQueryLength                 model.Duration `yaml:"max_query_length" json:"max_query_length"`
	MaxQueryParallelism            int            `yaml:"max_query_parallelism" json:"max_query_parallelism"`
	MaxLabelsQueryLength           model.Duration `yaml:"max_labels_query_length" json:"max_labels_query_length"`
	MaxCacheFreshness              model.Duration `yaml:"max_cache_freshness" json:"max_cache_freshness" category:"advanced"`
	MaxQueriersPerTenant           int            `yaml:"max_queriers_per_tenant" json:"max_queriers_per_tenant"`
	QueryShardingTotalShards       int            `yaml:"query_sharding_total_shards" json:"query_sharding_total_shards"`
	QueryShardingMaxShardedQueries int            `yaml:"query_sharding_max_sharded_queries" json:"query_sharding_max_sharded_queries"`
	// Cardinality
	CardinalityAnalysisEnabled                    bool `yaml:"cardinality_analysis_enabled" json:"cardinality_analysis_enabled"`
	LabelNamesAndValuesResultsMaxSizeBytes        int  `yaml:"label_names_and_values_results_max_size_bytes" json:"label_names_and_values_results_max_size_bytes"`
	LabelValuesMaxCardinalityLabelNamesPerRequest int  `yaml:"label_values_max_cardinality_label_names_per_request" json:"label_values_max_cardinality_label_names_per_request"`

	// Ruler defaults and limits.
	RulerEvaluationDelay        model.Duration `yaml:"ruler_evaluation_delay_duration" json:"ruler_evaluation_delay_duration"`
	RulerTenantShardSize        int            `yaml:"ruler_tenant_shard_size" json:"ruler_tenant_shard_size"`
	RulerMaxRulesPerRuleGroup   int            `yaml:"ruler_max_rules_per_rule_group" json:"ruler_max_rules_per_rule_group"`
	RulerMaxRuleGroupsPerTenant int            `yaml:"ruler_max_rule_groups_per_tenant" json:"ruler_max_rule_groups_per_tenant"`

	// Store-gateway.
	StoreGatewayTenantShardSize int `yaml:"store_gateway_tenant_shard_size" json:"store_gateway_tenant_shard_size"`

	// Compactor.
	CompactorBlocksRetentionPeriod model.Duration `yaml:"compactor_blocks_retention_period" json:"compactor_blocks_retention_period"`
	CompactorSplitAndMergeShards   int            `yaml:"compactor_split_and_merge_shards" json:"compactor_split_and_merge_shards"`
	CompactorSplitGroups           int            `yaml:"compactor_split_groups" json:"compactor_split_groups"`
	CompactorTenantShardSize       int            `yaml:"compactor_tenant_shard_size" json:"compactor_tenant_shard_size"`

	// This config doesn't have a CLI flag registered here because they're registered in
	// their own original config struct.
	S3SSEType                 string `yaml:"s3_sse_type" json:"s3_sse_type" doc:"nocli|description=S3 server-side encryption type. Required to enable server-side encryption overrides for a specific tenant. If not set, the default S3 client settings are used."`
	S3SSEKMSKeyID             string `yaml:"s3_sse_kms_key_id" json:"s3_sse_kms_key_id" doc:"nocli|description=S3 server-side encryption KMS Key ID. Ignored if the SSE type override is not set."`
	S3SSEKMSEncryptionContext string `yaml:"s3_sse_kms_encryption_context" json:"s3_sse_kms_encryption_context" doc:"nocli|description=S3 server-side encryption KMS encryption context. If unset and the key ID override is set, the encryption context will not be provided to S3. Ignored if the SSE type override is not set."`

	// Alertmanager.
	AlertmanagerReceiversBlockCIDRNetworks     flagext.CIDRSliceCSV `yaml:"alertmanager_receivers_firewall_block_cidr_networks" json:"alertmanager_receivers_firewall_block_cidr_networks"`
	AlertmanagerReceiversBlockPrivateAddresses bool                 `yaml:"alertmanager_receivers_firewall_block_private_addresses" json:"alertmanager_receivers_firewall_block_private_addresses"`

	NotificationRateLimit               float64                  `yaml:"alertmanager_notification_rate_limit" json:"alertmanager_notification_rate_limit"`
	NotificationRateLimitPerIntegration NotificationRateLimitMap `yaml:"alertmanager_notification_rate_limit_per_integration" json:"alertmanager_notification_rate_limit_per_integration"`

	AlertmanagerMaxConfigSizeBytes             int `yaml:"alertmanager_max_config_size_bytes" json:"alertmanager_max_config_size_bytes"`
	AlertmanagerMaxTemplatesCount              int `yaml:"alertmanager_max_templates_count" json:"alertmanager_max_templates_count"`
	AlertmanagerMaxTemplateSizeBytes           int `yaml:"alertmanager_max_template_size_bytes" json:"alertmanager_max_template_size_bytes"`
	AlertmanagerMaxDispatcherAggregationGroups int `yaml:"alertmanager_max_dispatcher_aggregation_groups" json:"alertmanager_max_dispatcher_aggregation_groups"`
	AlertmanagerMaxAlertsCount                 int `yaml:"alertmanager_max_alerts_count" json:"alertmanager_max_alerts_count"`
	AlertmanagerMaxAlertsSizeBytes             int `yaml:"alertmanager_max_alerts_size_bytes" json:"alertmanager_max_alerts_size_bytes"`

	ForwardingRules ForwardingRules `yaml:"forwarding_rules" json:"forwarding_rules" doc:"nocli|description=Rules based on which the Distributor decides whether a metric should be forwarded to an alternative remote_write API endpoint."`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (l *Limits) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&l.IngestionTenantShardSize, "distributor.ingestion-tenant-shard-size", 0, "The tenant's shard size used by shuffle-sharding. Must be set both on ingesters and distributors. 0 disables shuffle sharding.")
	f.Float64Var(&l.IngestionRate, "distributor.ingestion-rate-limit", 10000, "Per-tenant ingestion rate limit in samples per second.")
	f.IntVar(&l.IngestionBurstSize, "distributor.ingestion-burst-size", 200000, "Per-tenant allowed ingestion burst size (in number of samples).")
	f.BoolVar(&l.AcceptHASamples, "distributor.ha-tracker.enable-for-all-users", false, "Flag to enable, for all tenants, handling of samples with external labels identifying replicas in an HA Prometheus setup.")
	f.StringVar(&l.HAClusterLabel, "distributor.ha-tracker.cluster", "cluster", "Prometheus label to look for in samples to identify a Prometheus HA cluster.")
	f.StringVar(&l.HAReplicaLabel, "distributor.ha-tracker.replica", "__replica__", "Prometheus label to look for in samples to identify a Prometheus HA replica.")
	f.IntVar(&l.HAMaxClusters, "distributor.ha-tracker.max-clusters", 0, "Maximum number of clusters that HA tracker will keep track of for a single tenant. 0 to disable the limit.")
	f.Var(&l.DropLabels, "distributor.drop-label", "This flag can be used to specify label names that to drop during sample ingestion within the distributor and can be repeated in order to drop multiple labels.")
	f.IntVar(&l.MaxLabelNameLength, maxLabelNameLengthFlag, 1024, "Maximum length accepted for label names")
	f.IntVar(&l.MaxLabelValueLength, maxLabelValueLengthFlag, 2048, "Maximum length accepted for label value. This setting also applies to the metric name")
	f.IntVar(&l.MaxLabelNamesPerSeries, maxLabelNamesPerSeriesFlag, 30, "Maximum number of label names per series.")
	f.IntVar(&l.MaxMetadataLength, "validation.max-metadata-length", 1024, "Maximum length accepted for metric metadata. Metadata refers to Metric Name, HELP and UNIT.")
	_ = l.CreationGracePeriod.Set("10m")
	f.Var(&l.CreationGracePeriod, creationGracePeriodFlag, "Controls how far into the future incoming samples are accepted compared to the wall clock. Any sample with timestamp `t` will be rejected if `t > (now + validation.create-grace-period)`.")
	f.BoolVar(&l.EnforceMetadataMetricName, "validation.enforce-metadata-metric-name", true, "Enforce every metadata has a metric name.")

	f.IntVar(&l.MaxGlobalSeriesPerUser, "ingester.max-global-series-per-user", 150000, "The maximum number of active series per tenant, across the cluster before replication. 0 to disable.")
	f.IntVar(&l.MaxGlobalSeriesPerMetric, "ingester.max-global-series-per-metric", 20000, "The maximum number of active series per metric name, across the cluster before replication. 0 to disable.")

	f.IntVar(&l.MaxGlobalMetricsWithMetadataPerUser, "ingester.max-global-metadata-per-user", 0, "The maximum number of active metrics with metadata per tenant, across the cluster. 0 to disable.")
	f.IntVar(&l.MaxGlobalMetadataPerMetric, "ingester.max-global-metadata-per-metric", 0, "The maximum number of metadata per metric, across the cluster. 0 to disable.")
	f.IntVar(&l.MaxGlobalExemplarsPerUser, "ingester.max-global-exemplars-per-user", 0, "The maximum number of exemplars in memory, across the cluster. 0 to disable exemplars ingestion.")
	f.Var(&l.ActiveSeriesCustomTrackersConfig, "ingester.active-series-custom-trackers", "Additional active series metrics, matching the provided matchers. Matchers should be in form <name>:<matcher>, like 'foobar:{foo=\"bar\"}'. Multiple matchers can be provided either providing the flag multiple times or providing multiple semicolon-separated values to a single flag.")

	f.IntVar(&l.MaxChunksPerQuery, "querier.max-fetched-chunks-per-query", 2e6, "Maximum number of chunks that can be fetched in a single query from ingesters and long-term storage. This limit is enforced in the querier, ruler and store-gateway. 0 to disable.")
	f.IntVar(&l.MaxFetchedSeriesPerQuery, "querier.max-fetched-series-per-query", 0, "The maximum number of unique series for which a query can fetch samples from each ingesters and storage. This limit is enforced in the querier and ruler. 0 to disable")
	f.IntVar(&l.MaxFetchedChunkBytesPerQuery, "querier.max-fetched-chunk-bytes-per-query", 0, "The maximum size of all chunks in bytes that a query can fetch from each ingester and storage. This limit is enforced in the querier and ruler. 0 to disable.")
	f.Var(&l.MaxQueryLength, "store.max-query-length", "Limit the query time range (end - start time). This limit is enforced in the query-frontend (on the received query), in the querier (on the query possibly split by the query-frontend) and ruler. 0 to disable.")
	f.Var(&l.MaxQueryLookback, "querier.max-query-lookback", "Limit how long back data (series and metadata) can be queried, up until <lookback> duration ago. This limit is enforced in the query-frontend, querier and ruler. If the requested time range is outside the allowed range, the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.")
	f.IntVar(&l.MaxQueryParallelism, "querier.max-query-parallelism", 14, "Maximum number of split (by time) or partial (by shard) queries that will be scheduled in parallel by the query-frontend for a single input query. This limit is introduced to have a fairer query scheduling and avoid a single query over a large time range saturating all available queriers.")
	f.Var(&l.MaxLabelsQueryLength, "store.max-labels-query-length", "Limit the time range (end - start time) of series, label names and values queries. This limit is enforced in the querier. If the requested time range is outside the allowed range, the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.")
	f.IntVar(&l.LabelNamesAndValuesResultsMaxSizeBytes, "querier.label-names-and-values-results-max-size-bytes", 400*1024*1024, "Maximum size in bytes of distinct label names and values. When querier receives response from ingester, it merges the response with responses from other ingesters. This maximum size limit is applied to the merged(distinct) results. If the limit is reached, an error is returned.")
	f.BoolVar(&l.CardinalityAnalysisEnabled, "querier.cardinality-analysis-enabled", false, "Enables endpoints used for cardinality analysis.")
	f.IntVar(&l.LabelValuesMaxCardinalityLabelNamesPerRequest, "querier.label-values-max-cardinality-label-names-per-request", 100, "Maximum number of label names allowed to be queried in a single /api/v1/cardinality/label_values API call.")
	_ = l.MaxCacheFreshness.Set("1m")
	f.Var(&l.MaxCacheFreshness, "query-frontend.max-cache-freshness", "Most recent allowed cacheable result per-tenant, to prevent caching very recent results that might still be in flux.")
	f.IntVar(&l.MaxQueriersPerTenant, "query-frontend.max-queriers-per-tenant", 0, "Maximum number of queriers that can handle requests for a single tenant. If set to 0 or value higher than number of available queriers, *all* queriers will handle requests for the tenant. Each frontend (or query-scheduler, if used) will select the same set of queriers for the same tenant (given that all queriers are connected to all frontends / query-schedulers). This option only works with queriers connecting to the query-frontend / query-scheduler, not when using downstream URL.")
	f.IntVar(&l.QueryShardingTotalShards, "query-frontend.query-sharding-total-shards", 16, "The amount of shards to use when doing parallelisation via query sharding by tenant. 0 to disable query sharding for tenant. Query sharding implementation will adjust the number of query shards based on compactor shards. This allows querier to not search the blocks which cannot possibly have the series for given query shard.")
	f.IntVar(&l.QueryShardingMaxShardedQueries, "query-frontend.query-sharding-max-sharded-queries", 128, "The max number of sharded queries that can be run for a given received query. 0 to disable limit.")

	f.Var(&l.RulerEvaluationDelay, "ruler.evaluation-delay-duration", "Duration to delay the evaluation of rules to ensure the underlying metrics have been pushed.")
	f.IntVar(&l.RulerTenantShardSize, "ruler.tenant-shard-size", 0, "The tenant's shard size when sharding is used by ruler. Value of 0 disables shuffle sharding for the tenant, and tenant rules will be sharded across all ruler replicas.")
	f.IntVar(&l.RulerMaxRulesPerRuleGroup, "ruler.max-rules-per-rule-group", 20, "Maximum number of rules per rule group per-tenant. 0 to disable.")
	f.IntVar(&l.RulerMaxRuleGroupsPerTenant, "ruler.max-rule-groups-per-tenant", 70, "Maximum number of rule groups per-tenant. 0 to disable.")

	f.Var(&l.CompactorBlocksRetentionPeriod, "compactor.blocks-retention-period", "Delete blocks containing samples older than the specified retention period. 0 to disable.")
	f.IntVar(&l.CompactorSplitAndMergeShards, "compactor.split-and-merge-shards", 0, "The number of shards to use when splitting blocks. 0 to disable splitting.")
	f.IntVar(&l.CompactorSplitGroups, "compactor.split-groups", 1, "Number of groups that blocks for splitting should be grouped into. Each group of blocks is then split separately. Number of output split shards is controlled by -compactor.split-and-merge-shards.")
	f.IntVar(&l.CompactorTenantShardSize, "compactor.compactor-tenant-shard-size", 0, "Max number of compactors that can compact blocks for single tenant. 0 to disable the limit and use all compactors.")

	// Store-gateway.
	f.IntVar(&l.StoreGatewayTenantShardSize, "store-gateway.tenant-shard-size", 0, "The tenant's shard size, used when store-gateway sharding is enabled. Value of 0 disables shuffle sharding for the tenant, that is all tenant blocks are sharded across all store-gateway replicas.")

	// Alertmanager.
	f.Var(&l.AlertmanagerReceiversBlockCIDRNetworks, "alertmanager.receivers-firewall-block-cidr-networks", "Comma-separated list of network CIDRs to block in Alertmanager receiver integrations.")
	f.BoolVar(&l.AlertmanagerReceiversBlockPrivateAddresses, "alertmanager.receivers-firewall-block-private-addresses", false, "True to block private and local addresses in Alertmanager receiver integrations. It blocks private addresses defined by  RFC 1918 (IPv4 addresses) and RFC 4193 (IPv6 addresses), as well as loopback, local unicast and local multicast addresses.")

	f.Float64Var(&l.NotificationRateLimit, "alertmanager.notification-rate-limit", 0, "Per-tenant rate limit for sending notifications from Alertmanager in notifications/sec. 0 = rate limit disabled. Negative value = no notifications are allowed.")

	if l.NotificationRateLimitPerIntegration == nil {
		l.NotificationRateLimitPerIntegration = NotificationRateLimitMap{}
	}
	f.Var(&l.NotificationRateLimitPerIntegration, "alertmanager.notification-rate-limit-per-integration", "Per-integration notification rate limits. Value is a map, where each key is integration name and value is a rate-limit (float). On command line, this map is given in JSON format. Rate limit has the same meaning as -alertmanager.notification-rate-limit, but only applies for specific integration. Allowed integration names: "+strings.Join(allowedIntegrationNames, ", ")+".")
	f.IntVar(&l.AlertmanagerMaxConfigSizeBytes, "alertmanager.max-config-size-bytes", 0, "Maximum size of configuration file for Alertmanager that tenant can upload via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxTemplatesCount, "alertmanager.max-templates-count", 0, "Maximum number of templates in tenant's Alertmanager configuration uploaded via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxTemplateSizeBytes, "alertmanager.max-template-size-bytes", 0, "Maximum size of single template in tenant's Alertmanager configuration uploaded via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxDispatcherAggregationGroups, "alertmanager.max-dispatcher-aggregation-groups", 0, "Maximum number of aggregation groups in Alertmanager's dispatcher that a tenant can have. Each active aggregation group uses single goroutine. When the limit is reached, dispatcher will not dispatch alerts that belong to additional aggregation groups, but existing groups will keep working properly. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxAlertsCount, "alertmanager.max-alerts-count", 0, "Maximum number of alerts that a single tenant can have. Inserting more alerts will fail with a log message and metric increment. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxAlertsSizeBytes, "alertmanager.max-alerts-size-bytes", 0, "Maximum total size of alerts that a single tenant can have, alert size is the sum of the bytes of its labels, annotations and generatorURL. Inserting more alerts will fail with a log message and metric increment. 0 = no limit.")
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *Limits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// We want to set l to the defaults and then overwrite it with the input.
	// To make unmarshal fill the plain data struct rather than calling UnmarshalYAML
	// again, we have to hide it using a type indirection.  See prometheus/config.

	// During startup we wont have a default value so we don't want to overwrite them
	if defaultLimits != nil {
		*l = *defaultLimits
		// Make copy of default limits. Otherwise unmarshalling would modify map in default limits.
		l.copyNotificationIntegrationLimits(defaultLimits.NotificationRateLimitPerIntegration)
	}
	type plain Limits
	return unmarshal((*plain)(l))
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (l *Limits) UnmarshalJSON(data []byte) error {
	// Like the YAML method above, we want to set l to the defaults and then overwrite
	// it with the input. We prevent an infinite loop of calling UnmarshalJSON by hiding
	// behind type indirection.
	if defaultLimits != nil {
		*l = *defaultLimits
		// Make copy of default limits. Otherwise unmarshalling would modify map in default limits.
		l.copyNotificationIntegrationLimits(defaultLimits.NotificationRateLimitPerIntegration)
	}

	type plain Limits
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	return dec.Decode((*plain)(l))
}

func (l *Limits) copyNotificationIntegrationLimits(defaults NotificationRateLimitMap) {
	l.NotificationRateLimitPerIntegration = make(map[string]float64, len(defaults))
	for k, v := range defaults {
		l.NotificationRateLimitPerIntegration[k] = v
	}
}

// When we load YAML from disk, we want the various per-customer limits
// to default to any values specified on the command line, not default
// command line values.  This global contains those values.  I (Tom) cannot
// find a nicer way I'm afraid.
var defaultLimits *Limits

// SetDefaultLimitsForYAMLUnmarshalling sets global default limits, used when loading
// Limits from YAML files. This is used to ensure per-tenant limits are defaulted to
// those values.
func SetDefaultLimitsForYAMLUnmarshalling(defaults Limits) {
	defaultLimits = &defaults
}

// TenantLimits exposes per-tenant limit overrides to various resource usage limits
type TenantLimits interface {
	// ByUserID gets limits specific to a particular tenant or nil if there are none
	ByUserID(userID string) *Limits

	// AllByUserID gets a mapping of all tenant IDs and limits for that user
	AllByUserID() map[string]*Limits
}

// Overrides periodically fetch a set of per-user overrides, and provides convenience
// functions for fetching the correct value.
type Overrides struct {
	defaultLimits *Limits
	tenantLimits  TenantLimits
}

// NewOverrides makes a new Overrides.
func NewOverrides(defaults Limits, tenantLimits TenantLimits) (*Overrides, error) {
	return &Overrides{
		tenantLimits:  tenantLimits,
		defaultLimits: &defaults,
	}, nil
}

// IngestionRate returns the limit on ingester rate (samples per second).
func (o *Overrides) IngestionRate(userID string) float64 {
	return o.getOverridesForUser(userID).IngestionRate
}

// LabelNamesAndValuesResultsMaxSizeBytes returns the maximum size in bytes of distinct label names and values
func (o *Overrides) LabelNamesAndValuesResultsMaxSizeBytes(userID string) int {
	return o.getOverridesForUser(userID).LabelNamesAndValuesResultsMaxSizeBytes
}

func (o *Overrides) CardinalityAnalysisEnabled(userID string) bool {
	return o.getOverridesForUser(userID).CardinalityAnalysisEnabled
}

// LabelValuesMaxCardinalityLabelNamesPerRequest returns the maximum number of label names per cardinality request.
func (o *Overrides) LabelValuesMaxCardinalityLabelNamesPerRequest(userID string) int {
	return o.getOverridesForUser(userID).LabelValuesMaxCardinalityLabelNamesPerRequest
}

// IngestionBurstSize returns the burst size for ingestion rate.
func (o *Overrides) IngestionBurstSize(userID string) int {
	return o.getOverridesForUser(userID).IngestionBurstSize
}

// AcceptHASamples returns whether the distributor should track and accept samples from HA replicas for this user.
func (o *Overrides) AcceptHASamples(userID string) bool {
	return o.getOverridesForUser(userID).AcceptHASamples
}

// HAClusterLabel returns the cluster label to look for when deciding whether to accept a sample from a Prometheus HA replica.
func (o *Overrides) HAClusterLabel(userID string) string {
	return o.getOverridesForUser(userID).HAClusterLabel
}

// HAReplicaLabel returns the replica label to look for when deciding whether to accept a sample from a Prometheus HA replica.
func (o *Overrides) HAReplicaLabel(userID string) string {
	return o.getOverridesForUser(userID).HAReplicaLabel
}

// DropLabels returns the list of labels to be dropped when ingesting HA samples for the user.
func (o *Overrides) DropLabels(userID string) flagext.StringSlice {
	return o.getOverridesForUser(userID).DropLabels
}

// MaxLabelNameLength returns maximum length a label name can be.
func (o *Overrides) MaxLabelNameLength(userID string) int {
	return o.getOverridesForUser(userID).MaxLabelNameLength
}

// MaxLabelValueLength returns maximum length a label value can be. This also is
// the maximum length of a metric name.
func (o *Overrides) MaxLabelValueLength(userID string) int {
	return o.getOverridesForUser(userID).MaxLabelValueLength
}

// MaxLabelNamesPerSeries returns maximum number of label/value pairs timeseries.
func (o *Overrides) MaxLabelNamesPerSeries(userID string) int {
	return o.getOverridesForUser(userID).MaxLabelNamesPerSeries
}

// MaxMetadataLength returns maximum length metadata can be. Metadata refers
// to the Metric Name, HELP and UNIT.
func (o *Overrides) MaxMetadataLength(userID string) int {
	return o.getOverridesForUser(userID).MaxMetadataLength
}

// CreationGracePeriod is misnamed, and actually returns how far into the future
// we should accept samples.
func (o *Overrides) CreationGracePeriod(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).CreationGracePeriod)
}

// MaxGlobalSeriesPerUser returns the maximum number of series a user is allowed to store across the cluster.
func (o *Overrides) MaxGlobalSeriesPerUser(userID string) int {
	return o.getOverridesForUser(userID).MaxGlobalSeriesPerUser
}

// MaxGlobalSeriesPerMetric returns the maximum number of series allowed per metric across the cluster.
func (o *Overrides) MaxGlobalSeriesPerMetric(userID string) int {
	return o.getOverridesForUser(userID).MaxGlobalSeriesPerMetric
}

func (o *Overrides) MaxChunksPerQuery(userID string) int {
	return o.getOverridesForUser(userID).MaxChunksPerQuery
}

// MaxFetchedSeriesPerQuery returns the maximum number of series allowed per query when fetching
// chunks from ingesters and blocks storage.
func (o *Overrides) MaxFetchedSeriesPerQuery(userID string) int {
	return o.getOverridesForUser(userID).MaxFetchedSeriesPerQuery
}

// MaxFetchedChunkBytesPerQuery returns the maximum number of bytes for chunks allowed per query when fetching
// chunks from ingesters and blocks storage.
func (o *Overrides) MaxFetchedChunkBytesPerQuery(userID string) int {
	return o.getOverridesForUser(userID).MaxFetchedChunkBytesPerQuery
}

// MaxQueryLookback returns the max lookback period of queries.
func (o *Overrides) MaxQueryLookback(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).MaxQueryLookback)
}

// MaxQueryLength returns the limit of the length (in time) of a query.
func (o *Overrides) MaxQueryLength(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).MaxQueryLength)
}

// MaxLabelsQueryLength returns the limit of the length (in time) of a label names or values request.
func (o *Overrides) MaxLabelsQueryLength(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).MaxLabelsQueryLength)
}

// MaxCacheFreshness returns the period after which results are cacheable,
// to prevent caching of very recent results.
func (o *Overrides) MaxCacheFreshness(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).MaxCacheFreshness)
}

// MaxQueriersPerUser returns the maximum number of queriers that can handle requests for this user.
func (o *Overrides) MaxQueriersPerUser(userID string) int {
	return o.getOverridesForUser(userID).MaxQueriersPerTenant
}

// MaxQueryParallelism returns the limit to the number of split queries the
// frontend will process in parallel.
func (o *Overrides) MaxQueryParallelism(userID string) int {
	return o.getOverridesForUser(userID).MaxQueryParallelism
}

// QueryShardingTotalShards returns the total amount of shards to use when splitting queries via querysharding
// the frontend. When a query is shardable, each shards will be processed in parallel.
func (o *Overrides) QueryShardingTotalShards(userID string) int {
	return o.getOverridesForUser(userID).QueryShardingTotalShards
}

// QueryShardingMaxShardedQueries returns the max number of sharded queries that can
// be run for a given received query. 0 to disable limit.
func (o *Overrides) QueryShardingMaxShardedQueries(userID string) int {
	return o.getOverridesForUser(userID).QueryShardingMaxShardedQueries
}

// EnforceMetadataMetricName whether to enforce the presence of a metric name on metadata.
func (o *Overrides) EnforceMetadataMetricName(userID string) bool {
	return o.getOverridesForUser(userID).EnforceMetadataMetricName
}

// MaxGlobalMetricsWithMetadataPerUser returns the maximum number of metrics with metadata a user is allowed to store across the cluster.
func (o *Overrides) MaxGlobalMetricsWithMetadataPerUser(userID string) int {
	return o.getOverridesForUser(userID).MaxGlobalMetricsWithMetadataPerUser
}

// MaxGlobalMetadataPerMetric returns the maximum number of metadata allowed per metric across the cluster.
func (o *Overrides) MaxGlobalMetadataPerMetric(userID string) int {
	return o.getOverridesForUser(userID).MaxGlobalMetadataPerMetric
}

// MaxGlobalExemplars returns the maximum number of exemplars held in memory across the cluster.
func (o *Overrides) MaxGlobalExemplarsPerUser(userID string) int {
	return o.getOverridesForUser(userID).MaxGlobalExemplarsPerUser
}

func (o *Overrides) ActiveSeriesCustomTrackersConfig(userID string) activeseries.CustomTrackersConfig {
	return o.getOverridesForUser(userID).ActiveSeriesCustomTrackersConfig
}

// IngestionTenantShardSize returns the ingesters shard size for a given user.
func (o *Overrides) IngestionTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).IngestionTenantShardSize
}

// CompactorTenantShardSize returns number of compactors that this user can use. 0 = all compactors.
func (o *Overrides) CompactorTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).CompactorTenantShardSize
}

// EvaluationDelay returns the rules evaluation delay for a given user.
func (o *Overrides) EvaluationDelay(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).RulerEvaluationDelay)
}

// CompactorBlocksRetentionPeriod returns the retention period for a given user.
func (o *Overrides) CompactorBlocksRetentionPeriod(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).CompactorBlocksRetentionPeriod)
}

// CompactorSplitAndMergeShards returns the number of shards to use when splitting blocks.
func (o *Overrides) CompactorSplitAndMergeShards(userID string) int {
	return o.getOverridesForUser(userID).CompactorSplitAndMergeShards
}

// CompactorSplitGroupsCount returns the number of groups that blocks for splitting should be grouped into.
func (o *Overrides) CompactorSplitGroups(userID string) int {
	return o.getOverridesForUser(userID).CompactorSplitGroups
}

// MetricRelabelConfigs returns the metric relabel configs for a given user.
func (o *Overrides) MetricRelabelConfigs(userID string) []*relabel.Config {
	return o.getOverridesForUser(userID).MetricRelabelConfigs
}

// RulerTenantShardSize returns shard size (number of rulers) used by this tenant when using shuffle-sharding strategy.
func (o *Overrides) RulerTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).RulerTenantShardSize
}

// RulerMaxRulesPerRuleGroup returns the maximum number of rules per rule group for a given user.
func (o *Overrides) RulerMaxRulesPerRuleGroup(userID string) int {
	return o.getOverridesForUser(userID).RulerMaxRulesPerRuleGroup
}

// RulerMaxRuleGroupsPerTenant returns the maximum number of rule groups for a given user.
func (o *Overrides) RulerMaxRuleGroupsPerTenant(userID string) int {
	return o.getOverridesForUser(userID).RulerMaxRuleGroupsPerTenant
}

// StoreGatewayTenantShardSize returns the store-gateway shard size for a given user.
func (o *Overrides) StoreGatewayTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).StoreGatewayTenantShardSize
}

// MaxHAClusters returns maximum number of clusters that HA tracker will track for a user.
func (o *Overrides) MaxHAClusters(user string) int {
	return o.getOverridesForUser(user).HAMaxClusters
}

// S3SSEType returns the per-tenant S3 SSE type.
func (o *Overrides) S3SSEType(user string) string {
	return o.getOverridesForUser(user).S3SSEType
}

// S3SSEKMSKeyID returns the per-tenant S3 KMS-SSE key id.
func (o *Overrides) S3SSEKMSKeyID(user string) string {
	return o.getOverridesForUser(user).S3SSEKMSKeyID
}

// S3SSEKMSEncryptionContext returns the per-tenant S3 KMS-SSE encryption context.
func (o *Overrides) S3SSEKMSEncryptionContext(user string) string {
	return o.getOverridesForUser(user).S3SSEKMSEncryptionContext
}

// AlertmanagerReceiversBlockCIDRNetworks returns the list of network CIDRs that should be blocked
// in the Alertmanager receivers for the given user.
func (o *Overrides) AlertmanagerReceiversBlockCIDRNetworks(user string) []flagext.CIDR {
	return o.getOverridesForUser(user).AlertmanagerReceiversBlockCIDRNetworks
}

// AlertmanagerReceiversBlockPrivateAddresses returns true if private addresses should be blocked
// in the Alertmanager receivers for the given user.
func (o *Overrides) AlertmanagerReceiversBlockPrivateAddresses(user string) bool {
	return o.getOverridesForUser(user).AlertmanagerReceiversBlockPrivateAddresses
}

// Notification limits are special. Limits are returned in following order:
// 1. per-tenant limits for given integration
// 2. default limits for given integration
// 3. per-tenant limits
// 4. default limits
func (o *Overrides) getNotificationLimitForUser(user, integration string) float64 {
	u := o.getOverridesForUser(user)
	if n, ok := u.NotificationRateLimitPerIntegration[integration]; ok {
		return n
	}

	return u.NotificationRateLimit
}

func (o *Overrides) NotificationRateLimit(user string, integration string) rate.Limit {
	l := o.getNotificationLimitForUser(user, integration)
	if l == 0 || math.IsInf(l, 1) {
		return rate.Inf // No rate limit.
	}

	if l < 0 {
		l = 0 // No notifications will be sent.
	}
	return rate.Limit(l)
}

const maxInt = int(^uint(0) >> 1)

func (o *Overrides) NotificationBurstSize(user string, integration string) int {
	// Burst size is computed from rate limit. Rate limit is already normalized to [0, +inf), where 0 means disabled.
	l := o.NotificationRateLimit(user, integration)
	if l == 0 {
		return 0
	}

	// floats can be larger than max int. This also handles case where l == rate.Inf.
	if float64(l) >= float64(maxInt) {
		return maxInt
	}

	// For values between (0, 1), allow single notification per second (every 1/limit seconds).
	if l < 1 {
		return 1
	}

	return int(l)
}

func (o *Overrides) AlertmanagerMaxConfigSize(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxConfigSizeBytes
}

func (o *Overrides) AlertmanagerMaxTemplatesCount(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxTemplatesCount
}

func (o *Overrides) AlertmanagerMaxTemplateSize(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxTemplateSizeBytes
}

func (o *Overrides) AlertmanagerMaxDispatcherAggregationGroups(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxDispatcherAggregationGroups
}

func (o *Overrides) AlertmanagerMaxAlertsCount(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxAlertsCount
}

func (o *Overrides) AlertmanagerMaxAlertsSizeBytes(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxAlertsSizeBytes
}

func (o *Overrides) ForwardingRules(user string) ForwardingRules {
	return o.getOverridesForUser(user).ForwardingRules
}

func (o *Overrides) getOverridesForUser(userID string) *Limits {
	if o.tenantLimits != nil {
		l := o.tenantLimits.ByUserID(userID)
		if l != nil {
			return l
		}
	}
	return o.defaultLimits
}

// SmallestPositiveIntPerTenant is returning the minimal positive value of the
// supplied limit function for all given tenants.
func SmallestPositiveIntPerTenant(tenantIDs []string, f func(string) int) int {
	var result *int
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if result == nil || v < *result {
			result = &v
		}
	}
	if result == nil {
		return 0
	}
	return *result
}

// SmallestPositiveNonZeroIntPerTenant is returning the minimal positive and
// non-zero value of the supplied limit function for all given tenants. In many
// limits a value of 0 means unlimted so the method will return 0 only if all
// inputs have a limit of 0 or an empty tenant list is given.
func SmallestPositiveNonZeroIntPerTenant(tenantIDs []string, f func(string) int) int {
	var result *int
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if v > 0 && (result == nil || v < *result) {
			result = &v
		}
	}
	if result == nil {
		return 0
	}
	return *result
}

// SmallestPositiveNonZeroDurationPerTenant is returning the minimal positive
// and non-zero value of the supplied limit function for all given tenants. In
// many limits a value of 0 means unlimted so the method will return 0 only if
// all inputs have a limit of 0 or an empty tenant list is given.
func SmallestPositiveNonZeroDurationPerTenant(tenantIDs []string, f func(string) time.Duration) time.Duration {
	var result *time.Duration
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if v > 0 && (result == nil || v < *result) {
			result = &v
		}
	}
	if result == nil {
		return 0
	}
	return *result
}

// MaxDurationPerTenant is returning the maximum duration per tenant. Without
// tenants given it will return a time.Duration(0).
func MaxDurationPerTenant(tenantIDs []string, f func(string) time.Duration) time.Duration {
	result := time.Duration(0)
	for _, tenantID := range tenantIDs {
		v := f(tenantID)
		if v > result {
			result = v
		}
	}
	return result
}
