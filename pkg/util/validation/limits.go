// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/limits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"

	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util"
)

const (
	MaxSeriesPerMetricFlag                    = "ingester.max-global-series-per-metric"
	MaxMetadataPerMetricFlag                  = "ingester.max-global-metadata-per-metric"
	MaxSeriesPerUserFlag                      = "ingester.max-global-series-per-user"
	MaxMetadataPerUserFlag                    = "ingester.max-global-metadata-per-user"
	MaxChunksPerQueryFlag                     = "querier.max-fetched-chunks-per-query"
	MaxChunkBytesPerQueryFlag                 = "querier.max-fetched-chunk-bytes-per-query"
	MaxSeriesPerQueryFlag                     = "querier.max-fetched-series-per-query"
	MaxEstimatedChunksPerQueryMultiplierFlag  = "querier.max-estimated-fetched-chunks-per-query-multiplier"
	MaxEstimatedMemoryConsumptionPerQueryFlag = "querier.max-estimated-memory-consumption-per-query"
	MaxLabelNamesPerSeriesFlag                = "validation.max-label-names-per-series"
	MaxLabelNamesPerInfoSeriesFlag            = "validation.max-label-names-per-info-series"
	MaxLabelNameLengthFlag                    = "validation.max-length-label-name"
	MaxLabelValueLengthFlag                   = "validation.max-length-label-value"
	MaxMetadataLengthFlag                     = "validation.max-metadata-length"
	maxNativeHistogramBucketsFlag             = "validation.max-native-histogram-buckets"
	ReduceNativeHistogramOverMaxBucketsFlag   = "validation.reduce-native-histogram-over-max-buckets"
	CreationGracePeriodFlag                   = "validation.create-grace-period"
	PastGracePeriodFlag                       = "validation.past-grace-period"
	MaxPartialQueryLengthFlag                 = "querier.max-partial-query-length"
	MaxTotalQueryLengthFlag                   = "query-frontend.max-total-query-length"
	MaxQueryExpressionSizeBytesFlag           = "query-frontend.max-query-expression-size-bytes"
	RequestRateFlag                           = "distributor.request-rate-limit"
	RequestBurstSizeFlag                      = "distributor.request-burst-size"
	IngestionRateFlag                         = "distributor.ingestion-rate-limit"
	IngestionBurstSizeFlag                    = "distributor.ingestion-burst-size"
	IngestionBurstFactorFlag                  = "distributor.ingestion-burst-factor"
	HATrackerMaxClustersFlag                  = "distributor.ha-tracker.max-clusters"
	resultsCacheTTLFlag                       = "query-frontend.results-cache-ttl"
	resultsCacheTTLForOutOfOrderWindowFlag    = "query-frontend.results-cache-ttl-for-out-of-order-time-window"
	alignQueriesWithStepFlag                  = "query-frontend.align-queries-with-step"
	QueryIngestersWithinFlag                  = "querier.query-ingesters-within"
	AlertmanagerMaxGrafanaConfigSizeFlag      = "alertmanager.max-grafana-config-size-bytes"
	AlertmanagerMaxGrafanaStateSizeFlag       = "alertmanager.max-grafana-state-size-bytes"

	// MinCompactorPartialBlockDeletionDelay is the minimum partial blocks deletion delay that can be configured in Mimir.
	MinCompactorPartialBlockDeletionDelay = 4 * time.Hour
)

var (
	errInvalidIngestStorageReadConsistency         = fmt.Errorf("invalid ingest storage read consistency (supported values: %s)", strings.Join(api.ReadConsistencies, ", "))
	errInvalidMaxEstimatedChunksPerQueryMultiplier = errors.New("invalid value for -" + MaxEstimatedChunksPerQueryMultiplierFlag + ": must be 0 or greater than or equal to 1")
)

// LimitError is a marker interface for the errors that do not comply with the specified limits.
type LimitError interface {
	error
	limitError()
}

type limitErr string

// limitErr implements error and LimitError interfaces
func (e limitErr) Error() string {
	return string(e)
}

// limitErr implements LimitError interface
func (e limitErr) limitError() {}

func NewLimitError(msg string) LimitError {
	return limitErr(msg)
}

func IsLimitError(err error) bool {
	var limitErr LimitError
	return errors.As(err, &limitErr)
}

// Limits describe all the limits for users; can be used to describe global default
// limits via flags, or per-user limits via yaml config.
type Limits struct {
	// Distributor enforced limits.
	RequestRate                                 float64             `yaml:"request_rate" json:"request_rate"`
	RequestBurstSize                            int                 `yaml:"request_burst_size" json:"request_burst_size"`
	IngestionRate                               float64             `yaml:"ingestion_rate" json:"ingestion_rate"`
	IngestionBurstSize                          int                 `yaml:"ingestion_burst_size" json:"ingestion_burst_size"`
	IngestionBurstFactor                        float64             `yaml:"ingestion_burst_factor" json:"ingestion_burst_factor" category:"experimental"`
	AcceptHASamples                             bool                `yaml:"accept_ha_samples" json:"accept_ha_samples"`
	HAClusterLabel                              string              `yaml:"ha_cluster_label" json:"ha_cluster_label"`
	HAReplicaLabel                              string              `yaml:"ha_replica_label" json:"ha_replica_label"`
	HAMaxClusters                               int                 `yaml:"ha_max_clusters" json:"ha_max_clusters"`
	DropLabels                                  flagext.StringSlice `yaml:"drop_labels" json:"drop_labels" category:"advanced"`
	MaxLabelNameLength                          int                 `yaml:"max_label_name_length" json:"max_label_name_length"`
	MaxLabelValueLength                         int                 `yaml:"max_label_value_length" json:"max_label_value_length"`
	MaxLabelNamesPerSeries                      int                 `yaml:"max_label_names_per_series" json:"max_label_names_per_series"`
	MaxLabelNamesPerInfoSeries                  int                 `yaml:"max_label_names_per_info_series" json:"max_label_names_per_info_series"`
	MaxMetadataLength                           int                 `yaml:"max_metadata_length" json:"max_metadata_length"`
	MaxNativeHistogramBuckets                   int                 `yaml:"max_native_histogram_buckets" json:"max_native_histogram_buckets"`
	MaxExemplarsPerSeriesPerRequest             int                 `yaml:"max_exemplars_per_series_per_request" json:"max_exemplars_per_series_per_request" category:"experimental"`
	ReduceNativeHistogramOverMaxBuckets         bool                `yaml:"reduce_native_histogram_over_max_buckets" json:"reduce_native_histogram_over_max_buckets"`
	CreationGracePeriod                         model.Duration      `yaml:"creation_grace_period" json:"creation_grace_period" category:"advanced"`
	PastGracePeriod                             model.Duration      `yaml:"past_grace_period" json:"past_grace_period" category:"advanced"`
	EnforceMetadataMetricName                   bool                `yaml:"enforce_metadata_metric_name" json:"enforce_metadata_metric_name" category:"advanced"`
	IngestionTenantShardSize                    int                 `yaml:"ingestion_tenant_shard_size" json:"ingestion_tenant_shard_size"`
	MetricRelabelConfigs                        []*relabel.Config   `yaml:"metric_relabel_configs,omitempty" json:"metric_relabel_configs,omitempty" doc:"nocli|description=List of metric relabel configurations. Note that in most situations, it is more effective to use metrics relabeling directly in the Prometheus server, e.g. remote_write.write_relabel_configs. Labels available during the relabeling phase and cleaned afterwards: __meta_tenant_id" category:"experimental"`
	MetricRelabelingEnabled                     bool                `yaml:"metric_relabeling_enabled" json:"metric_relabeling_enabled" category:"experimental"`
	ServiceOverloadStatusCodeOnRateLimitEnabled bool                `yaml:"service_overload_status_code_on_rate_limit_enabled" json:"service_overload_status_code_on_rate_limit_enabled" category:"experimental"`
	IngestionArtificialDelay                    model.Duration      `yaml:"ingestion_artificial_delay" json:"ingestion_artificial_delay" category:"experimental" doc:"hidden"`
	// Ingester enforced limits.
	// Series
	MaxGlobalSeriesPerUser   int `yaml:"max_global_series_per_user" json:"max_global_series_per_user"`
	MaxGlobalSeriesPerMetric int `yaml:"max_global_series_per_metric" json:"max_global_series_per_metric"`
	// Metadata
	MaxGlobalMetricsWithMetadataPerUser int `yaml:"max_global_metadata_per_user" json:"max_global_metadata_per_user"`
	MaxGlobalMetadataPerMetric          int `yaml:"max_global_metadata_per_metric" json:"max_global_metadata_per_metric"`
	// Exemplars
	MaxGlobalExemplarsPerUser int  `yaml:"max_global_exemplars_per_user" json:"max_global_exemplars_per_user" category:"experimental"`
	IgnoreOOOExemplars        bool `yaml:"ignore_ooo_exemplars" json:"ignore_ooo_exemplars" category:"experimental"`
	// Native histograms
	NativeHistogramsIngestionEnabled bool `yaml:"native_histograms_ingestion_enabled" json:"native_histograms_ingestion_enabled" category:"experimental"`
	// OOO native histograms
	OOONativeHistogramsIngestionEnabled bool `yaml:"ooo_native_histograms_ingestion_enabled" json:"ooo_native_histograms_ingestion_enabled" category:"experimental"`
	// Active series custom trackers
	ActiveSeriesCustomTrackersConfig asmodel.CustomTrackersConfig `yaml:"active_series_custom_trackers" json:"active_series_custom_trackers" doc:"description=Additional custom trackers for active metrics. If there are active series matching a provided matcher (map value), the count will be exposed in the custom trackers metric labeled using the tracker name (map key). Zero valued counts are not exposed (and removed when they go back to zero)." category:"advanced"`
	// Max allowed time window for out-of-order samples.
	OutOfOrderTimeWindow                 model.Duration `yaml:"out_of_order_time_window" json:"out_of_order_time_window" category:"experimental"`
	OutOfOrderBlocksExternalLabelEnabled bool           `yaml:"out_of_order_blocks_external_label_enabled" json:"out_of_order_blocks_external_label_enabled" category:"experimental"`

	// User defined label to give the option of subdividing specific metrics by another label
	SeparateMetricsGroupLabel string `yaml:"separate_metrics_group_label" json:"separate_metrics_group_label" category:"experimental"`

	// Querier enforced limits.
	MaxChunksPerQuery                     int            `yaml:"max_fetched_chunks_per_query" json:"max_fetched_chunks_per_query"`
	MaxEstimatedChunksPerQueryMultiplier  float64        `yaml:"max_estimated_fetched_chunks_per_query_multiplier" json:"max_estimated_fetched_chunks_per_query_multiplier" category:"experimental"`
	MaxFetchedSeriesPerQuery              int            `yaml:"max_fetched_series_per_query" json:"max_fetched_series_per_query"`
	MaxFetchedChunkBytesPerQuery          int            `yaml:"max_fetched_chunk_bytes_per_query" json:"max_fetched_chunk_bytes_per_query"`
	MaxEstimatedMemoryConsumptionPerQuery uint64         `yaml:"max_estimated_memory_consumption_per_query" json:"max_estimated_memory_consumption_per_query" category:"experimental"`
	MaxQueryLookback                      model.Duration `yaml:"max_query_lookback" json:"max_query_lookback"`
	MaxPartialQueryLength                 model.Duration `yaml:"max_partial_query_length" json:"max_partial_query_length"`
	MaxQueryParallelism                   int            `yaml:"max_query_parallelism" json:"max_query_parallelism"`
	MaxLabelsQueryLength                  model.Duration `yaml:"max_labels_query_length" json:"max_labels_query_length"`
	MaxCacheFreshness                     model.Duration `yaml:"max_cache_freshness" json:"max_cache_freshness" category:"advanced"`
	MaxQueriersPerTenant                  int            `yaml:"max_queriers_per_tenant" json:"max_queriers_per_tenant"`
	QueryShardingTotalShards              int            `yaml:"query_sharding_total_shards" json:"query_sharding_total_shards"`
	QueryShardingMaxShardedQueries        int            `yaml:"query_sharding_max_sharded_queries" json:"query_sharding_max_sharded_queries"`
	QueryShardingMaxRegexpSizeBytes       int            `yaml:"query_sharding_max_regexp_size_bytes" json:"query_sharding_max_regexp_size_bytes"`
	SplitInstantQueriesByInterval         model.Duration `yaml:"split_instant_queries_by_interval" json:"split_instant_queries_by_interval" category:"experimental"`
	QueryIngestersWithin                  model.Duration `yaml:"query_ingesters_within" json:"query_ingesters_within" category:"advanced"`

	// Query-frontend limits.
	MaxTotalQueryLength                    model.Duration         `yaml:"max_total_query_length" json:"max_total_query_length"`
	ResultsCacheTTL                        model.Duration         `yaml:"results_cache_ttl" json:"results_cache_ttl"`
	ResultsCacheTTLForOutOfOrderTimeWindow model.Duration         `yaml:"results_cache_ttl_for_out_of_order_time_window" json:"results_cache_ttl_for_out_of_order_time_window"`
	ResultsCacheTTLForCardinalityQuery     model.Duration         `yaml:"results_cache_ttl_for_cardinality_query" json:"results_cache_ttl_for_cardinality_query"`
	ResultsCacheTTLForLabelsQuery          model.Duration         `yaml:"results_cache_ttl_for_labels_query" json:"results_cache_ttl_for_labels_query"`
	ResultsCacheTTLForErrors               model.Duration         `yaml:"results_cache_ttl_for_errors" json:"results_cache_ttl_for_errors" category:"experimental"`
	ResultsCacheForUnalignedQueryEnabled   bool                   `yaml:"cache_unaligned_requests" json:"cache_unaligned_requests" category:"advanced"`
	MaxQueryExpressionSizeBytes            int                    `yaml:"max_query_expression_size_bytes" json:"max_query_expression_size_bytes"`
	BlockedQueries                         []*BlockedQuery        `yaml:"blocked_queries,omitempty" json:"blocked_queries,omitempty" doc:"nocli|description=List of queries to block." category:"experimental"`
	AlignQueriesWithStep                   bool                   `yaml:"align_queries_with_step" json:"align_queries_with_step"`
	EnabledPromQLExperimentalFunctions     flagext.StringSliceCSV `yaml:"enabled_promql_experimental_functions" json:"enabled_promql_experimental_functions" category:"experimental"`

	// Cardinality
	CardinalityAnalysisEnabled                    bool `yaml:"cardinality_analysis_enabled" json:"cardinality_analysis_enabled"`
	LabelNamesAndValuesResultsMaxSizeBytes        int  `yaml:"label_names_and_values_results_max_size_bytes" json:"label_names_and_values_results_max_size_bytes"`
	LabelValuesMaxCardinalityLabelNamesPerRequest int  `yaml:"label_values_max_cardinality_label_names_per_request" json:"label_values_max_cardinality_label_names_per_request"`
	ActiveSeriesResultsMaxSizeBytes               int  `yaml:"active_series_results_max_size_bytes" json:"active_series_results_max_size_bytes" category:"experimental"`

	// Ruler defaults and limits.
	RulerEvaluationDelay                       model.Duration         `yaml:"ruler_evaluation_delay_duration" json:"ruler_evaluation_delay_duration"`
	RulerTenantShardSize                       int                    `yaml:"ruler_tenant_shard_size" json:"ruler_tenant_shard_size"`
	RulerMaxRulesPerRuleGroup                  int                    `yaml:"ruler_max_rules_per_rule_group" json:"ruler_max_rules_per_rule_group"`
	RulerMaxRuleGroupsPerTenant                int                    `yaml:"ruler_max_rule_groups_per_tenant" json:"ruler_max_rule_groups_per_tenant"`
	RulerRecordingRulesEvaluationEnabled       bool                   `yaml:"ruler_recording_rules_evaluation_enabled" json:"ruler_recording_rules_evaluation_enabled"`
	RulerAlertingRulesEvaluationEnabled        bool                   `yaml:"ruler_alerting_rules_evaluation_enabled" json:"ruler_alerting_rules_evaluation_enabled"`
	RulerSyncRulesOnChangesEnabled             bool                   `yaml:"ruler_sync_rules_on_changes_enabled" json:"ruler_sync_rules_on_changes_enabled" category:"advanced"`
	RulerMaxRulesPerRuleGroupByNamespace       LimitsMap[int]         `yaml:"ruler_max_rules_per_rule_group_by_namespace" json:"ruler_max_rules_per_rule_group_by_namespace" category:"experimental"`
	RulerMaxRuleGroupsPerTenantByNamespace     LimitsMap[int]         `yaml:"ruler_max_rule_groups_per_tenant_by_namespace" json:"ruler_max_rule_groups_per_tenant_by_namespace" category:"experimental"`
	RulerProtectedNamespaces                   flagext.StringSliceCSV `yaml:"ruler_protected_namespaces" json:"ruler_protected_namespaces" category:"experimental"`
	RulerMaxRuleEvaluationConcurrencyPerTenant int64                  `yaml:"ruler_max_rule_evaluation_concurrency_per_tenant" json:"ruler_max_rule_evaluation_concurrency_per_tenant" category:"experimental"`

	// Store-gateway.
	StoreGatewayTenantShardSize int `yaml:"store_gateway_tenant_shard_size" json:"store_gateway_tenant_shard_size"`

	// Compactor.
	CompactorBlocksRetentionPeriod        model.Duration `yaml:"compactor_blocks_retention_period" json:"compactor_blocks_retention_period"`
	CompactorSplitAndMergeShards          int            `yaml:"compactor_split_and_merge_shards" json:"compactor_split_and_merge_shards"`
	CompactorSplitGroups                  int            `yaml:"compactor_split_groups" json:"compactor_split_groups"`
	CompactorTenantShardSize              int            `yaml:"compactor_tenant_shard_size" json:"compactor_tenant_shard_size"`
	CompactorPartialBlockDeletionDelay    model.Duration `yaml:"compactor_partial_block_deletion_delay" json:"compactor_partial_block_deletion_delay"`
	CompactorBlockUploadEnabled           bool           `yaml:"compactor_block_upload_enabled" json:"compactor_block_upload_enabled"`
	CompactorBlockUploadValidationEnabled bool           `yaml:"compactor_block_upload_validation_enabled" json:"compactor_block_upload_validation_enabled"`
	CompactorBlockUploadVerifyChunks      bool           `yaml:"compactor_block_upload_verify_chunks" json:"compactor_block_upload_verify_chunks"`
	CompactorBlockUploadMaxBlockSizeBytes int64          `yaml:"compactor_block_upload_max_block_size_bytes" json:"compactor_block_upload_max_block_size_bytes" category:"advanced"`
	CompactorInMemoryTenantMetaCacheSize  int            `yaml:"compactor_in_memory_tenant_meta_cache_size" json:"compactor_in_memory_tenant_meta_cache_size" category:"experimental" doc:"hidden"`

	// This config doesn't have a CLI flag registered here because they're registered in
	// their own original config struct.
	S3SSEType                 string `yaml:"s3_sse_type" json:"s3_sse_type" doc:"nocli|description=S3 server-side encryption type. Required to enable server-side encryption overrides for a specific tenant. If not set, the default S3 client settings are used."`
	S3SSEKMSKeyID             string `yaml:"s3_sse_kms_key_id" json:"s3_sse_kms_key_id" doc:"nocli|description=S3 server-side encryption KMS Key ID. Ignored if the SSE type override is not set."`
	S3SSEKMSEncryptionContext string `yaml:"s3_sse_kms_encryption_context" json:"s3_sse_kms_encryption_context" doc:"nocli|description=S3 server-side encryption KMS encryption context. If unset and the key ID override is set, the encryption context will not be provided to S3. Ignored if the SSE type override is not set."`

	// Alertmanager.
	AlertmanagerReceiversBlockCIDRNetworks     flagext.CIDRSliceCSV `yaml:"alertmanager_receivers_firewall_block_cidr_networks" json:"alertmanager_receivers_firewall_block_cidr_networks"`
	AlertmanagerReceiversBlockPrivateAddresses bool                 `yaml:"alertmanager_receivers_firewall_block_private_addresses" json:"alertmanager_receivers_firewall_block_private_addresses"`

	NotificationRateLimit               float64            `yaml:"alertmanager_notification_rate_limit" json:"alertmanager_notification_rate_limit"`
	NotificationRateLimitPerIntegration LimitsMap[float64] `yaml:"alertmanager_notification_rate_limit_per_integration" json:"alertmanager_notification_rate_limit_per_integration"`

	AlertmanagerMaxGrafanaConfigSizeBytes      flagext.Bytes `yaml:"alertmanager_max_grafana_config_size_bytes" json:"alertmanager_max_grafana_config_size_bytes"`
	AlertmanagerMaxConfigSizeBytes             int           `yaml:"alertmanager_max_config_size_bytes" json:"alertmanager_max_config_size_bytes"`
	AlertmanagerMaxGrafanaStateSizeBytes       flagext.Bytes `yaml:"alertmanager_max_grafana_state_size_bytes" json:"alertmanager_max_grafana_state_size_bytes"`
	AlertmanagerMaxSilencesCount               int           `yaml:"alertmanager_max_silences_count" json:"alertmanager_max_silences_count"`
	AlertmanagerMaxSilenceSizeBytes            int           `yaml:"alertmanager_max_silence_size_bytes" json:"alertmanager_max_silence_size_bytes"`
	AlertmanagerMaxTemplatesCount              int           `yaml:"alertmanager_max_templates_count" json:"alertmanager_max_templates_count"`
	AlertmanagerMaxTemplateSizeBytes           int           `yaml:"alertmanager_max_template_size_bytes" json:"alertmanager_max_template_size_bytes"`
	AlertmanagerMaxDispatcherAggregationGroups int           `yaml:"alertmanager_max_dispatcher_aggregation_groups" json:"alertmanager_max_dispatcher_aggregation_groups"`
	AlertmanagerMaxAlertsCount                 int           `yaml:"alertmanager_max_alerts_count" json:"alertmanager_max_alerts_count"`
	AlertmanagerMaxAlertsSizeBytes             int           `yaml:"alertmanager_max_alerts_size_bytes" json:"alertmanager_max_alerts_size_bytes"`

	// OpenTelemetry
	OTelMetricSuffixesEnabled                bool                   `yaml:"otel_metric_suffixes_enabled" json:"otel_metric_suffixes_enabled" category:"advanced"`
	OTelCreatedTimestampZeroIngestionEnabled bool                   `yaml:"otel_created_timestamp_zero_ingestion_enabled" json:"otel_created_timestamp_zero_ingestion_enabled" category:"experimental"`
	PromoteOTelResourceAttributes            flagext.StringSliceCSV `yaml:"promote_otel_resource_attributes" json:"promote_otel_resource_attributes" category:"experimental"`
	OTelKeepIdentifyingResourceAttributes    bool                   `yaml:"otel_keep_identifying_resource_attributes" json:"otel_keep_identifying_resource_attributes" category:"experimental"`

	// Ingest storage.
	IngestStorageReadConsistency       string `yaml:"ingest_storage_read_consistency" json:"ingest_storage_read_consistency" category:"experimental"`
	IngestionPartitionsTenantShardSize int    `yaml:"ingestion_partitions_tenant_shard_size" json:"ingestion_partitions_tenant_shard_size" category:"experimental"`

	extensions map[string]interface{}
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (l *Limits) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&l.IngestionTenantShardSize, "distributor.ingestion-tenant-shard-size", 0, "The tenant's shard size used by shuffle-sharding. This value is the total size of the shard (ie. it is not the number of ingesters in the shard per zone, but the number of ingesters in the shard across all zones, if zone-awareness is enabled). Must be set both on ingesters and distributors. 0 disables shuffle sharding.")
	f.Float64Var(&l.RequestRate, RequestRateFlag, 0, "Per-tenant push request rate limit in requests per second. 0 to disable.")
	f.IntVar(&l.RequestBurstSize, RequestBurstSizeFlag, 0, "Per-tenant allowed push request burst size. 0 to disable.")
	f.Float64Var(&l.IngestionRate, IngestionRateFlag, 10000, "Per-tenant ingestion rate limit in samples per second.")
	f.IntVar(&l.IngestionBurstSize, IngestionBurstSizeFlag, 200000, "Per-tenant allowed ingestion burst size (in number of samples).")
	f.Float64Var(&l.IngestionBurstFactor, IngestionBurstFactorFlag, 0, "Per-tenant burst factor which is the maximum burst size allowed as a multiple of the per-tenant ingestion rate, this burst-factor must be greater than or equal to 1. If this is set it will override the ingestion-burst-size option.")
	f.BoolVar(&l.AcceptHASamples, "distributor.ha-tracker.enable-for-all-users", false, "Flag to enable, for all tenants, handling of samples with external labels identifying replicas in an HA Prometheus setup.")
	f.StringVar(&l.HAClusterLabel, "distributor.ha-tracker.cluster", "cluster", "Prometheus label to look for in samples to identify a Prometheus HA cluster.")
	f.StringVar(&l.HAReplicaLabel, "distributor.ha-tracker.replica", "__replica__", "Prometheus label to look for in samples to identify a Prometheus HA replica.")
	f.IntVar(&l.HAMaxClusters, HATrackerMaxClustersFlag, 100, "Maximum number of clusters that HA tracker will keep track of for a single tenant. 0 to disable the limit.")
	f.Var(&l.DropLabels, "distributor.drop-label", "This flag can be used to specify label names that to drop during sample ingestion within the distributor and can be repeated in order to drop multiple labels.")
	f.IntVar(&l.MaxLabelNameLength, MaxLabelNameLengthFlag, 1024, "Maximum length accepted for label names")
	f.IntVar(&l.MaxLabelValueLength, MaxLabelValueLengthFlag, 2048, "Maximum length accepted for label value. This setting also applies to the metric name")
	f.IntVar(&l.MaxLabelNamesPerSeries, MaxLabelNamesPerSeriesFlag, 30, "Maximum number of label names per series.")
	f.IntVar(&l.MaxLabelNamesPerInfoSeries, MaxLabelNamesPerInfoSeriesFlag, 80, "Maximum number of label names per info series. Has no effect if less than the value of the maximum number of label names per series option (-"+MaxLabelNamesPerSeriesFlag+")")
	f.IntVar(&l.MaxMetadataLength, MaxMetadataLengthFlag, 1024, "Maximum length accepted for metric metadata. Metadata refers to Metric Name, HELP and UNIT. Longer metadata is dropped except for HELP which is truncated.")
	f.IntVar(&l.MaxNativeHistogramBuckets, maxNativeHistogramBucketsFlag, 0, "Maximum number of buckets per native histogram sample. 0 to disable the limit.")
	f.IntVar(&l.MaxExemplarsPerSeriesPerRequest, "distributor.max-exemplars-per-series-per-request", 0, "Maximum number of exemplars per series per request. 0 to disable limit in request. The exceeding exemplars are dropped.")
	f.BoolVar(&l.ReduceNativeHistogramOverMaxBuckets, ReduceNativeHistogramOverMaxBucketsFlag, true, "Whether to reduce or reject native histogram samples with more buckets than the configured limit.")
	_ = l.CreationGracePeriod.Set("10m")
	f.Var(&l.CreationGracePeriod, CreationGracePeriodFlag, "Controls how far into the future incoming samples and exemplars are accepted compared to the wall clock. Any sample or exemplar will be rejected if its timestamp is greater than '(now + creation_grace_period)'. This configuration is enforced in the distributor and ingester.")
	f.Var(&l.PastGracePeriod, PastGracePeriodFlag, "Controls how far into the past incoming samples and exemplars are accepted compared to the wall clock. Any sample or exemplar will be rejected if its timestamp is lower than '(now - OOO window - past_grace_period)'. This configuration is enforced in the distributor and ingester. 0 to disable.")
	f.BoolVar(&l.EnforceMetadataMetricName, "validation.enforce-metadata-metric-name", true, "Enforce every metadata has a metric name.")
	f.BoolVar(&l.MetricRelabelingEnabled, "distributor.metric-relabeling-enabled", true, "Enable metric relabeling for the tenant. This configuration option can be used to forcefully disable metric relabeling on a per-tenant basis.")
	f.BoolVar(&l.ServiceOverloadStatusCodeOnRateLimitEnabled, "distributor.service-overload-status-code-on-rate-limit-enabled", false, "If enabled, rate limit errors will be reported to the client with HTTP status code 529 (Service is overloaded). If disabled, status code 429 (Too Many Requests) is used. Enabling -distributor.retry-after-header.enabled before utilizing this option is strongly recommended as it helps prevent premature request retries by the client.")
	f.BoolVar(&l.OTelMetricSuffixesEnabled, "distributor.otel-metric-suffixes-enabled", false, "Whether to enable automatic suffixes to names of metrics ingested through OTLP.")
	f.BoolVar(&l.OTelCreatedTimestampZeroIngestionEnabled, "distributor.otel-created-timestamp-zero-ingestion-enabled", false, "Whether to enable translation of OTel start timestamps to Prometheus zero samples in the OTLP endpoint.")
	f.Var(&l.PromoteOTelResourceAttributes, "distributor.otel-promote-resource-attributes", "Optionally specify OTel resource attributes to promote to labels.")
	f.BoolVar(&l.OTelKeepIdentifyingResourceAttributes, "distributor.otel-keep-identifying-resource-attributes", false, "Whether to keep identifying OTel resource attributes in the target_info metric on top of converting to job and instance labels.")
	f.Var(&l.IngestionArtificialDelay, "distributor.ingestion-artificial-delay", "Target ingestion delay. If set to a non-zero value, the distributor will artificially delay ingestion time-frame by the specified duration by computing the difference between actual ingestion and the target. There is no delay on actual ingestion of samples, it is only the response back to the client.")

	f.IntVar(&l.MaxGlobalSeriesPerUser, MaxSeriesPerUserFlag, 150000, "The maximum number of in-memory series per tenant, across the cluster before replication. 0 to disable.")
	f.IntVar(&l.MaxGlobalSeriesPerMetric, MaxSeriesPerMetricFlag, 0, "The maximum number of in-memory series per metric name, across the cluster before replication. 0 to disable.")

	f.IntVar(&l.MaxGlobalMetricsWithMetadataPerUser, MaxMetadataPerUserFlag, 0, "The maximum number of in-memory metrics with metadata per tenant, across the cluster. 0 to disable.")
	f.IntVar(&l.MaxGlobalMetadataPerMetric, MaxMetadataPerMetricFlag, 0, "The maximum number of metadata per metric, across the cluster. 0 to disable.")
	f.IntVar(&l.MaxGlobalExemplarsPerUser, "ingester.max-global-exemplars-per-user", 0, "The maximum number of exemplars in memory, across the cluster. 0 to disable exemplars ingestion.")
	f.BoolVar(&l.IgnoreOOOExemplars, "ingester.ignore-ooo-exemplars", false, "Whether to ignore exemplars with out-of-order timestamps. If enabled, exemplars with out-of-order timestamps are silently dropped, otherwise they cause partial errors.")
	f.Var(&l.ActiveSeriesCustomTrackersConfig, "ingester.active-series-custom-trackers", "Additional active series metrics, matching the provided matchers. Matchers should be in form <name>:<matcher>, like 'foobar:{foo=\"bar\"}'. Multiple matchers can be provided either providing the flag multiple times or providing multiple semicolon-separated values to a single flag.")
	f.Var(&l.OutOfOrderTimeWindow, "ingester.out-of-order-time-window", fmt.Sprintf("Non-zero value enables out-of-order support for most recent samples that are within the time window in relation to the TSDB's maximum time, i.e., within [db.maxTime-timeWindow, db.maxTime]). The ingester will need more memory as a factor of rate of out-of-order samples being ingested and the number of series that are getting out-of-order samples. If query falls into this window, cached results will use value from -%s option to specify TTL for resulting cache entry.", resultsCacheTTLForOutOfOrderWindowFlag))
	f.BoolVar(&l.NativeHistogramsIngestionEnabled, "ingester.native-histograms-ingestion-enabled", false, "Enable ingestion of native histogram samples. If false, native histogram samples are ignored without an error. To query native histograms with query-sharding enabled make sure to set -query-frontend.query-result-response-format to 'protobuf'.")
	f.BoolVar(&l.OOONativeHistogramsIngestionEnabled, "ingester.ooo-native-histograms-ingestion-enabled", false, "Enable experimental out-of-order native histogram ingestion. This only takes effect if the `-ingester.out-of-order-time-window` value is greater than zero and if `-ingester.native-histograms-ingestion-enabled = true`")
	f.BoolVar(&l.OutOfOrderBlocksExternalLabelEnabled, "ingester.out-of-order-blocks-external-label-enabled", false, "Whether the shipper should label out-of-order blocks with an external label before uploading them. Setting this label will compact out-of-order blocks separately from non-out-of-order blocks")

	f.StringVar(&l.SeparateMetricsGroupLabel, "validation.separate-metrics-group-label", "", "Label used to define the group label for metrics separation. For each write request, the group is obtained from the first non-empty group label from the first timeseries in the incoming list of timeseries. Specific distributor and ingester metrics will be further separated adding a 'group' label with group label's value. Currently applies to the following metrics: cortex_discarded_samples_total")

	f.IntVar(&l.MaxChunksPerQuery, MaxChunksPerQueryFlag, 2e6, "Maximum number of chunks that can be fetched in a single query from ingesters and store-gateways. This limit is enforced in the querier, ruler and store-gateway. 0 to disable.")
	f.Float64Var(&l.MaxEstimatedChunksPerQueryMultiplier, MaxEstimatedChunksPerQueryMultiplierFlag, 0, "Maximum number of chunks estimated to be fetched in a single query from ingesters and store-gateways, as a multiple of -"+MaxChunksPerQueryFlag+". This limit is enforced in the querier. Must be greater than or equal to 1, or 0 to disable.")
	f.IntVar(&l.MaxFetchedSeriesPerQuery, MaxSeriesPerQueryFlag, 0, "The maximum number of unique series for which a query can fetch samples from ingesters and store-gateways. This limit is enforced in the querier, ruler and store-gateway. 0 to disable")
	f.IntVar(&l.MaxFetchedChunkBytesPerQuery, MaxChunkBytesPerQueryFlag, 0, "The maximum size of all chunks in bytes that a query can fetch from ingesters and store-gateways. This limit is enforced in the querier and ruler. 0 to disable.")
	f.Uint64Var(&l.MaxEstimatedMemoryConsumptionPerQuery, MaxEstimatedMemoryConsumptionPerQueryFlag, 0, "The maximum estimated memory a single query can consume at once, in bytes. This limit is only enforced when Mimir's query engine is in use. This limit is enforced in the querier. 0 to disable.")
	f.Var(&l.MaxPartialQueryLength, MaxPartialQueryLengthFlag, "Limit the time range for partial queries at the querier level.")
	f.Var(&l.MaxQueryLookback, "querier.max-query-lookback", "Limit how long back data (series and metadata) can be queried, up until <lookback> duration ago. This limit is enforced in the query-frontend, querier and ruler for instant, range and remote read queries. For metadata queries like series, label names, label values queries the limit is enforced in the querier and ruler. If the requested time range is outside the allowed range, the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.")
	f.IntVar(&l.MaxQueryParallelism, "querier.max-query-parallelism", 14, "Maximum number of split (by time) or partial (by shard) queries that will be scheduled in parallel by the query-frontend for a single input query. This limit is introduced to have a fairer query scheduling and avoid a single query over a large time range saturating all available queriers.")
	f.Var(&l.MaxLabelsQueryLength, "store.max-labels-query-length", "Limit the time range (end - start time) of series, label names and values queries. This limit is enforced in the querier. If the requested time range is outside the allowed range, the request will not fail but will be manipulated to only query data within the allowed time range. 0 to disable.")
	f.IntVar(&l.LabelNamesAndValuesResultsMaxSizeBytes, "querier.label-names-and-values-results-max-size-bytes", 400*1024*1024, "Maximum size in bytes of distinct label names and values. When querier receives response from ingester, it merges the response with responses from other ingesters. This maximum size limit is applied to the merged(distinct) results. If the limit is reached, an error is returned.")
	f.IntVar(&l.ActiveSeriesResultsMaxSizeBytes, "querier.active-series-results-max-size-bytes", 400*1024*1024, "Maximum size of an active series or active native histogram series request result shard in bytes. 0 to disable.")
	f.BoolVar(&l.CardinalityAnalysisEnabled, "querier.cardinality-analysis-enabled", false, "Enables endpoints used for cardinality analysis.")
	f.IntVar(&l.LabelValuesMaxCardinalityLabelNamesPerRequest, "querier.label-values-max-cardinality-label-names-per-request", 100, "Maximum number of label names allowed to be queried in a single /api/v1/cardinality/label_values API call.")
	_ = l.MaxCacheFreshness.Set("10m")
	f.Var(&l.MaxCacheFreshness, "query-frontend.max-cache-freshness", "Most recent allowed cacheable result per-tenant, to prevent caching very recent results that might still be in flux.")

	f.IntVar(&l.MaxQueriersPerTenant, "query-frontend.max-queriers-per-tenant", 0, "Maximum number of queriers that can handle requests for a single tenant. If set to 0 or value higher than number of available queriers, *all* queriers will handle requests for the tenant. Each frontend (or query-scheduler, if used) will select the same set of queriers for the same tenant (given that all queriers are connected to all frontends / query-schedulers). This option only works with queriers connecting to the query-frontend / query-scheduler, not when using downstream URL.")
	f.IntVar(&l.QueryShardingTotalShards, "query-frontend.query-sharding-total-shards", 16, "The amount of shards to use when doing parallelisation via query sharding by tenant. 0 to disable query sharding for tenant. Query sharding implementation will adjust the number of query shards based on compactor shards. This allows querier to not search the blocks which cannot possibly have the series for given query shard.")
	f.IntVar(&l.QueryShardingMaxShardedQueries, "query-frontend.query-sharding-max-sharded-queries", 128, "The max number of sharded queries that can be run for a given received query. 0 to disable limit.")
	f.IntVar(&l.QueryShardingMaxRegexpSizeBytes, "query-frontend.query-sharding-max-regexp-size-bytes", 4096, "Disable query sharding for any query containing a regular expression matcher longer than the configured number of bytes. 0 to disable the limit.")
	f.Var(&l.SplitInstantQueriesByInterval, "query-frontend.split-instant-queries-by-interval", "Split instant queries by an interval and execute in parallel. 0 to disable it.")
	_ = l.QueryIngestersWithin.Set("13h")
	f.Var(&l.QueryIngestersWithin, QueryIngestersWithinFlag, "Maximum lookback beyond which queries are not sent to ingester. 0 means all queries are sent to ingester.")

	_ = l.RulerEvaluationDelay.Set("1m")
	f.Var(&l.RulerEvaluationDelay, "ruler.evaluation-delay-duration", "Duration to delay the evaluation of rules to ensure the underlying metrics have been pushed.")
	f.IntVar(&l.RulerTenantShardSize, "ruler.tenant-shard-size", 0, "The tenant's shard size when sharding is used by ruler. Value of 0 disables shuffle sharding for the tenant, and tenant rules will be sharded across all ruler replicas.")
	f.IntVar(&l.RulerMaxRulesPerRuleGroup, "ruler.max-rules-per-rule-group", 20, "Maximum number of rules per rule group per-tenant. 0 to disable.")
	f.IntVar(&l.RulerMaxRuleGroupsPerTenant, "ruler.max-rule-groups-per-tenant", 70, "Maximum number of rule groups per-tenant. 0 to disable.")
	f.BoolVar(&l.RulerRecordingRulesEvaluationEnabled, "ruler.recording-rules-evaluation-enabled", true, "Controls whether recording rules evaluation is enabled. This configuration option can be used to forcefully disable recording rules evaluation on a per-tenant basis.")
	f.BoolVar(&l.RulerAlertingRulesEvaluationEnabled, "ruler.alerting-rules-evaluation-enabled", true, "Controls whether alerting rules evaluation is enabled. This configuration option can be used to forcefully disable alerting rules evaluation on a per-tenant basis.")
	f.BoolVar(&l.RulerSyncRulesOnChangesEnabled, "ruler.sync-rules-on-changes-enabled", true, "True to enable a re-sync of the configured rule groups as soon as they're changed via ruler's config API. This re-sync is in addition of the periodic syncing. When enabled, it may take up to few tens of seconds before a configuration change triggers the re-sync.")
	// Needs to be initialised to a value so that the documentation can pick up the default value of `{}` because this is set as JSON from the command-line.
	if !l.RulerMaxRulesPerRuleGroupByNamespace.IsInitialized() {
		l.RulerMaxRulesPerRuleGroupByNamespace = NewLimitsMap[int](nil)
	}
	f.Var(&l.RulerMaxRulesPerRuleGroupByNamespace, "ruler.max-rules-per-rule-group-by-namespace", "Maximum number of rules per rule group by namespace. Value is a map, where each key is the namespace and value is the number of rules allowed in the namespace (int). On the command line, this map is given in a JSON format. The number of rules specified has the same meaning as -ruler.max-rules-per-rule-group, but only applies for the specific namespace. If specified, it supersedes -ruler.max-rules-per-rule-group.")

	if !l.RulerMaxRuleGroupsPerTenantByNamespace.IsInitialized() {
		l.RulerMaxRuleGroupsPerTenantByNamespace = NewLimitsMap[int](nil)
	}
	f.Var(&l.RulerMaxRuleGroupsPerTenantByNamespace, "ruler.max-rule-groups-per-tenant-by-namespace", "Maximum number of rule groups per tenant by namespace. Value is a map, where each key is the namespace and value is the number of rule groups allowed in the namespace (int). On the command line, this map is given in a JSON format. The number of rule groups specified has the same meaning as -ruler.max-rule-groups-per-tenant, but only applies for the specific namespace. If specified, it supersedes -ruler.max-rule-groups-per-tenant.")
	f.Var(&l.RulerProtectedNamespaces, "ruler.protected-namespaces", "List of namespaces that are protected from modification unless a special HTTP header is used. If a namespace is protected, it can only be read, not modified via the ruler's configuration API. The value is a list of strings, where each string is a namespace name. On the command line, this list is given as a comma-separated list.")
	f.Int64Var(&l.RulerMaxRuleEvaluationConcurrencyPerTenant, "ruler.max-rule-evaluation-concurrency-per-tenant", 4, "Maximum number of rules that can run concurrently for each tenant. Depends on ruler.max-rule-evaluation-concurrency being greater than 0. Ideally this flag should be a lower value. 0 to disable.")

	f.Var(&l.CompactorBlocksRetentionPeriod, "compactor.blocks-retention-period", "Delete blocks containing samples older than the specified retention period. Also used by query-frontend to avoid querying beyond the retention period by instant, range or remote read queries. 0 to disable.")
	f.IntVar(&l.CompactorSplitAndMergeShards, "compactor.split-and-merge-shards", 0, "The number of shards to use when splitting blocks. 0 to disable splitting.")
	f.IntVar(&l.CompactorSplitGroups, "compactor.split-groups", 1, "Number of groups that blocks for splitting should be grouped into. Each group of blocks is then split separately. Number of output split shards is controlled by -compactor.split-and-merge-shards.")
	f.IntVar(&l.CompactorTenantShardSize, "compactor.compactor-tenant-shard-size", 0, "Max number of compactors that can compact blocks for single tenant. 0 to disable the limit and use all compactors.")
	_ = l.CompactorPartialBlockDeletionDelay.Set("1d")
	f.Var(&l.CompactorPartialBlockDeletionDelay, "compactor.partial-block-deletion-delay", fmt.Sprintf("If a partial block (unfinished block without %s file) hasn't been modified for this time, it will be marked for deletion. The minimum accepted value is %s: a lower value will be ignored and the feature disabled. 0 to disable.", block.MetaFilename, MinCompactorPartialBlockDeletionDelay.String()))
	f.BoolVar(&l.CompactorBlockUploadEnabled, "compactor.block-upload-enabled", false, "Enable block upload API for the tenant.")
	f.BoolVar(&l.CompactorBlockUploadValidationEnabled, "compactor.block-upload-validation-enabled", true, "Enable block upload validation for the tenant.")
	f.BoolVar(&l.CompactorBlockUploadVerifyChunks, "compactor.block-upload-verify-chunks", true, "Verify chunks when uploading blocks via the upload API for the tenant.")
	f.Int64Var(&l.CompactorBlockUploadMaxBlockSizeBytes, "compactor.block-upload-max-block-size-bytes", 0, "Maximum size in bytes of a block that is allowed to be uploaded or validated. 0 = no limit.")
	f.IntVar(&l.CompactorInMemoryTenantMetaCacheSize, "compactor.in-memory-tenant-meta-cache-size", 0, "Size of per-tenant in-memory cache for parsed meta.json files. This is useful when meta.json files are big and parsing is expensive. Small meta.json files are not cached. 0 means this cache is disabled.")

	// Query-frontend.
	f.Var(&l.MaxTotalQueryLength, MaxTotalQueryLengthFlag, "Limit the total query time range (end - start time). This limit is enforced in the query-frontend on the received instant, range or remote read query.")
	_ = l.ResultsCacheTTL.Set("7d")
	f.Var(&l.ResultsCacheTTL, resultsCacheTTLFlag, fmt.Sprintf("Time to live duration for cached query results. If query falls into out-of-order time window, -%s is used instead.", resultsCacheTTLForOutOfOrderWindowFlag))
	_ = l.ResultsCacheTTLForOutOfOrderTimeWindow.Set("10m")
	f.Var(&l.ResultsCacheTTLForOutOfOrderTimeWindow, resultsCacheTTLForOutOfOrderWindowFlag, fmt.Sprintf("Time to live duration for cached query results if query falls into out-of-order time window. This is lower than -%s so that incoming out-of-order samples are returned in the query results sooner.", resultsCacheTTLFlag))
	f.Var(&l.ResultsCacheTTLForCardinalityQuery, "query-frontend.results-cache-ttl-for-cardinality-query", "Time to live duration for cached cardinality query results. The value 0 disables the cache.")
	f.Var(&l.ResultsCacheTTLForLabelsQuery, "query-frontend.results-cache-ttl-for-labels-query", "Time to live duration for cached label names and label values query results. The value 0 disables the cache.")
	_ = l.ResultsCacheTTLForErrors.Set("5m")
	f.Var(&l.ResultsCacheTTLForErrors, "query-frontend.results-cache-ttl-for-errors", "Time to live duration for cached non-transient errors")
	f.BoolVar(&l.ResultsCacheForUnalignedQueryEnabled, "query-frontend.cache-unaligned-requests", false, "Cache requests that are not step-aligned.")
	f.IntVar(&l.MaxQueryExpressionSizeBytes, MaxQueryExpressionSizeBytesFlag, 0, "Max size of the raw query, in bytes. This limit is enforced by the query-frontend for instant, range and remote read queries. 0 to not apply a limit to the size of the query.")
	f.BoolVar(&l.AlignQueriesWithStep, alignQueriesWithStepFlag, false, "Mutate incoming queries to align their start and end with their step to improve result caching.")
	f.Var(&l.EnabledPromQLExperimentalFunctions, "query-frontend.enabled-promql-experimental-functions", "Enable certain experimental PromQL functions, which are subject to being changed or removed at any time, on a per-tenant basis. Defaults to empty which means all experimental functions are disabled. Set to 'all' to enable all experimental functions.")

	// Store-gateway.
	f.IntVar(&l.StoreGatewayTenantShardSize, "store-gateway.tenant-shard-size", 0, "The tenant's shard size, used when store-gateway sharding is enabled. Value of 0 disables shuffle sharding for the tenant, that is all tenant blocks are sharded across all store-gateway replicas.")

	// Alertmanager.
	f.Var(&l.AlertmanagerReceiversBlockCIDRNetworks, "alertmanager.receivers-firewall-block-cidr-networks", "Comma-separated list of network CIDRs to block in Alertmanager receiver integrations.")
	f.BoolVar(&l.AlertmanagerReceiversBlockPrivateAddresses, "alertmanager.receivers-firewall-block-private-addresses", false, "True to block private and local addresses in Alertmanager receiver integrations. It blocks private addresses defined by  RFC 1918 (IPv4 addresses) and RFC 4193 (IPv6 addresses), as well as loopback, local unicast and local multicast addresses.")

	f.Float64Var(&l.NotificationRateLimit, "alertmanager.notification-rate-limit", 0, "Per-tenant rate limit for sending notifications from Alertmanager in notifications/sec. 0 = rate limit disabled. Negative value = no notifications are allowed.")

	// Needs to be initialised to a value so that the documentation can pick up the default value of `{}` because this is set as JSON from the command-line.
	if !l.NotificationRateLimitPerIntegration.IsInitialized() {
		l.NotificationRateLimitPerIntegration = NotificationRateLimitMap()
	}
	f.Var(&l.NotificationRateLimitPerIntegration, "alertmanager.notification-rate-limit-per-integration", "Per-integration notification rate limits. Value is a map, where each key is integration name and value is a rate-limit (float). On command line, this map is given in JSON format. Rate limit has the same meaning as -alertmanager.notification-rate-limit, but only applies for specific integration. Allowed integration names: "+strings.Join(allowedIntegrationNames, ", ")+".")
	_ = l.AlertmanagerMaxGrafanaConfigSizeBytes.Set("0")
	f.Var(&l.AlertmanagerMaxGrafanaConfigSizeBytes, AlertmanagerMaxGrafanaConfigSizeFlag, "Maximum size of the Grafana Alertmanager configuration for a tenant. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxConfigSizeBytes, "alertmanager.max-config-size-bytes", 0, "Maximum size of the Alertmanager configuration for a tenant. 0 = no limit.")
	_ = l.AlertmanagerMaxGrafanaStateSizeBytes.Set("0")
	f.Var(&l.AlertmanagerMaxGrafanaStateSizeBytes, AlertmanagerMaxGrafanaStateSizeFlag, "Maximum size of the Grafana Alertmanager state for a tenant. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxSilencesCount, "alertmanager.max-silences-count", 0, "Maximum number of silences, including expired silences, that a tenant can have at once. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxSilenceSizeBytes, "alertmanager.max-silence-size-bytes", 0, "Maximum silence size in bytes. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxTemplatesCount, "alertmanager.max-templates-count", 0, "Maximum number of templates in tenant's Alertmanager configuration uploaded via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxTemplateSizeBytes, "alertmanager.max-template-size-bytes", 0, "Maximum size of single template in tenant's Alertmanager configuration uploaded via Alertmanager API. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxDispatcherAggregationGroups, "alertmanager.max-dispatcher-aggregation-groups", 0, "Maximum number of aggregation groups in Alertmanager's dispatcher that a tenant can have. Each active aggregation group uses single goroutine. When the limit is reached, dispatcher will not dispatch alerts that belong to additional aggregation groups, but existing groups will keep working properly. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxAlertsCount, "alertmanager.max-alerts-count", 0, "Maximum number of alerts that a single tenant can have. Inserting more alerts will fail with a log message and metric increment. 0 = no limit.")
	f.IntVar(&l.AlertmanagerMaxAlertsSizeBytes, "alertmanager.max-alerts-size-bytes", 0, "Maximum total size of alerts that a single tenant can have, alert size is the sum of the bytes of its labels, annotations and generatorURL. Inserting more alerts will fail with a log message and metric increment. 0 = no limit.")

	// Ingest storage.
	f.StringVar(&l.IngestStorageReadConsistency, "ingest-storage.read-consistency", api.ReadConsistencyEventual, fmt.Sprintf("The default consistency level to enforce for queries when using the ingest storage. Supports values: %s.", strings.Join(api.ReadConsistencies, ", ")))
	f.IntVar(&l.IngestionPartitionsTenantShardSize, "ingest-storage.ingestion-partition-tenant-shard-size", 0, "The number of partitions a tenant's data should be sharded to when using the ingest storage. Tenants are sharded across partitions using shuffle-sharding. 0 disables shuffle sharding and tenant is sharded across all partitions.")
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *Limits) UnmarshalYAML(value *yaml.Node) error {
	return l.unmarshal(func(v any) error {
		return value.DecodeWithOptions(v, yaml.DecodeOptions{KnownFields: true})
	})
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (l *Limits) UnmarshalJSON(data []byte) error {
	return l.unmarshal(func(v any) error {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()

		return dec.Decode(v)
	})
}

// unmarshal does both YAML and JSON.
func (l *Limits) unmarshal(decode func(any) error) error {
	// We want to set l to the defaults and then overwrite it with the input.
	if defaultLimits != nil {
		*l = *defaultLimits

		// Make copy of default limits, otherwise unmarshalling would modify map in default limits.
		l.NotificationRateLimitPerIntegration = defaultLimits.NotificationRateLimitPerIntegration.Clone()
		l.RulerMaxRulesPerRuleGroupByNamespace = defaultLimits.RulerMaxRulesPerRuleGroupByNamespace.Clone()
		l.RulerMaxRuleGroupsPerTenantByNamespace = defaultLimits.RulerMaxRuleGroupsPerTenantByNamespace.Clone()
	}

	// Decode into a reflection-crafted struct that has fields for the extensions.
	cfg, getExtensions := newLimitsWithExtensions((*plainLimits)(l))
	err := decode(cfg)
	if err != nil {
		return err
	}
	l.extensions = getExtensions()

	return l.validate()
}

// RegisterExtensionsDefaults registers the default values for extensions into l.
// This is especially handy for those downstream projects that wish to have control
// over the exact moment in which the registration happens (e.g. during service
// dependency initialization).
func (l *Limits) RegisterExtensionsDefaults() {
	_, getExtensions := newLimitsWithExtensions((*plainLimits)(l))
	l.extensions = getExtensions()
}

func (l *Limits) MarshalJSON() ([]byte, error) {
	return json.Marshal(limitsToStructWithExtensionFields(l))
}

func (l *Limits) MarshalYAML() (interface{}, error) {
	return limitsToStructWithExtensionFields(l), nil
}

func (l *Limits) validate() error {
	for _, cfg := range l.MetricRelabelConfigs {
		if cfg == nil {
			return errors.New("invalid metric_relabel_configs")
		}
	}

	if l.MaxEstimatedChunksPerQueryMultiplier < 1 && l.MaxEstimatedChunksPerQueryMultiplier != 0 {
		return errInvalidMaxEstimatedChunksPerQueryMultiplier
	}

	if !util.StringsContain(api.ReadConsistencies, l.IngestStorageReadConsistency) {
		return errInvalidIngestStorageReadConsistency
	}

	return nil
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

// RequestRate returns the limit on request rate (requests per second).
func (o *Overrides) RequestRate(userID string) float64 {
	return o.getOverridesForUser(userID).RequestRate
}

// RequestBurstSize returns the burst size for request rate.
func (o *Overrides) RequestBurstSize(userID string) int {
	return o.getOverridesForUser(userID).RequestBurstSize
}

// IngestionRate returns the limit on ingester rate (samples per second).
func (o *Overrides) IngestionRate(userID string) float64 {
	return o.getOverridesForUser(userID).IngestionRate
}

// LabelNamesAndValuesResultsMaxSizeBytes returns the maximum size in bytes of distinct label names and values
func (o *Overrides) LabelNamesAndValuesResultsMaxSizeBytes(userID string) int {
	return o.getOverridesForUser(userID).LabelNamesAndValuesResultsMaxSizeBytes
}

func (o *Overrides) ActiveSeriesResultsMaxSizeBytes(userID string) int {
	return o.getOverridesForUser(userID).ActiveSeriesResultsMaxSizeBytes
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

func (o *Overrides) IngestionBurstFactor(userID string) float64 {
	burstFactor := o.getOverridesForUser(userID).IngestionBurstFactor
	if burstFactor < 1 {
		return 0
	}
	return burstFactor
}

// AcceptHASamples returns whether the distributor should track and accept samples from HA replicas for this user.
func (o *Overrides) AcceptHASamples(userID string) bool {
	return o.getOverridesForUser(userID).AcceptHASamples
}

// ServiceOverloadStatusCodeOnRateLimitEnabled return whether the distributor uses status code 529 instead of 429 when the rate limit is exceeded.
func (o *Overrides) ServiceOverloadStatusCodeOnRateLimitEnabled(userID string) bool {
	return o.getOverridesForUser(userID).ServiceOverloadStatusCodeOnRateLimitEnabled
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

// MaxLabelNamesPerInfoSeries returns maximum number of label/value pairs for info timeseries.
func (o *Overrides) MaxLabelNamesPerInfoSeries(userID string) int {
	return o.getOverridesForUser(userID).MaxLabelNamesPerInfoSeries
}

// MaxMetadataLength returns maximum length metadata can be. Metadata refers
// to the Metric Name, HELP and UNIT.
func (o *Overrides) MaxMetadataLength(userID string) int {
	return o.getOverridesForUser(userID).MaxMetadataLength
}

// MaxNativeHistogramBuckets returns the maximum number of buckets per native
// histogram sample.
func (o *Overrides) MaxNativeHistogramBuckets(userID string) int {
	return o.getOverridesForUser(userID).MaxNativeHistogramBuckets
}

// ReduceNativeHistogramOverMaxBuckets returns whether to reduce or reject
// native histogram samples with more buckets than the configured limit.
func (o *Overrides) ReduceNativeHistogramOverMaxBuckets(userID string) bool {
	return o.getOverridesForUser(userID).ReduceNativeHistogramOverMaxBuckets
}

// CreationGracePeriod is misnamed, and actually returns how far into the future
// we should accept samples.
func (o *Overrides) CreationGracePeriod(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).CreationGracePeriod)
}

// PastGracePeriod is similar to CreationGracePeriod but looking into the past.
// Zero means disabled.
func (o *Overrides) PastGracePeriod(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).PastGracePeriod)
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

func (o *Overrides) MaxEstimatedChunksPerQuery(userID string) int {
	overridesForUser := o.getOverridesForUser(userID)
	return int(overridesForUser.MaxEstimatedChunksPerQueryMultiplier * float64(overridesForUser.MaxChunksPerQuery))
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

// MaxEstimatedMemoryConsumptionPerQuery returns the maximum allowed estimated memory consumption of a single query.
// This is only effective when using Mimir's query engine (not Prometheus' engine).
func (o *Overrides) MaxEstimatedMemoryConsumptionPerQuery(userID string) uint64 {
	return o.getOverridesForUser(userID).MaxEstimatedMemoryConsumptionPerQuery
}

// MaxQueryLookback returns the max lookback period of queries.
func (o *Overrides) MaxQueryLookback(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).MaxQueryLookback)
}

// MaxPartialQueryLength returns the limit of the length (in time) of a (partial) query.
func (o *Overrides) MaxPartialQueryLength(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).MaxPartialQueryLength)
}

// MaxTotalQueryLength returns the limit of the total length (in time) of a query.
func (o *Overrides) MaxTotalQueryLength(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).MaxTotalQueryLength)
}

// MaxQueryExpressionSizeBytes returns the limit of the raw query size, in bytes.
func (o *Overrides) MaxQueryExpressionSizeBytes(userID string) int {
	return o.getOverridesForUser(userID).MaxQueryExpressionSizeBytes
}

// BlockedQueries returns the blocked queries.
func (o *Overrides) BlockedQueries(userID string) []*BlockedQuery {
	return o.getOverridesForUser(userID).BlockedQueries
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

// QueryShardingMaxRegexpSizeBytes returns the limit to the max number of bytes allowed
// for a regexp matcher in a shardable query. If a query contains a regexp matcher longer
// than this limit, the query will not be sharded. 0 to disable limit.
func (o *Overrides) QueryShardingMaxRegexpSizeBytes(userID string) int {
	return o.getOverridesForUser(userID).QueryShardingMaxRegexpSizeBytes
}

// SplitInstantQueriesByInterval returns the split time interval to use when splitting an instant query
// via the query-frontend. 0 to disable limit.
func (o *Overrides) SplitInstantQueriesByInterval(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).SplitInstantQueriesByInterval)
}

// QueryIngestersWithin returns the maximum lookback beyond which queries are not sent to ingester.
// 0 means all queries are sent to ingester.
func (o *Overrides) QueryIngestersWithin(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).QueryIngestersWithin)
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

// MaxGlobalExemplarsPerUser returns the maximum number of exemplars held in memory across the cluster.
func (o *Overrides) MaxGlobalExemplarsPerUser(userID string) int {
	return o.getOverridesForUser(userID).MaxGlobalExemplarsPerUser
}

// IgnoreOOOExemplars returns whether to ignore out-of-order exemplars.
func (o *Overrides) IgnoreOOOExemplars(userID string) bool {
	return o.getOverridesForUser(userID).IgnoreOOOExemplars
}

func (o *Overrides) ActiveSeriesCustomTrackersConfig(userID string) asmodel.CustomTrackersConfig {
	return o.getOverridesForUser(userID).ActiveSeriesCustomTrackersConfig
}

// OutOfOrderTimeWindow returns the out-of-order time window for the user.
func (o *Overrides) OutOfOrderTimeWindow(userID string) time.Duration {
	return time.Duration(o.getOverridesForUser(userID).OutOfOrderTimeWindow)
}

// OutOfOrderBlocksExternalLabelEnabled returns if the shipper is flagging out-of-order blocks with an external label.
func (o *Overrides) OutOfOrderBlocksExternalLabelEnabled(userID string) bool {
	return o.getOverridesForUser(userID).OutOfOrderBlocksExternalLabelEnabled
}

// SeparateMetricsGroupLabel returns the custom label used to separate specific metrics
func (o *Overrides) SeparateMetricsGroupLabel(userID string) string {
	return o.getOverridesForUser(userID).SeparateMetricsGroupLabel
}

// IngestionTenantShardSize returns the ingesters shard size for a given user.
func (o *Overrides) IngestionTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).IngestionTenantShardSize
}

// CompactorTenantShardSize returns number of compactors that this user can use. 0 = all compactors.
func (o *Overrides) CompactorTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).CompactorTenantShardSize
}

func (o *Overrides) CompactorInMemoryTenantMetaCacheSize(userID string) int {
	return o.getOverridesForUser(userID).CompactorInMemoryTenantMetaCacheSize
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

// CompactorSplitGroups returns the number of groups that blocks for splitting should be grouped into.
func (o *Overrides) CompactorSplitGroups(userID string) int {
	return o.getOverridesForUser(userID).CompactorSplitGroups
}

// CompactorPartialBlockDeletionDelay returns the partial block deletion delay time period for a given user,
// and whether the configured value was valid. If the value wasn't valid, the returned delay is the default one
// and the caller is responsible to warn the Mimir operator about it.
func (o *Overrides) CompactorPartialBlockDeletionDelay(userID string) (delay time.Duration, valid bool) {
	delay = time.Duration(o.getOverridesForUser(userID).CompactorPartialBlockDeletionDelay)

	// Forcefully disable partial blocks deletion if the configured delay is too low.
	if delay > 0 && delay < MinCompactorPartialBlockDeletionDelay {
		return 0, false
	}

	return delay, true
}

// CompactorBlockUploadEnabled returns whether block upload is enabled for a certain tenant.
func (o *Overrides) CompactorBlockUploadEnabled(tenantID string) bool {
	return o.getOverridesForUser(tenantID).CompactorBlockUploadEnabled
}

// CompactorBlockUploadValidationEnabled returns whether block upload validation is enabled for a certain tenant.
func (o *Overrides) CompactorBlockUploadValidationEnabled(tenantID string) bool {
	return o.getOverridesForUser(tenantID).CompactorBlockUploadValidationEnabled
}

// CompactorBlockUploadVerifyChunks returns whether compaction chunk verification is enabled for a certain tenant.
func (o *Overrides) CompactorBlockUploadVerifyChunks(tenantID string) bool {
	return o.getOverridesForUser(tenantID).CompactorBlockUploadVerifyChunks
}

// CompactorBlockUploadMaxBlockSizeBytes returns the maximum size in bytes of a block that is allowed to be uploaded or validated for a given user.
func (o *Overrides) CompactorBlockUploadMaxBlockSizeBytes(userID string) int64 {
	return o.getOverridesForUser(userID).CompactorBlockUploadMaxBlockSizeBytes
}

// MetricRelabelConfigs returns the metric relabel configs for a given user.
func (o *Overrides) MetricRelabelConfigs(userID string) []*relabel.Config {
	return o.getOverridesForUser(userID).MetricRelabelConfigs
}

func (o *Overrides) MetricRelabelingEnabled(userID string) bool {
	return o.getOverridesForUser(userID).MetricRelabelingEnabled
}

// NativeHistogramsIngestionEnabled returns whether to ingest native histograms in the ingester
func (o *Overrides) NativeHistogramsIngestionEnabled(userID string) bool {
	return o.getOverridesForUser(userID).NativeHistogramsIngestionEnabled
}

// OOONativeHistogramsIngestionEnabled returns whether to ingest OOO native histograms in the ingester
func (o *Overrides) OOONativeHistogramsIngestionEnabled(userID string) bool {
	return o.getOverridesForUser(userID).OOONativeHistogramsIngestionEnabled
}

func (o *Overrides) MaxExemplarsPerSeriesPerRequest(userID string) int {
	return o.getOverridesForUser(userID).MaxExemplarsPerSeriesPerRequest
}

// RulerTenantShardSize returns shard size (number of rulers) used by this tenant when using shuffle-sharding strategy.
func (o *Overrides) RulerTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).RulerTenantShardSize
}

// RulerMaxRulesPerRuleGroup returns the maximum number of rules per rule group for a given user.
// This limit is special. Limits are returned in the following order:
// 1. Per tenant limit for the given namespace.
// 2. Default limit for the given namespace.
// 3. Per tenant limit set by RulerMaxRulesPerRuleGroup
// 4. Default limit set by RulerMaxRulesPerRuleGroup
func (o *Overrides) RulerMaxRulesPerRuleGroup(userID, namespace string) int {
	u := o.getOverridesForUser(userID)

	if namespaceLimit, ok := u.RulerMaxRulesPerRuleGroupByNamespace.Read()[namespace]; ok {
		return namespaceLimit
	}

	return u.RulerMaxRulesPerRuleGroup
}

// RulerMaxRuleGroupsPerTenant returns the maximum number of rule groups for a given user.
// This limit is special. Limits are returned in the following order:
// 1. Per tenant limit for the given namespace.
// 2. Default limit for the given namespace.
// 3. Per tenant limit set by RulerMaxRuleGroupsPerTenant
// 4. Default limit set by RulerMaxRuleGroupsPerTenant
func (o *Overrides) RulerMaxRuleGroupsPerTenant(userID, namespace string) int {
	u := o.getOverridesForUser(userID)

	if namespaceLimit, ok := u.RulerMaxRuleGroupsPerTenantByNamespace.Read()[namespace]; ok {
		return namespaceLimit
	}

	return u.RulerMaxRuleGroupsPerTenant
}

// RulerProtectedNamespaces returns the list of namespaces that are protected from modification.
func (o *Overrides) RulerProtectedNamespaces(userID string) []string {
	return o.getOverridesForUser(userID).RulerProtectedNamespaces
}

// RulerRecordingRulesEvaluationEnabled returns whether the recording rules evaluation is enabled for a given user.
func (o *Overrides) RulerRecordingRulesEvaluationEnabled(userID string) bool {
	return o.getOverridesForUser(userID).RulerRecordingRulesEvaluationEnabled
}

// RulerAlertingRulesEvaluationEnabled returns whether the alerting rules evaluation is enabled for a given user.
func (o *Overrides) RulerAlertingRulesEvaluationEnabled(userID string) bool {
	return o.getOverridesForUser(userID).RulerAlertingRulesEvaluationEnabled
}

// RulerSyncRulesOnChangesEnabled returns whether the ruler's event-based sync is enabled.
func (o *Overrides) RulerSyncRulesOnChangesEnabled(userID string) bool {
	return o.getOverridesForUser(userID).RulerSyncRulesOnChangesEnabled
}

// RulerMaxRuleEvaluationConcurrencyPerTenant returns the maximum number of rules that can run concurrently for a given user.
func (o *Overrides) RulerMaxRuleEvaluationConcurrencyPerTenant(userID string) int64 {
	return o.getOverridesForUser(userID).RulerMaxRuleEvaluationConcurrencyPerTenant
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
	if n, ok := u.NotificationRateLimitPerIntegration.Read()[integration]; ok {
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

func (o *Overrides) AlertmanagerMaxGrafanaStateSize(userID string) int {
	return int(o.getOverridesForUser(userID).AlertmanagerMaxGrafanaStateSizeBytes)
}

func (o *Overrides) AlertmanagerMaxGrafanaConfigSize(userID string) int {
	return int(o.getOverridesForUser(userID).AlertmanagerMaxGrafanaConfigSizeBytes)
}

func (o *Overrides) AlertmanagerMaxConfigSize(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxConfigSizeBytes
}

func (o *Overrides) AlertmanagerMaxSilencesCount(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxSilencesCount
}

func (o *Overrides) AlertmanagerMaxSilenceSizeBytes(userID string) int {
	return o.getOverridesForUser(userID).AlertmanagerMaxSilenceSizeBytes
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

func (o *Overrides) ResultsCacheTTL(user string) time.Duration {
	return time.Duration(o.getOverridesForUser(user).ResultsCacheTTL)
}

func (o *Overrides) ResultsCacheTTLForOutOfOrderTimeWindow(user string) time.Duration {
	return time.Duration(o.getOverridesForUser(user).ResultsCacheTTLForOutOfOrderTimeWindow)
}

func (o *Overrides) ResultsCacheTTLForCardinalityQuery(user string) time.Duration {
	return time.Duration(o.getOverridesForUser(user).ResultsCacheTTLForCardinalityQuery)
}

func (o *Overrides) ResultsCacheTTLForLabelsQuery(user string) time.Duration {
	return time.Duration(o.getOverridesForUser(user).ResultsCacheTTLForLabelsQuery)
}

func (o *Overrides) ResultsCacheTTLForErrors(user string) time.Duration {
	return time.Duration(o.getOverridesForUser(user).ResultsCacheTTLForErrors)
}

func (o *Overrides) ResultsCacheForUnalignedQueryEnabled(userID string) bool {
	return o.getOverridesForUser(userID).ResultsCacheForUnalignedQueryEnabled
}

func (o *Overrides) EnabledPromQLExperimentalFunctions(userID string) []string {
	return o.getOverridesForUser(userID).EnabledPromQLExperimentalFunctions
}

func (o *Overrides) OTelMetricSuffixesEnabled(tenantID string) bool {
	return o.getOverridesForUser(tenantID).OTelMetricSuffixesEnabled
}

func (o *Overrides) OTelCreatedTimestampZeroIngestionEnabled(tenantID string) bool {
	return o.getOverridesForUser(tenantID).OTelCreatedTimestampZeroIngestionEnabled
}

func (o *Overrides) PromoteOTelResourceAttributes(tenantID string) []string {
	return o.getOverridesForUser(tenantID).PromoteOTelResourceAttributes
}

func (o *Overrides) OTelKeepIdentifyingResourceAttributes(tenantID string) bool {
	return o.getOverridesForUser(tenantID).OTelKeepIdentifyingResourceAttributes
}

// DistributorIngestionArtificialDelay returns the artificial ingestion latency for a given use.
func (o *Overrides) DistributorIngestionArtificialDelay(tenantID string) time.Duration {
	return time.Duration(o.getOverridesForUser(tenantID).IngestionArtificialDelay)
}

func (o *Overrides) AlignQueriesWithStep(userID string) bool {
	return o.getOverridesForUser(userID).AlignQueriesWithStep
}

// IngestStorageReadConsistency returns the default read consistency for the tenant.
func (o *Overrides) IngestStorageReadConsistency(userID string) string {
	return o.getOverridesForUser(userID).IngestStorageReadConsistency
}

func (o *Overrides) IngestionPartitionsTenantShardSize(userID string) int {
	return o.getOverridesForUser(userID).IngestionPartitionsTenantShardSize
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

// AllTrueBooleansPerTenant returns true only if limit func is true for all given tenants
func AllTrueBooleansPerTenant(tenantIDs []string, f func(string) bool) bool {
	for _, tenantID := range tenantIDs {
		if !f(tenantID) {
			return false
		}
	}
	return true
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
// limits a value of 0 means unlimited so the method will return 0 only if all
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
// many limits a value of 0 means unlimited so the method will return 0 only if
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

// MinDurationPerTenant is returning the minimum duration per tenant. Without
// tenants given it will return a time.Duration(0).
func MinDurationPerTenant(tenantIDs []string, f func(string) time.Duration) time.Duration {
	result := time.Duration(0)
	for idx, tenantID := range tenantIDs {
		v := f(tenantID)

		if idx == 0 || v < result {
			result = v
		}
	}
	return result
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

// MustRegisterExtension registers the extensions type with given name
// and returns a function to get the extensions value from a *Limits instance.
//
// The provided name will be used as YAML/JSON key to decode the extensions.
//
// The returned getter will return the result of E.Default() if *Limits is nil.
//
// This method is not thread safe and should be called only during package initialization.
// Registering same name twice, or registering a name that is already a *Limits JSON or YAML key will cause a panic.
func MustRegisterExtension[E interface{ Default() E }](name string) func(*Limits) E {
	if name == "" {
		panic("extension name cannot be empty")
	}
	if _, ok := standardLimitsYAMLJSONKeys[name]; ok {
		panic(fmt.Errorf("extension %s cannot be registered because it's a standard limits field", name))
	}
	if _, ok := registeredExtensions[name]; ok {
		panic(fmt.Errorf("extension %s already registered", name))
	}

	var zeroE E
	registeredExtensions[name] = registeredExtension{
		index:            len(registeredExtensions),
		reflectedDefault: func() reflect.Value { return reflect.ValueOf(zeroE.Default()) },
	}

	limitsExtensionsFields = append(limitsExtensionsFields, reflect.StructField{
		Name: strings.ToUpper(name),
		Type: reflect.TypeOf(zeroE),
		Tag:  reflect.StructTag(fmt.Sprintf(`yaml:"%s" json:"%s"`, name, name)),
	})

	return func(l *Limits) (e E) {
		if l == nil {
			// Call e.Default() here every time instead of storing it when the extension is being registered, as it might change over time.
			// Especially when the default values are initialized after package initialization phase, where this is registered.
			return e.Default()
		}
		if l.extensions[name] == nil {
			return zeroE
		}
		return l.extensions[name].(E)
	}
}

var standardLimitsYAMLJSONKeys = map[string]struct{}{}

func init() {
	limitsType := reflect.TypeOf(Limits{})
	for i := 0; i < limitsType.NumField(); i++ {
		// yamlKey/jsonKey could be empty, but we also shouldn't allow registering a field with an empty name, so just add it to the map.
		yamlKey, _, _ := strings.Cut(limitsType.Field(i).Tag.Get("yaml"), ",")
		jsonKey, _, _ := strings.Cut(limitsType.Field(i).Tag.Get("json"), ",")
		standardLimitsYAMLJSONKeys[yamlKey] = struct{}{}
		standardLimitsYAMLJSONKeys[jsonKey] = struct{}{}
	}
}

type registeredExtension struct {
	index            int
	reflectedDefault func() reflect.Value
}

// registeredExtensions is used to keep track of the indexes of each registered extension.
var registeredExtensions = map[string]registeredExtension{}

// limitsExtensionsFields is the list of the extension fields to be added to the reflection-crafted Limits struct.
var limitsExtensionsFields []reflect.StructField

// plainLimits is used to prevent an infinite loop of calling UnmarshalJSON/UnmarshalYAML by hiding behind type indirection.
type plainLimits Limits

// plainLimitsStructField is the last field in the struct crafted by newLimitsWithExtensions.
var plainLimitsStructField = reflect.StructField{
	Name:      "PlainLimits",
	Type:      reflect.TypeOf(new(plainLimits)),
	Tag:       `yaml:",inline"`,
	Anonymous: true,
}

// buildStructWithExtensionFieldsAndDefaultValues returns an interface{} value of a pointer to a struct of type:
//
//	struct {
//	    EXTNAME1    T1                     `yaml:"extname1" json:"extname1"`
//	    // ...
//	    EXTNAMEN    TN                     `yaml:"extnameN" json:"extnameN"`
//
//	    PlainLimits map[string]interface{} `yaml:",inline"`
//	}
//
// Where TN is the type of the registered extension N, and extnameN is the name of it.
// This makes the JSON/YAML unmarshaler go through each extension field, and unmarshal the rest of the payload in the plain limits field.
//
// Embedding PlainLimits in the struct makes JSON parser act like `yaml:",inline"`.
//
// This method doesn't set any field values.
func buildStructWithExtensionFieldsAndDefaultValues() reflect.Value {
	// We have extensions, craft our own type.
	// It's not strictly necessary to create a new slice here, we could just append to limitsExtensionsFields assuming that
	// this would allocate a new underlying array, but that is too fragile, so let's copy the fields to a new slice.
	fields := make([]reflect.StructField, 0, len(limitsExtensionsFields)+1)
	fields = append(fields, limitsExtensionsFields...)
	fields = append(fields, plainLimitsStructField)

	// typ is the type of the new struct.
	typ := reflect.StructOf(fields)
	// cfg is an instance of a pointer to a new struct.
	cfg := reflect.New(typ)
	return cfg
}

// newLimitsWithExtensions returns struct suitable for unmarshalling and a function to return map of extension values.
func newLimitsWithExtensions(limits *plainLimits) (any interface{}, getExtensions func() map[string]interface{}) {
	if len(registeredExtensions) == 0 {
		// No extensions, so just return the plain limits and an extension getter that returns nil.
		return limits, func() map[string]interface{} { return nil }
	}

	cfg := buildStructWithExtensionFieldsAndDefaultValues()

	// Set default values of each field
	// In other words:
	//     cfg.EXTNAME1 = cfg.EXTNAME1.Default()
	for _, ext := range registeredExtensions {
		cfg.Elem().Field(ext.index).Set(ext.reflectedDefault())
	}

	// set the limits provided (they probably contain default limits) to the new struct, so we'll unmarshal on top of them.
	// In other words:
	//     cfg.PlainLimits = limits
	cfg.Elem().FieldByName(plainLimitsStructField.Name).Set(reflect.ValueOf(limits))

	return cfg.Interface(), func() map[string]interface{} {
		ext := map[string]interface{}{}
		for name, re := range registeredExtensions {
			ext[name] = cfg.Elem().Field(re.index).Interface()
		}
		return ext
	}
}

// limitsToStructWithExtensionFields converts existing Limits into a struct that contains all extension fields
// set to values in the limits. This struct can be used for JSON/YAML marshalling.
func limitsToStructWithExtensionFields(limits *Limits) interface{} {
	if len(registeredExtensions) == 0 {
		return (*plainLimits)(limits)
	}

	cfg := buildStructWithExtensionFieldsAndDefaultValues()

	// Set values from limits.
	// In other words:
	//     cfg.PlainLimits = limits
	cfg.Elem().FieldByName(plainLimitsStructField.Name).Set(reflect.ValueOf((*plainLimits)(limits)))

	// Set value of each extension field based on value stored in supplied limits.
	// Some values may not be set.
	// In other words:
	//     cfg.EXTNAME = limits.extension[EXTNAME]
	for name, val := range limits.extensions {
		ext := registeredExtensions[name]
		cfg.Elem().Field(ext.index).Set(reflect.ValueOf(val))
	}

	return cfg.Interface()
}
