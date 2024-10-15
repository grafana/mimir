// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"fmt"
	"strings"

	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/storage/bucket/s3"
)

// CortexToMimirMapper maps from cortex-1.11.0 to latest mimir configurations
func CortexToMimirMapper() Mapper {
	return MultiMapper{
		mapCortexInstanceInterfaceNames(),
		// Try to naively map keys from old config to same keys from new config
		BestEffortDirectMapper{},
		// next map alertmanager URL in the ruler config
		MapperFunc(alertmanagerURLMapperFunc),
		// Removed `-alertmanager.storage.*` configuration options, use `-alertmanager-storage.*` instead. -alertmanager.storage.* should take precedence
		MapperFunc(alertmanagerStorageMapperFunc),
		// Removed the support for `-ruler.storage.*`, use `-ruler-storage.*` instead. -ruler.storage.* should take precedence
		MapperFunc(rulerStorageMapperFunc),
		// Replace (ruler|alertmanager).storage.s3.sse_encryption=true with (alertmanager|ruler)_storage.s3.sse.type="SSE-S3"
		mapS3SSE("alertmanager"), mapS3SSE("ruler"),
		// Apply trivial renames and moves of parameters
		PathMapper{PathMappings: cortexRenameMappings},
		// Remap sharding configs
		MapperFunc(updateKVStoreValue),
		// Convert provided memcached service and host to the DNS service discovery format
		mapMemcachedAddresses("query_range.results_cache.cache.memcached_client", "frontend.results_cache.memcached"),
		// Map `-*.s3.url` to `-*.s3.(endpoint|access_key_id|secret_access_key)`
		mapRulerAlertmanagerS3URL("alertmanager.storage", "alertmanager_storage"), mapRulerAlertmanagerS3URL("ruler.storage", "ruler_storage"),
		// Map `-*.s3.bucketnames` and (maybe part of `-*s3.s3.url`) to `-*.s3.bucket-name`
		mapRulerAlertmanagerS3Buckets("alertmanager.storage", "alertmanager_storage"), mapRulerAlertmanagerS3Buckets("ruler.storage", "ruler_storage"),
		// Prevent server.http_listen_port from being updated with a new default (8080) implicitly and always set it to the old default (80)
		setOldDefaultExplicitly("server.http_listen_port"),
		// Manually override the dynamic fields' default values.
		MapperFunc(mapCortexRingInstanceIDDefaults),
		// Set frontend.results_cache.backend when results cache was enabled in cortex
		MapperFunc(mapQueryFrontendBackend),
		// Prevent *_storage.backend from being updated with a new default (filesystem) implicitly and always set it to the old default (s3)
		setOldDefaultExplicitly("blocks_storage.backend"),
		setOldDefaultExplicitly("ruler_storage.backend"),
		setOldDefaultExplicitly("alertmanager_storage.backend"),
		// Prevent activity_tracker.filepath from being updates with a new default (./metrics-activity.log) implicitly and always set it to the old default (./active-query-tracker)
		setOldDefaultWithNewPathExplicitly("querier.active_query_tracker_dir", "activity_tracker.filepath"),
		// Prevent alertmanager.data_dir from being updates with a new default (./data-alertmanager/) implicitly and always set it to the old default (data/)
		setOldDefaultExplicitly("alertmanager.data_dir"),
		// Prevent blocks_storage.filesystem.dir from being updates with a new default (blocks) implicitly and always set it to the old default ("")
		setOldDefaultExplicitly("blocks_storage.filesystem.dir"),
		// Prevent compactor.data_dir from being updates with a new default (./data-compactor/) implicitly and always set it to the old default (./data)
		setOldDefaultExplicitly("compactor.data_dir"),
		// Prevent ruler.rule_path from being updates with a new default (./data-ruler/) implicitly and always set it to the old default (/rules)
		setOldDefaultExplicitly("ruler.rule_path"),
		// Prevent ruler_storage.filesystem.dir from being updates with a new default (ruler) implicitly and always set it to the old default ("")
		setOldDefaultExplicitly("ruler_storage.filesystem.dir"),
	}
}

var cortexRenameMappings = map[string]Mapping{
	"blocks_storage.tsdb.max_exemplars": RenameMapping("limits.max_global_exemplars_per_user"),

	"query_range.results_cache.cache.background.writeback_buffer":     RenameMapping("frontend.results_cache.memcached.max_async_buffer_size"),
	"query_range.results_cache.cache.background.writeback_goroutines": RenameMapping("frontend.results_cache.memcached.max_async_concurrency"),
	"query_range.results_cache.cache.memcached.batch_size":            RenameMapping("frontend.results_cache.memcached.max_get_multi_batch_size"),
	"query_range.results_cache.cache.memcached.parallelism":           RenameMapping("frontend.results_cache.memcached.max_get_multi_concurrency"),
	"query_range.results_cache.cache.memcached_client.addresses":      RenameMapping("frontend.results_cache.memcached.addresses"),
	"query_range.results_cache.cache.memcached_client.max_idle_conns": RenameMapping("frontend.results_cache.memcached.max_idle_connections"),
	"query_range.results_cache.cache.memcached_client.max_item_size":  RenameMapping("frontend.results_cache.memcached.max_item_size"),
	"query_range.results_cache.cache.memcached_client.timeout":        RenameMapping("frontend.results_cache.memcached.timeout"),
	"query_range.results_cache.compression":                           RenameMapping("frontend.results_cache.compression"),

	"alertmanager.cluster.peer_timeout": RenameMapping("alertmanager.peer_timeout"),
	"alertmanager.enable_api":           RenameMapping("alertmanager.enable_api"), // added because CLI flag was renamed
	"ruler.enable_api":                  RenameMapping("ruler.enable_api"),        // added because CLI flag was renamed

	"limits.max_chunks_per_query":      RenameMapping("limits.max_fetched_chunks_per_query"),
	"querier.active_query_tracker_dir": RenameMapping("activity_tracker.filepath"),

	// frontend query-frontend flags have been renamed to query-frontend, all the mappings below are there mostly for posterity (and maybe for logging)
	"frontend.downstream_url":                                RenameMapping("frontend.downstream_url"),
	"frontend.grpc_client_config.backoff_config.max_period":  RenameMapping("frontend.grpc_client_config.backoff_config.max_period"),
	"frontend.grpc_client_config.backoff_config.max_retries": RenameMapping("frontend.grpc_client_config.backoff_config.max_retries"),
	"frontend.grpc_client_config.backoff_config.min_period":  RenameMapping("frontend.grpc_client_config.backoff_config.min_period"),
	"frontend.grpc_client_config.backoff_on_ratelimits":      RenameMapping("frontend.grpc_client_config.backoff_on_ratelimits"),
	"frontend.grpc_client_config.grpc_compression":           RenameMapping("frontend.grpc_client_config.grpc_compression"),
	"frontend.grpc_client_config.max_recv_msg_size":          RenameMapping("frontend.grpc_client_config.max_recv_msg_size"),
	"frontend.grpc_client_config.max_send_msg_size":          RenameMapping("frontend.grpc_client_config.max_send_msg_size"),
	"frontend.grpc_client_config.rate_limit":                 RenameMapping("frontend.grpc_client_config.rate_limit"),
	"frontend.grpc_client_config.rate_limit_burst":           RenameMapping("frontend.grpc_client_config.rate_limit_burst"),
	"frontend.grpc_client_config.tls_ca_path":                RenameMapping("frontend.grpc_client_config.tls_ca_path"),
	"frontend.grpc_client_config.tls_cert_path":              RenameMapping("frontend.grpc_client_config.tls_cert_path"),
	"frontend.grpc_client_config.tls_enabled":                RenameMapping("frontend.grpc_client_config.tls_enabled"),
	"frontend.grpc_client_config.tls_insecure_skip_verify":   RenameMapping("frontend.grpc_client_config.tls_insecure_skip_verify"),
	"frontend.grpc_client_config.tls_key_path":               RenameMapping("frontend.grpc_client_config.tls_key_path"),
	"frontend.grpc_client_config.tls_server_name":            RenameMapping("frontend.grpc_client_config.tls_server_name"),
	"frontend.log_queries_longer_than":                       RenameMapping("frontend.log_queries_longer_than"),
	"frontend.max_body_size":                                 RenameMapping("frontend.max_body_size"),
	"frontend.max_outstanding_per_tenant":                    RenameMapping("frontend.max_outstanding_per_tenant"),
	"frontend.querier_forget_delay":                          RenameMapping("frontend.querier_forget_delay"),
	"frontend.query_stats_enabled":                           RenameMapping("frontend.query_stats_enabled"),
	"frontend.scheduler_address":                             RenameMapping("frontend.scheduler_address"),
	"frontend.scheduler_dns_lookup_period":                   RenameMapping("frontend.scheduler_dns_lookup_period"),
	"frontend.scheduler_worker_concurrency":                  RenameMapping("frontend.scheduler_worker_concurrency"),

	"query_range.align_queries_with_step":       RenameMapping("frontend.align_queries_with_step"),
	"query_range.cache_results":                 RenameMapping("frontend.cache_results"),
	"query_range.max_retries":                   RenameMapping("frontend.max_retries"),
	"query_range.parallelise_shardable_queries": RenameMapping("frontend.parallelize_shardable_queries"),
	"query_range.split_queries_by_interval":     RenameMapping("frontend.split_queries_by_interval"),

	"ingester.lifecycler.availability_zone":                          RenameMapping("ingester.ring.instance_availability_zone"),
	"ingester.lifecycler.final_sleep":                                RenameMapping("ingester.ring.final_sleep"),
	"ingester.lifecycler.heartbeat_period":                           RenameMapping("ingester.ring.heartbeat_period"),
	"ingester.lifecycler.min_ready_duration":                         RenameMapping("ingester.ring.min_ready_duration"),
	"ingester.lifecycler.num_tokens":                                 RenameMapping("ingester.ring.num_tokens"),
	"ingester.lifecycler.observe_period":                             RenameMapping("ingester.ring.observe_period"),
	"ingester.lifecycler.ring.heartbeat_timeout":                     RenameMapping("ingester.ring.heartbeat_timeout"),
	"ingester.lifecycler.ring.kvstore.consul.acl_token":              RenameMapping("ingester.ring.kvstore.consul.acl_token"),
	"ingester.lifecycler.ring.kvstore.consul.consistent_reads":       RenameMapping("ingester.ring.kvstore.consul.consistent_reads"),
	"ingester.lifecycler.ring.kvstore.consul.host":                   RenameMapping("ingester.ring.kvstore.consul.host"),
	"ingester.lifecycler.ring.kvstore.consul.http_client_timeout":    RenameMapping("ingester.ring.kvstore.consul.http_client_timeout"),
	"ingester.lifecycler.ring.kvstore.consul.watch_burst_size":       RenameMapping("ingester.ring.kvstore.consul.watch_burst_size"),
	"ingester.lifecycler.ring.kvstore.consul.watch_rate_limit":       RenameMapping("ingester.ring.kvstore.consul.watch_rate_limit"),
	"ingester.lifecycler.ring.kvstore.etcd.dial_timeout":             RenameMapping("ingester.ring.kvstore.etcd.dial_timeout"),
	"ingester.lifecycler.ring.kvstore.etcd.endpoints":                RenameMapping("ingester.ring.kvstore.etcd.endpoints"),
	"ingester.lifecycler.ring.kvstore.etcd.max_retries":              RenameMapping("ingester.ring.kvstore.etcd.max_retries"),
	"ingester.lifecycler.ring.kvstore.etcd.password":                 RenameMapping("ingester.ring.kvstore.etcd.password"),
	"ingester.lifecycler.ring.kvstore.etcd.tls_ca_path":              RenameMapping("ingester.ring.kvstore.etcd.tls_ca_path"),
	"ingester.lifecycler.ring.kvstore.etcd.tls_cert_path":            RenameMapping("ingester.ring.kvstore.etcd.tls_cert_path"),
	"ingester.lifecycler.ring.kvstore.etcd.tls_enabled":              RenameMapping("ingester.ring.kvstore.etcd.tls_enabled"),
	"ingester.lifecycler.ring.kvstore.etcd.tls_insecure_skip_verify": RenameMapping("ingester.ring.kvstore.etcd.tls_insecure_skip_verify"),
	"ingester.lifecycler.ring.kvstore.etcd.tls_key_path":             RenameMapping("ingester.ring.kvstore.etcd.tls_key_path"),
	"ingester.lifecycler.ring.kvstore.etcd.tls_server_name":          RenameMapping("ingester.ring.kvstore.etcd.tls_server_name"),
	"ingester.lifecycler.ring.kvstore.etcd.username":                 RenameMapping("ingester.ring.kvstore.etcd.username"),
	"ingester.lifecycler.ring.kvstore.multi.mirror_enabled":          RenameMapping("ingester.ring.kvstore.multi.mirror_enabled"),
	"ingester.lifecycler.ring.kvstore.multi.mirror_timeout":          RenameMapping("ingester.ring.kvstore.multi.mirror_timeout"),
	"ingester.lifecycler.ring.kvstore.multi.primary":                 RenameMapping("ingester.ring.kvstore.multi.primary"),
	"ingester.lifecycler.ring.kvstore.multi.secondary":               RenameMapping("ingester.ring.kvstore.multi.secondary"),
	"ingester.lifecycler.ring.kvstore.prefix":                        RenameMapping("ingester.ring.kvstore.prefix"),
	"ingester.lifecycler.ring.kvstore.store":                         RenameMapping("ingester.ring.kvstore.store"),
	"ingester.lifecycler.ring.replication_factor":                    RenameMapping("ingester.ring.replication_factor"),
	"ingester.lifecycler.ring.zone_awareness_enabled":                RenameMapping("ingester.ring.zone_awareness_enabled"),
	"ingester.lifecycler.tokens_file_path":                           RenameMapping("ingester.ring.tokens_file_path"),
	"ingester.lifecycler.unregister_on_shutdown":                     RenameMapping("ingester.ring.unregister_on_shutdown"),
	notInYaml + ".ingester-lifecycler-id":                            RenameMapping("ingester.ring.instance_id"),

	"auth_enabled": RenameMapping("multitenancy_enabled"),

	// limits.max_query_length replaced with limits.max_partial_query_length in 2.5
	"limits.max_query_length": RenameMapping("limits.max_partial_query_length"),
}

func updateKVStoreValue(source, target Parameters) error {
	storeFields := map[string]string{
		"alertmanager.sharding_ring.kvstore.store":  "alertmanager.sharding_enabled",
		"compactor.sharding_ring.kvstore.store":     "compactor.sharding_enabled",
		"ruler.ring.kvstore.store":                  "ruler.enable_sharding",
		"store_gateway.sharding_ring.kvstore.store": "store_gateway.sharding_enabled",
		"distributor.ring.kvstore.store":            "limits.ingestion_rate_strategy",
		"ingester.lifecycler.ring.kvstore.store":    "",
	}

	// If KV store is NOT set by user, but sharding for given component is enabled, we must explicitly set KV store to "consul" (old default)
	for storePath, shardingEnabledPath := range storeFields {
		kvStore, err := source.GetValue(storePath)
		if err != nil {
			return errors.Wrapf(err, "could not find %s", storePath)
		}

		if !kvStore.IsUnset() {
			// set explicitly, don't change it.
			continue
		}

		ringUsed := false
		targetStorePath := storePath

		switch storePath {
		case "ingester.lifecycler.ring.kvstore.store":
			targetStorePath = "ingester.ring.kvstore.store"

			// For ingesters, check if consul was actually configured. If not (maybe this isn't even config for ingester),
			// let's ignore this ring.
			consul, err := source.GetValue("ingester.lifecycler.ring.kvstore.consul.host")
			if err != nil {
				return errors.Wrapf(err, "could not find %s", shardingEnabledPath)
			}
			if !consul.IsUnset() {
				// Only update configuration if consul is actually configured.
				ringUsed = true
			}

		case "distributor.ring.kvstore.store":
			enabled, err := source.GetValue(shardingEnabledPath)
			if err != nil {
				return errors.Wrapf(err, "could not find %s", shardingEnabledPath)
			}

			ringUsed = enabled.AsString() == "global" // Using of distributor ring was enabled by setting limits.ingestion_rate_strategy to "global".

		default:
			enabled, err := source.GetValue(shardingEnabledPath)
			if err != nil {
				return errors.Wrapf(err, "could not find %s", shardingEnabledPath)
			}

			ringUsed = enabled.AsBool()
		}

		// If ring is not used, ignore this KV store config.
		if !ringUsed {
			continue
		}

		// At this point we know:
		// 1) KV store was not configured (= consul)
		// 2) Ring is actually used
		// => We must set "consul" in new config.

		err = target.SetValue(targetStorePath, StringValue("consul"))
		if err != nil {
			return errors.Wrapf(err, "failed to update %s", targetStorePath)
		}
	}

	return nil
}

func alertmanagerURLMapperFunc(source, target Parameters) error {
	amDiscovery, err := source.GetValue("ruler.enable_alertmanager_discovery")
	if err != nil {
		return errors.Wrap(err, "could not convert ruler.enable_alertmanager_discovery")
	}
	if !amDiscovery.AsBool() {
		return nil
	}

	amURL, err := source.GetValue("ruler.alertmanager_url")
	if err != nil {
		return errors.Wrap(err, "could not get ruler.alertmanager_url")
	}

	amURLs := strings.Split(amURL.AsString(), ",")
	for i := range amURLs {
		amURLs[i] = "dnssrvnoa+" + amURLs[i]
	}
	return target.SetValue("ruler.alertmanager_url", StringValue(strings.Join(amURLs, ",")))
}

// rulerStorageMapperFunc returns a MapperFunc that maps alertmanager.storage and alertmanager_storage to alertmanager_storage.
// Values from alertmanager.storage take precedence.
func alertmanagerStorageMapperFunc(source, target Parameters) error {

	pathRenames := map[string]string{
		"alertmanager.storage.azure.account_key":                      "alertmanager_storage.azure.account_key",
		"alertmanager.storage.azure.account_name":                     "alertmanager_storage.azure.account_name",
		"alertmanager.storage.azure.container_name":                   "alertmanager_storage.azure.container_name",
		"alertmanager.storage.azure.max_retries":                      "alertmanager_storage.azure.max_retries",
		"alertmanager.storage.gcs.bucket_name":                        "alertmanager_storage.gcs.bucket_name",
		"alertmanager.storage.local.path":                             "alertmanager_storage.local.path",
		"alertmanager.storage.s3.access_key_id":                       "alertmanager_storage.s3.access_key_id",
		"alertmanager.storage.s3.endpoint":                            "alertmanager_storage.s3.endpoint",
		"alertmanager.storage.s3.http_config.idle_conn_timeout":       "alertmanager_storage.s3.http.idle_conn_timeout",
		"alertmanager.storage.s3.http_config.insecure_skip_verify":    "alertmanager_storage.s3.http.insecure_skip_verify",
		"alertmanager.storage.s3.http_config.response_header_timeout": "alertmanager_storage.s3.http.response_header_timeout",
		"alertmanager.storage.s3.insecure":                            "alertmanager_storage.s3.insecure",
		"alertmanager.storage.s3.region":                              "alertmanager_storage.s3.region",
		"alertmanager.storage.s3.secret_access_key":                   "alertmanager_storage.s3.secret_access_key",
		"alertmanager.storage.s3.signature_version":                   "alertmanager_storage.s3.signature_version",
		"alertmanager.storage.s3.sse.kms_encryption_context":          "alertmanager_storage.s3.sse.kms_encryption_context",
		"alertmanager.storage.s3.sse.kms_key_id":                      "alertmanager_storage.s3.sse.kms_key_id",
		"alertmanager.storage.s3.sse.type":                            "alertmanager_storage.s3.sse.type",
		"alertmanager.storage.type":                                   "alertmanager_storage.backend",
	}

	return mapDotStorage(pathRenames, source, target)
}

// rulerStorageMapperFunc returns a MapperFunc that maps ruler.storage and ruler_storage to ruler_storage.
// Values from ruler.storage take precedence.
func rulerStorageMapperFunc(source, target Parameters) error {
	pathRenames := map[string]string{
		"ruler.storage.azure.account_key":                      "ruler_storage.azure.account_key",
		"ruler.storage.azure.account_name":                     "ruler_storage.azure.account_name",
		"ruler.storage.azure.container_name":                   "ruler_storage.azure.container_name",
		"ruler.storage.azure.max_retries":                      "ruler_storage.azure.max_retries",
		"ruler.storage.gcs.bucket_name":                        "ruler_storage.gcs.bucket_name",
		"ruler.storage.local.directory":                        "ruler_storage.local.directory",
		"ruler.storage.s3.access_key_id":                       "ruler_storage.s3.access_key_id",
		"ruler.storage.s3.endpoint":                            "ruler_storage.s3.endpoint",
		"ruler.storage.s3.http_config.idle_conn_timeout":       "ruler_storage.s3.http.idle_conn_timeout",
		"ruler.storage.s3.http_config.insecure_skip_verify":    "ruler_storage.s3.http.insecure_skip_verify",
		"ruler.storage.s3.http_config.response_header_timeout": "ruler_storage.s3.http.response_header_timeout",
		"ruler.storage.s3.insecure":                            "ruler_storage.s3.insecure",
		"ruler.storage.s3.region":                              "ruler_storage.s3.region",
		"ruler.storage.s3.secret_access_key":                   "ruler_storage.s3.secret_access_key",
		"ruler.storage.s3.signature_version":                   "ruler_storage.s3.signature_version",
		"ruler.storage.s3.sse.kms_encryption_context":          "ruler_storage.s3.sse.kms_encryption_context",
		"ruler.storage.s3.sse.kms_key_id":                      "ruler_storage.s3.sse.kms_key_id",
		"ruler.storage.s3.sse.type":                            "ruler_storage.s3.sse.type",
		"ruler.storage.swift.auth_url":                         "ruler_storage.swift.auth_url",
		"ruler.storage.swift.auth_version":                     "ruler_storage.swift.auth_version",
		"ruler.storage.swift.connect_timeout":                  "ruler_storage.swift.connect_timeout",
		"ruler.storage.swift.container_name":                   "ruler_storage.swift.container_name",
		"ruler.storage.swift.domain_id":                        "ruler_storage.swift.domain_id",
		"ruler.storage.swift.domain_name":                      "ruler_storage.swift.domain_name",
		"ruler.storage.swift.max_retries":                      "ruler_storage.swift.max_retries",
		"ruler.storage.swift.password":                         "ruler_storage.swift.password",
		"ruler.storage.swift.project_domain_id":                "ruler_storage.swift.project_domain_id",
		"ruler.storage.swift.project_domain_name":              "ruler_storage.swift.project_domain_name",
		"ruler.storage.swift.project_id":                       "ruler_storage.swift.project_id",
		"ruler.storage.swift.project_name":                     "ruler_storage.swift.project_name",
		"ruler.storage.swift.region_name":                      "ruler_storage.swift.region_name",
		"ruler.storage.swift.request_timeout":                  "ruler_storage.swift.request_timeout",
		"ruler.storage.swift.user_domain_id":                   "ruler_storage.swift.user_domain_id",
		"ruler.storage.swift.user_domain_name":                 "ruler_storage.swift.user_domain_name",
		"ruler.storage.swift.user_id":                          "ruler_storage.swift.user_id",
		"ruler.storage.swift.username":                         "ruler_storage.swift.username",
		"ruler.storage.type":                                   "ruler_storage.backend",
	}

	return mapDotStorage(pathRenames, source, target)
}

// Mappings run both for default values and user-provided values. We do the mapping from old
// to new if and only if the value in the config is different from the default value:
// this is only the case when mapping user-provided values, not defaults.
func differentFromDefault(p Parameters, path string) bool {
	val, err1 := p.GetValue(path)
	defaultVal, err2 := p.GetDefaultValue(path)

	return err1 == nil && err2 == nil && !val.IsUnset() && !val.Equals(defaultVal)
}

func mapDotStorage(pathRenames map[string]string, source, target Parameters) error {
	mapper := &PathMapper{PathMappings: map[string]Mapping{}}

	for dotStoragePath, storagePath := range pathRenames {
		// if the ruler.storage was set, then use that in the final config
		if differentFromDefault(source, dotStoragePath) {
			mapper.PathMappings[dotStoragePath] = RenameMapping(storagePath)
			continue
		}

		// if the ruler_storage was set to something other than the default, then we
		// take that value as the one in the final config.
		if differentFromDefault(source, storagePath) {
			mapper.PathMappings[storagePath] = RenameMapping(storagePath)
		}
	}

	return mapper.DoMap(source, target)
}

// mapS3SSE maps (alertmanager|ruler).storage.s3.sse_encryption to (alertmanager|ruler)_storage.s3.sse.type.
// prefix should be either "alertmanager" or "ruler". If <prefix>.storage.s3.sse_encryption was true,
// it is replaced by alertmanager_storage.s3.sse.type="SSE-S3"
func mapS3SSE(prefix string) MapperFunc {
	return func(source, target Parameters) error {
		var (
			sseEncryptionPath = prefix + ".storage.s3.sse_encryption"
			sseTypePath       = prefix + "_storage.s3.sse.type"
		)

		sseWasEnabledVal, err := source.GetValue(sseEncryptionPath)
		if err != nil {
			return err
		}
		sseWasEnabled := sseWasEnabledVal.AsBool()
		if sseWasEnabled && target.MustGetValue(sseTypePath).IsUnset() {
			return target.SetValue(sseTypePath, StringValue(s3.SSES3))
		}

		return nil
	}
}

func mapRulerAlertmanagerS3URL(dotStoragePath, storagePath string) MapperFunc {
	return func(source, target Parameters) error {
		var (
			oldS3URLPath             = dotStoragePath + ".s3.s3"
			newS3SecretAccessKeyPath = storagePath + ".s3.secret_access_key"
			newS3KeyIDPath           = storagePath + ".s3.access_key_id"
			newS3EndpointPath        = storagePath + ".s3.endpoint"
		)

		if differentFromDefault(target, newS3EndpointPath) {
			// User has already set the s3 endpoint to something, we won't override it.
			return nil
		}

		s3URLVal, _ := source.GetValue(oldS3URLPath)
		s3URL := s3URLVal.AsURL()
		if s3URL.URL == nil {
			return nil
		}
		if s3URL.Scheme == "inmemory" {
			return errors.New(oldS3URLPath + ": inmemory s3 storage is no longer supported, please provide a real s3 endpoint")
		}
		if s3URL.User != nil {
			username := s3URL.User.Username()
			password, _ := s3URL.User.Password()
			setIfNonEmpty := func(_ Parameters, path, val string) error {
				currentVal, _ := target.GetValue(path)
				currentStr := currentVal.AsString()
				if val == "" || currentStr != "" {
					// Values set by the user take precedence over ones in the URL
					return nil
				}
				err := target.SetValue(path, StringValue(val))
				if err != nil {
					return err
				}
				return nil
			}

			err := setIfNonEmpty(target, newS3SecretAccessKeyPath, password)
			if err != nil {
				return err
			}

			err = setIfNonEmpty(target, newS3KeyIDPath, username)
			if err != nil {
				return err
			}
		}

		err := target.SetValue(newS3EndpointPath, StringValue(s3URL.Host))
		if err != nil {
			return err
		}
		return nil
	}
}

func mapRulerAlertmanagerS3Buckets(dotStoragePath, storagePath string) Mapper {
	return MapperFunc(func(source, target Parameters) error {
		var (
			oldBucketNamesPath = dotStoragePath + ".s3.bucketnames"
			oldS3URLPath       = dotStoragePath + ".s3.s3"
			newS3BucketPath    = storagePath + ".s3.bucket_name"
		)

		if differentFromDefault(target, newS3BucketPath) {
			return nil
		}

		bucketNamesVal, _ := source.GetValue(oldBucketNamesPath)
		bucketNames := bucketNamesVal.AsString()
		if strings.Contains(bucketNames, ",") {
			return errors.New(oldBucketNamesPath + ": multiple bucket names cannot be converted, please provide only a single bucket name")
		}

		if bucketNames == "" {
			s3URLVal, _ := source.GetValue(oldS3URLPath)
			s3URL := s3URLVal.AsURL()
			if s3URL.URL != nil {
				bucketNames = strings.TrimPrefix(s3URL.Path, "/")
			}
		}
		if bucketNames == "" {
			return nil
		}

		return target.SetValue(newS3BucketPath, StringValue(bucketNames))
	})
}

// mapMemcachedAddresses maps query_range...memcached_client.host and .service to a DNS Service Discovery format
// address. This should preserve the behaviour in cortex v1.11.0:
// https://github.com/cortexproject/cortex/blob/43c646ba3ff906e80a6a1812f2322a0c276e9deb/pkg/chunk/cache/memcached_client.go#L242-L258
// Also applies to GEM and graphite querier caches
func mapMemcachedAddresses(oldPrefix, newPrefix string) MapperFunc {
	return func(source, target Parameters) error {
		presetAddressesVal, err := source.GetValue(oldPrefix + ".addresses")
		if err != nil {
			return err
		}
		if presetAddressesVal.AsString() != "" {
			return nil // respect already set values of addresses
		}

		service, hostname := source.MustGetValue(oldPrefix+".service"), source.MustGetValue(oldPrefix+".host")
		if service.IsUnset() || hostname.IsUnset() {
			return nil
		}
		newAddress := fmt.Sprintf("dnssrvnoa+_%s._tcp.%s", service.AsString(), hostname.AsString())

		return target.SetValue(newPrefix+".addresses", StringValue(newAddress))
	}
}

func mapCortexInstanceInterfaceNames() Mapper {
	ifaceNames := map[string]string{
		"alertmanager.sharding_ring.instance_interface_names":  "alertmanager.sharding_ring.instance_interface_names",
		"compactor.sharding_ring.instance_interface_names":     "compactor.sharding_ring.instance_interface_names",
		"distributor.ring.instance_interface_names":            "distributor.ring.instance_interface_names",
		"frontend.instance_interface_names":                    "frontend.instance_interface_names",
		"ingester.lifecycler.interface_names":                  "ingester.ring.instance_interface_names",
		"ruler.ring.instance_interface_names":                  "ruler.ring.instance_interface_names",
		"store_gateway.sharding_ring.instance_interface_names": "store_gateway.sharding_ring.instance_interface_names",
	}
	return mapInstanceInterfaceNames(ifaceNames)
}

func mapInstanceInterfaceNames(ifaceNames map[string]string) Mapper {
	return MapperFunc(func(source, target Parameters) error {
		errs := multierror.New()
		for sourcePath, targetPath := range ifaceNames {
			// We want to update these interface_names to use the new autodetection in mimir
			// if and only if they match the old default AND the user has provided -update-defaults.
			// To do that we set the new default to nil. If -update-defaults is set, it will replace the
			// [eth0, en0] default value with nil. pruneNils will then delete that nil parameter.
			err := target.SetDefaultValue(targetPath, Nil)
			if err != nil {
				errs.Add(err)
				continue
			}
			instanceNamesVal, _ := source.GetValue(sourcePath)
			if !instanceNamesVal.IsUnset() {
				// The user has set the value to something, we want to keep that.
				// But also when mapping defaults this restores the old default when mapping defaults after we've set it to Nil above.
				errs.Add(target.SetValue(targetPath, instanceNamesVal))
				continue
			}
			errs.Add(target.Delete(targetPath))
		}
		return errs.Err()
	})
}

func setOldDefaultExplicitly(path string) Mapper {
	return setOldDefaultWithNewPathExplicitly(path, path)
}

func setOldDefaultWithNewPathExplicitly(oldPath, newPath string) Mapper {
	return MapperFunc(func(source, target Parameters) error {
		v, err := source.GetValue(oldPath)
		if err != nil {
			return err
		}

		if v.IsUnset() || !differentFromDefault(source, oldPath) {
			err = target.SetValue(newPath, source.MustGetDefaultValue(oldPath))
			// We set the default again after the value itself because when prepareSourceDefaults is mapping defaults
			// `SetValue` actually sets the default value.
			// Also set the source default to Nil, so that when updating defaults this parameter isn't affected
			err2 := target.SetDefaultValue(newPath, Nil)
			return multierror.New(err, err2).Err()
		}

		return nil
	})
}

func mapQueryFrontendBackend(source, target Parameters) error {
	v, _ := source.GetValue("query_range.cache_results")
	if v.AsBool() {
		return target.SetValue("frontend.results_cache.backend", StringValue("memcached"))
	}
	return nil
}

func mapCortexRingInstanceIDDefaults(_, target Parameters) error {
	return multierror.New(
		target.SetDefaultValue("alertmanager.sharding_ring.instance_id", Nil),
		target.SetDefaultValue("compactor.sharding_ring.instance_id", Nil),
		target.SetDefaultValue("distributor.ring.instance_id", Nil),
		target.SetDefaultValue("ingester.ring.instance_id", Nil),
		target.SetDefaultValue("ruler.ring.instance_id", Nil),
		target.SetDefaultValue("store_gateway.sharding_ring.instance_id", Nil),
	).Err()
}

// YAML Paths for config options removed since Cortex 1.11.0.
var removedConfigPaths = append(gemRemovedConfigPath, []string{
	"flusher.concurrent_flushes",                            // -flusher.concurrent-flushes
	"flusher.flush_op_timeout",                              // -flusher.flush-op-timeout
	"flusher.wal_dir",                                       // -flusher.wal-dir
	"ingester.chunk_age_jitter",                             // -ingester.chunk-age-jitter
	"ingester.concurrent_flushes",                           // -ingester.concurrent-flushes
	"ingester.flush_op_timeout",                             // -ingester.flush-op-timeout
	"ingester.flush_period",                                 // -ingester.flush-period
	"ingester.lifecycler.join_after",                        // -ingester.ring.join-after
	"ingester.max_chunk_age",                                // -ingester.max-chunk-age
	"ingester.max_chunk_idle_time",                          // -ingester.max-chunk-idle
	"ingester.max_stale_chunk_idle_time",                    // -ingester.max-stale-chunk-idle
	"ingester.max_transfer_retries",                         // -ingester.max-transfer-retries
	"ingester.retain_period",                                // -ingester.retain-period
	"ingester.spread_flushes",                               // -ingester.spread-flushes
	"ingester.walconfig.checkpoint_duration",                // -ingester.checkpoint-duration
	"ingester.walconfig.checkpoint_enabled",                 // -ingester.checkpoint-enabled
	"ingester.walconfig.flush_on_shutdown_with_wal_enabled", // -ingester.flush-on-shutdown-with-wal-enabled
	"ingester.walconfig.recover_from_wal",                   // -ingester.recover-from-wal
	"ingester.walconfig.wal_dir",                            // -ingester.wal-dir
	"ingester.walconfig.wal_enabled",                        // -ingester.wal-enabled
	"limits.max_series_per_query",                           // -ingester.max-series-per-query
	"limits.min_chunk_length",                               // -ingester.min-chunk-length
	"querier.second_store_engine",                           // -querier.second-store-engine
	"querier.use_second_store_before_time",                  // -querier.use-second-store-before-time
	"storage.engine",                                        // -store.engine

	// All table-manager flags.
	"table_manager.chunk_tables_provisioning.enable_inactive_throughput_on_demand_mode", // -table-manager.chunk-table.inactive-enable-ondemand-throughput-mode
	"table_manager.chunk_tables_provisioning.enable_ondemand_throughput_mode",           // -table-manager.chunk-table.enable-ondemand-throughput-mode
	"table_manager.chunk_tables_provisioning.inactive_read_scale.enabled",               // -table-manager.chunk-table.inactive-read-throughput.scale.enabled
	"table_manager.chunk_tables_provisioning.inactive_read_scale.in_cooldown",           // -table-manager.chunk-table.inactive-read-throughput.scale.in-cooldown
	"table_manager.chunk_tables_provisioning.inactive_read_scale.max_capacity",          // -table-manager.chunk-table.inactive-read-throughput.scale.max-capacity
	"table_manager.chunk_tables_provisioning.inactive_read_scale.min_capacity",          // -table-manager.chunk-table.inactive-read-throughput.scale.min-capacity
	"table_manager.chunk_tables_provisioning.inactive_read_scale.out_cooldown",          // -table-manager.chunk-table.inactive-read-throughput.scale.out-cooldown
	"table_manager.chunk_tables_provisioning.inactive_read_scale.role_arn",              // -table-manager.chunk-table.inactive-read-throughput.scale.role-arn
	"table_manager.chunk_tables_provisioning.inactive_read_scale.target",                // -table-manager.chunk-table.inactive-read-throughput.scale.target-value
	"table_manager.chunk_tables_provisioning.inactive_read_scale_lastn",                 // -table-manager.chunk-table.inactive-read-throughput.scale-last-n
	"table_manager.chunk_tables_provisioning.inactive_read_throughput",                  // -table-manager.chunk-table.inactive-read-throughput
	"table_manager.chunk_tables_provisioning.inactive_write_scale.enabled",              // -table-manager.chunk-table.inactive-write-throughput.scale.enabled
	"table_manager.chunk_tables_provisioning.inactive_write_scale.in_cooldown",          // -table-manager.chunk-table.inactive-write-throughput.scale.in-cooldown
	"table_manager.chunk_tables_provisioning.inactive_write_scale.max_capacity",         // -table-manager.chunk-table.inactive-write-throughput.scale.max-capacity
	"table_manager.chunk_tables_provisioning.inactive_write_scale.min_capacity",         // -table-manager.chunk-table.inactive-write-throughput.scale.min-capacity
	"table_manager.chunk_tables_provisioning.inactive_write_scale.out_cooldown",         // -table-manager.chunk-table.inactive-write-throughput.scale.out-cooldown
	"table_manager.chunk_tables_provisioning.inactive_write_scale.role_arn",             // -table-manager.chunk-table.inactive-write-throughput.scale.role-arn
	"table_manager.chunk_tables_provisioning.inactive_write_scale.target",               // -table-manager.chunk-table.inactive-write-throughput.scale.target-value
	"table_manager.chunk_tables_provisioning.inactive_write_scale_lastn",                // -table-manager.chunk-table.inactive-write-throughput.scale-last-n
	"table_manager.chunk_tables_provisioning.inactive_write_throughput",                 // -table-manager.chunk-table.inactive-write-throughput
	"table_manager.chunk_tables_provisioning.provisioned_read_throughput",               // -table-manager.chunk-table.read-throughput
	"table_manager.chunk_tables_provisioning.provisioned_write_throughput",              // -table-manager.chunk-table.write-throughput
	"table_manager.chunk_tables_provisioning.read_scale.enabled",                        // -table-manager.chunk-table.read-throughput.scale.enabled
	"table_manager.chunk_tables_provisioning.read_scale.in_cooldown",                    // -table-manager.chunk-table.read-throughput.scale.in-cooldown
	"table_manager.chunk_tables_provisioning.read_scale.max_capacity",                   // -table-manager.chunk-table.read-throughput.scale.max-capacity
	"table_manager.chunk_tables_provisioning.read_scale.min_capacity",                   // -table-manager.chunk-table.read-throughput.scale.min-capacity
	"table_manager.chunk_tables_provisioning.read_scale.out_cooldown",                   // -table-manager.chunk-table.read-throughput.scale.out-cooldown
	"table_manager.chunk_tables_provisioning.read_scale.role_arn",                       // -table-manager.chunk-table.read-throughput.scale.role-arn
	"table_manager.chunk_tables_provisioning.read_scale.target",                         // -table-manager.chunk-table.read-throughput.scale.target-value
	"table_manager.chunk_tables_provisioning.write_scale.enabled",                       // -table-manager.chunk-table.write-throughput.scale.enabled
	"table_manager.chunk_tables_provisioning.write_scale.in_cooldown",                   // -table-manager.chunk-table.write-throughput.scale.in-cooldown
	"table_manager.chunk_tables_provisioning.write_scale.max_capacity",                  // -table-manager.chunk-table.write-throughput.scale.max-capacity
	"table_manager.chunk_tables_provisioning.write_scale.min_capacity",                  // -table-manager.chunk-table.write-throughput.scale.min-capacity
	"table_manager.chunk_tables_provisioning.write_scale.out_cooldown",                  // -table-manager.chunk-table.write-throughput.scale.out-cooldown
	"table_manager.chunk_tables_provisioning.write_scale.role_arn",                      // -table-manager.chunk-table.write-throughput.scale.role-arn
	"table_manager.chunk_tables_provisioning.write_scale.target",                        // -table-manager.chunk-table.write-throughput.scale.target-value
	"table_manager.creation_grace_period",                                               // -table-manager.periodic-table.grace-period
	"table_manager.index_tables_provisioning.enable_inactive_throughput_on_demand_mode", // -table-manager.index-table.inactive-enable-ondemand-throughput-mode
	"table_manager.index_tables_provisioning.enable_ondemand_throughput_mode",           // -table-manager.index-table.enable-ondemand-throughput-mode
	"table_manager.index_tables_provisioning.inactive_read_scale.enabled",               // -table-manager.index-table.inactive-read-throughput.scale.enabled
	"table_manager.index_tables_provisioning.inactive_read_scale.in_cooldown",           // -table-manager.index-table.inactive-read-throughput.scale.in-cooldown
	"table_manager.index_tables_provisioning.inactive_read_scale.max_capacity",          // -table-manager.index-table.inactive-read-throughput.scale.max-capacity
	"table_manager.index_tables_provisioning.inactive_read_scale.min_capacity",          // -table-manager.index-table.inactive-read-throughput.scale.min-capacity
	"table_manager.index_tables_provisioning.inactive_read_scale.out_cooldown",          // -table-manager.index-table.inactive-read-throughput.scale.out-cooldown
	"table_manager.index_tables_provisioning.inactive_read_scale.role_arn",              // -table-manager.index-table.inactive-read-throughput.scale.role-arn
	"table_manager.index_tables_provisioning.inactive_read_scale.target",                // -table-manager.index-table.inactive-read-throughput.scale.target-value
	"table_manager.index_tables_provisioning.inactive_read_scale_lastn",                 // -table-manager.index-table.inactive-read-throughput.scale-last-n
	"table_manager.index_tables_provisioning.inactive_read_throughput",                  // -table-manager.index-table.inactive-read-throughput
	"table_manager.index_tables_provisioning.inactive_write_scale.enabled",              // -table-manager.index-table.inactive-write-throughput.scale.enabled
	"table_manager.index_tables_provisioning.inactive_write_scale.in_cooldown",          // -table-manager.index-table.inactive-write-throughput.scale.in-cooldown
	"table_manager.index_tables_provisioning.inactive_write_scale.max_capacity",         // -table-manager.index-table.inactive-write-throughput.scale.max-capacity
	"table_manager.index_tables_provisioning.inactive_write_scale.min_capacity",         // -table-manager.index-table.inactive-write-throughput.scale.min-capacity
	"table_manager.index_tables_provisioning.inactive_write_scale.out_cooldown",         // -table-manager.index-table.inactive-write-throughput.scale.out-cooldown
	"table_manager.index_tables_provisioning.inactive_write_scale.role_arn",             // -table-manager.index-table.inactive-write-throughput.scale.role-arn
	"table_manager.index_tables_provisioning.inactive_write_scale.target",               // -table-manager.index-table.inactive-write-throughput.scale.target-value
	"table_manager.index_tables_provisioning.inactive_write_scale_lastn",                // -table-manager.index-table.inactive-write-throughput.scale-last-n
	"table_manager.index_tables_provisioning.inactive_write_throughput",                 // -table-manager.index-table.inactive-write-throughput
	"table_manager.index_tables_provisioning.provisioned_read_throughput",               // -table-manager.index-table.read-throughput
	"table_manager.index_tables_provisioning.provisioned_write_throughput",              // -table-manager.index-table.write-throughput
	"table_manager.index_tables_provisioning.read_scale.enabled",                        // -table-manager.index-table.read-throughput.scale.enabled
	"table_manager.index_tables_provisioning.read_scale.in_cooldown",                    // -table-manager.index-table.read-throughput.scale.in-cooldown
	"table_manager.index_tables_provisioning.read_scale.max_capacity",                   // -table-manager.index-table.read-throughput.scale.max-capacity
	"table_manager.index_tables_provisioning.read_scale.min_capacity",                   // -table-manager.index-table.read-throughput.scale.min-capacity
	"table_manager.index_tables_provisioning.read_scale.out_cooldown",                   // -table-manager.index-table.read-throughput.scale.out-cooldown
	"table_manager.index_tables_provisioning.read_scale.role_arn",                       // -table-manager.index-table.read-throughput.scale.role-arn
	"table_manager.index_tables_provisioning.read_scale.target",                         // -table-manager.index-table.read-throughput.scale.target-value
	"table_manager.index_tables_provisioning.write_scale.enabled",                       // -table-manager.index-table.write-throughput.scale.enabled
	"table_manager.index_tables_provisioning.write_scale.in_cooldown",                   // -table-manager.index-table.write-throughput.scale.in-cooldown
	"table_manager.index_tables_provisioning.write_scale.max_capacity",                  // -table-manager.index-table.write-throughput.scale.max-capacity
	"table_manager.index_tables_provisioning.write_scale.min_capacity",                  // -table-manager.index-table.write-throughput.scale.min-capacity
	"table_manager.index_tables_provisioning.write_scale.out_cooldown",                  // -table-manager.index-table.write-throughput.scale.out-cooldown
	"table_manager.index_tables_provisioning.write_scale.role_arn",                      // -table-manager.index-table.write-throughput.scale.role-arn
	"table_manager.index_tables_provisioning.write_scale.target",                        // -table-manager.index-table.write-throughput.scale.target-value
	"table_manager.poll_interval",                                                       // -table-manager.poll-interval
	"table_manager.retention_deletes_enabled",                                           // -table-manager.retention-deletes-enabled
	"table_manager.retention_period",                                                    // -table-manager.retention-period
	"table_manager.throughput_updates_disabled",                                         // -table-manager.throughput-updates-disabled

	// All `-deletes.*` flags
	"storage.delete_store.requests_table_name",                                // -deletes.requests-table-name
	"storage.delete_store.store",                                              // -deletes.store
	"storage.delete_store.table_provisioning.enable_ondemand_throughput_mode", // -deletes.table.enable-ondemand-throughput-mode
	"storage.delete_store.table_provisioning.provisioned_read_throughput",     // -deletes.table.read-throughput
	"storage.delete_store.table_provisioning.provisioned_write_throughput",    // -deletes.table.write-throughput
	"storage.delete_store.table_provisioning.read_scale.enabled",              // -deletes.table.read-throughput.scale.enabled
	"storage.delete_store.table_provisioning.read_scale.in_cooldown",          // -deletes.table.read-throughput.scale.in-cooldown
	"storage.delete_store.table_provisioning.read_scale.max_capacity",         // -deletes.table.read-throughput.scale.max-capacity
	"storage.delete_store.table_provisioning.read_scale.min_capacity",         // -deletes.table.read-throughput.scale.min-capacity
	"storage.delete_store.table_provisioning.read_scale.out_cooldown",         // -deletes.table.read-throughput.scale.out-cooldown
	"storage.delete_store.table_provisioning.read_scale.role_arn",             // -deletes.table.read-throughput.scale.role-arn
	"storage.delete_store.table_provisioning.read_scale.target",               // -deletes.table.read-throughput.scale.target-value
	"storage.delete_store.table_provisioning.tags",                            // -deletes.table.tags
	"storage.delete_store.table_provisioning.write_scale.enabled",             // -deletes.table.write-throughput.scale.enabled
	"storage.delete_store.table_provisioning.write_scale.in_cooldown",         // -deletes.table.write-throughput.scale.in-cooldown
	"storage.delete_store.table_provisioning.write_scale.max_capacity",        // -deletes.table.write-throughput.scale.max-capacity
	"storage.delete_store.table_provisioning.write_scale.min_capacity",        // -deletes.table.write-throughput.scale.min-capacity
	"storage.delete_store.table_provisioning.write_scale.out_cooldown",        // -deletes.table.write-throughput.scale.out-cooldown
	"storage.delete_store.table_provisioning.write_scale.role_arn",            // -deletes.table.write-throughput.scale.role-arn
	"storage.delete_store.table_provisioning.write_scale.target",              // -deletes.table.write-throughput.scale.target-value

	// All `-purger.*` flags
	"purger.delete_request_cancel_period", // -purger.delete-request-cancel-period
	"purger.enable",                       // -purger.enable
	"purger.num_workers",                  // -purger.num-workers
	"purger.object_store_type",            // -purger.object-store-type

	// All `-metrics.*` flags
	"storage.aws.dynamodb.metrics.ignore_throttle_below", // -metrics.ignore-throttle-below
	"storage.aws.dynamodb.metrics.queue_length_query",    // -metrics.queue-length-query
	"storage.aws.dynamodb.metrics.read_error_query",      // -metrics.read-error-query
	"storage.aws.dynamodb.metrics.read_usage_query",      // -metrics.read-usage-query
	"storage.aws.dynamodb.metrics.scale_up_factor",       // -metrics.scale-up-factor
	"storage.aws.dynamodb.metrics.target_queue_length",   // -metrics.target-queue-length
	"storage.aws.dynamodb.metrics.url",                   // -metrics.url
	"storage.aws.dynamodb.metrics.write_throttle_query",  // -metrics.write-throttle-query
	"storage.aws.dynamodb.metrics.write_usage_query",     // -metrics.usage-query

	// All `-dynamodb.*` flags
	"storage.aws.dynamodb.api_limit",                  // -dynamodb.api-limit
	"storage.aws.dynamodb.backoff_config.max_period",  // -dynamodb.max-backoff
	"storage.aws.dynamodb.backoff_config.max_retries", // -dynamodb.max-retries
	"storage.aws.dynamodb.backoff_config.min_period",  // -dynamodb.min-backoff
	"storage.aws.dynamodb.chunk_gang_size",            // -dynamodb.chunk-gang-size
	"storage.aws.dynamodb.chunk_get_max_parallelism",  // -dynamodb.chunk.get-max-parallelism
	"storage.aws.dynamodb.dynamodb_url",               // -dynamodb.url
	"storage.aws.dynamodb.throttle_limit",             // -dynamodb.throttle-limit

	// All `-s3.*` flags
	"storage.aws.access_key_id",                       // -s3.access-key-id
	"storage.aws.bucketnames",                         // -s3.buckets
	"storage.aws.endpoint",                            // -s3.endpoint
	"storage.aws.http_config.idle_conn_timeout",       // -s3.http.idle-conn-timeout
	"storage.aws.http_config.insecure_skip_verify",    // -s3.http.insecure-skip-verify
	"storage.aws.http_config.response_header_timeout", // -s3.http.response-header-timeout
	"storage.aws.insecure",                            // -s3.insecure
	"storage.aws.region",                              // -s3.region
	"storage.aws.s3",                                  // -s3.url
	"storage.aws.s3forcepathstyle",                    // -s3.force-path-style
	"storage.aws.secret_access_key",                   // -s3.secret-access-key
	"storage.aws.signature_version",                   // -s3.signature-version
	"storage.aws.sse.kms_encryption_context",          // -s3.sse.kms-encryption-context
	"storage.aws.sse.kms_key_id",                      // -s3.sse.kms-key-id
	"storage.aws.sse.type",                            // -s3.sse.type
	"storage.aws.sse_encryption",                      // -s3.sse-encryption

	// All `-azure.*` flags
	"storage.azure.account_key",          // -azure.account-key
	"storage.azure.account_name",         // -azure.account-name
	"storage.azure.container_name",       // -azure.container-name
	"storage.azure.download_buffer_size", // -azure.download-buffer-size
	"storage.azure.environment",          // -azure.environment
	"storage.azure.max_retries",          // -azure.max-retries
	"storage.azure.max_retry_delay",      // -azure.max-retry-delay
	"storage.azure.min_retry_delay",      // -azure.min-retry-delay
	"storage.azure.request_timeout",      // -azure.request-timeout
	"storage.azure.upload_buffer_count",  // -azure.download-buffer-count
	"storage.azure.upload_buffer_size",   // -azure.upload-buffer-size

	// All `-bigtable.*` flags
	"storage.bigtable.grpc_client_config.backoff_config.max_period",  // -bigtable.backoff-max-period
	"storage.bigtable.grpc_client_config.backoff_config.max_retries", // -bigtable.backoff-retries
	"storage.bigtable.grpc_client_config.backoff_config.min_period",  // -bigtable.backoff-min-period
	"storage.bigtable.grpc_client_config.backoff_on_ratelimits",      // -bigtable.backoff-on-ratelimits
	"storage.bigtable.grpc_client_config.grpc_compression",           // -bigtable.grpc-compression
	"storage.bigtable.grpc_client_config.max_recv_msg_size",          // -bigtable.grpc-max-recv-msg-size
	"storage.bigtable.grpc_client_config.max_send_msg_size",          // -bigtable.grpc-max-send-msg-size
	"storage.bigtable.grpc_client_config.rate_limit",                 // -bigtable.grpc-client-rate-limit
	"storage.bigtable.grpc_client_config.rate_limit_burst",           // -bigtable.grpc-client-rate-limit-burst
	"storage.bigtable.grpc_client_config.tls_ca_path",                // -bigtable.tls-ca-path
	"storage.bigtable.grpc_client_config.tls_cert_path",              // -bigtable.tls-cert-path
	"storage.bigtable.grpc_client_config.tls_enabled",                // -bigtable.tls-enabled
	"storage.bigtable.grpc_client_config.tls_insecure_skip_verify",   // -bigtable.tls-insecure-skip-verify
	"storage.bigtable.grpc_client_config.tls_key_path",               // -bigtable.tls-key-path
	"storage.bigtable.grpc_client_config.tls_server_name",            // -bigtable.tls-server-name
	"storage.bigtable.instance",                                      // -bigtable.instance
	"storage.bigtable.project",                                       // -bigtable.project
	"storage.bigtable.table_cache_enabled",                           // -bigtable.table-cache.enabled
	"storage.bigtable.table_cache_expiration",                        // -bigtable.table-cache.expiration

	// All `-gcs.*` flags
	"storage.gcs.bucket_name",       // -gcs.bucketname
	"storage.gcs.chunk_buffer_size", // -gcs.chunk-buffer-size
	"storage.gcs.enable_opencensus", // -gcs.enable-opencensus
	"storage.gcs.request_timeout",   // -gcs.request-timeout

	// All `-cassandra.*` flags
	"storage.cassandra.CA_path",                     // -cassandra.ca-path
	"storage.cassandra.SSL",                         // -cassandra.ssl
	"storage.cassandra.addresses",                   // -cassandra.addresses
	"storage.cassandra.auth",                        // -cassandra.auth
	"storage.cassandra.connect_timeout",             // -cassandra.connect-timeout
	"storage.cassandra.consistency",                 // -cassandra.consistency
	"storage.cassandra.convict_hosts_on_failure",    // -cassandra.convict-hosts-on-failure
	"storage.cassandra.custom_authenticators",       // -cassandra.custom-authenticator
	"storage.cassandra.disable_initial_host_lookup", // -cassandra.disable-initial-host-lookup
	"storage.cassandra.host_selection_policy",       // -cassandra.host-selection-policy
	"storage.cassandra.host_verification",           // -cassandra.host-verification
	"storage.cassandra.keyspace",                    // -cassandra.keyspace
	"storage.cassandra.max_retries",                 // -cassandra.max-retries
	"storage.cassandra.num_connections",             // -cassandra.num-connections
	"storage.cassandra.password",                    // -cassandra.password
	"storage.cassandra.password_file",               // -cassandra.password-file
	"storage.cassandra.port",                        // -cassandra.port
	"storage.cassandra.query_concurrency",           // -cassandra.query-concurrency
	"storage.cassandra.reconnect_interval",          // -cassandra.reconnent-interval
	"storage.cassandra.replication_factor",          // -cassandra.replication-factor
	"storage.cassandra.retry_max_backoff",           // -cassandra.retry-max-backoff
	"storage.cassandra.retry_min_backoff",           // -cassandra.retry-min-backoff
	"storage.cassandra.table_options",               // -cassandra.table-options
	"storage.cassandra.timeout",                     // -cassandra.timeout
	"storage.cassandra.tls_cert_path",               // -cassandra.tls-cert-path
	"storage.cassandra.tls_key_path",                // -cassandra.tls-key-path
	"storage.cassandra.username",                    // -cassandra.username

	// All `-boltdb.*` flags
	"storage.boltdb.directory", // -boltdb.dir

	// All `-local.*` flags
	"storage.filesystem.directory", // -local.chunk-directory

	// All `-swift.*` flags
	"storage.swift.auth_url",            // -swift.auth-url
	"storage.swift.auth_version",        // -swift.auth-version
	"storage.swift.connect_timeout",     // -swift.connect-timeout
	"storage.swift.container_name",      // -swift.container-name
	"storage.swift.domain_id",           // -swift.domain-id
	"storage.swift.domain_name",         // -swift.domain-name
	"storage.swift.max_retries",         // -swift.max-retries
	"storage.swift.password",            // -swift.password
	"storage.swift.project_domain_id",   // -swift.project-domain-id
	"storage.swift.project_domain_name", // -swift.project-domain-name
	"storage.swift.project_id",          // -swift.project-id
	"storage.swift.project_name",        // -swift.project-name
	"storage.swift.region_name",         // -swift.region-name
	"storage.swift.request_timeout",     // -swift.request-timeout
	"storage.swift.user_domain_id",      // -swift.user-domain-id
	"storage.swift.user_domain_name",    // -swift.user-domain-name
	"storage.swift.user_id",             // -swift.user-id
	"storage.swift.username",            // -swift.username

	// All `-store.*` flags except `-store.engine`, `-store.max-query-length`, `-store.max-labels-query-length`
	"chunk_store.cache_lookups_older_than",                                                        // -store.cache-lookups-older-than
	"chunk_store.chunk_cache_config.background.writeback_buffer",                                  // -store.chunks-cache.background.write-back-buffer
	"chunk_store.chunk_cache_config.background.writeback_goroutines",                              // -store.chunks-cache.background.write-back-concurrency
	"chunk_store.chunk_cache_config.default_validity",                                             // -store.chunks-cache.default-validity
	"chunk_store.chunk_cache_config.enable_fifocache",                                             // -store.chunks-cache.cache.enable-fifocache
	"chunk_store.chunk_cache_config.fifocache.max_size_bytes",                                     // -store.chunks-cache.fifocache.max-size-bytes
	"chunk_store.chunk_cache_config.fifocache.max_size_items",                                     // -store.chunks-cache.fifocache.max-size-items
	"chunk_store.chunk_cache_config.fifocache.size",                                               // -store.chunks-cache.fifocache.size
	"chunk_store.chunk_cache_config.fifocache.validity",                                           // -store.chunks-cache.fifocache.duration
	"chunk_store.chunk_cache_config.memcached.batch_size",                                         // -store.chunks-cache.memcached.batchsize
	"chunk_store.chunk_cache_config.memcached.expiration",                                         // -store.chunks-cache.memcached.expiration
	"chunk_store.chunk_cache_config.memcached.parallelism",                                        // -store.chunks-cache.memcached.parallelism
	"chunk_store.chunk_cache_config.memcached_client.addresses",                                   // -store.chunks-cache.memcached.addresses
	"chunk_store.chunk_cache_config.memcached_client.circuit_breaker_consecutive_failures",        // -store.chunks-cache.memcached.circuit-breaker-consecutive-failures
	"chunk_store.chunk_cache_config.memcached_client.circuit_breaker_interval",                    // -store.chunks-cache.memcached.circuit-breaker-interval
	"chunk_store.chunk_cache_config.memcached_client.circuit_breaker_timeout",                     // -store.chunks-cache.memcached.circuit-breaker-timeout
	"chunk_store.chunk_cache_config.memcached_client.consistent_hash",                             // -store.chunks-cache.memcached.consistent-hash
	"chunk_store.chunk_cache_config.memcached_client.host",                                        // -store.chunks-cache.memcached.hostname
	"chunk_store.chunk_cache_config.memcached_client.max_idle_conns",                              // -store.chunks-cache.memcached.max-idle-conns
	"chunk_store.chunk_cache_config.memcached_client.max_item_size",                               // -store.chunks-cache.memcached.max-item-size
	"chunk_store.chunk_cache_config.memcached_client.service",                                     // -store.chunks-cache.memcached.service
	"chunk_store.chunk_cache_config.memcached_client.timeout",                                     // -store.chunks-cache.memcached.timeout
	"chunk_store.chunk_cache_config.memcached_client.update_interval",                             // -store.chunks-cache.memcached.update-interval
	"chunk_store.chunk_cache_config.redis.db",                                                     // -store.chunks-cache.redis.db
	"chunk_store.chunk_cache_config.redis.endpoint",                                               // -store.chunks-cache.redis.endpoint
	"chunk_store.chunk_cache_config.redis.expiration",                                             // -store.chunks-cache.redis.expiration
	"chunk_store.chunk_cache_config.redis.idle_timeout",                                           // -store.chunks-cache.redis.idle-timeout
	"chunk_store.chunk_cache_config.redis.master_name",                                            // -store.chunks-cache.redis.master-name
	"chunk_store.chunk_cache_config.redis.max_connection_age",                                     // -store.chunks-cache.redis.max-connection-age
	"chunk_store.chunk_cache_config.redis.password",                                               // -store.chunks-cache.redis.password
	"chunk_store.chunk_cache_config.redis.pool_size",                                              // -store.chunks-cache.redis.pool-size
	"chunk_store.chunk_cache_config.redis.timeout",                                                // -store.chunks-cache.redis.timeout
	"chunk_store.chunk_cache_config.redis.tls_enabled",                                            // -store.chunks-cache.redis.tls-enabled
	"chunk_store.chunk_cache_config.redis.tls_insecure_skip_verify",                               // -store.chunks-cache.redis.tls-insecure-skip-verify
	"chunk_store.write_dedupe_cache_config.background.writeback_buffer",                           // -store.index-cache-write.background.write-back-buffer
	"chunk_store.write_dedupe_cache_config.background.writeback_goroutines",                       // -store.index-cache-write.background.write-back-concurrency
	"chunk_store.write_dedupe_cache_config.default_validity",                                      // -store.index-cache-write.default-validity
	"chunk_store.write_dedupe_cache_config.enable_fifocache",                                      // -store.index-cache-write.cache.enable-fifocache
	"chunk_store.write_dedupe_cache_config.fifocache.max_size_bytes",                              // -store.index-cache-write.fifocache.max-size-bytes
	"chunk_store.write_dedupe_cache_config.fifocache.max_size_items",                              // -store.index-cache-write.fifocache.max-size-items
	"chunk_store.write_dedupe_cache_config.fifocache.size",                                        // -store.index-cache-write.fifocache.size
	"chunk_store.write_dedupe_cache_config.fifocache.validity",                                    // -store.index-cache-write.fifocache.duration
	"chunk_store.write_dedupe_cache_config.memcached.batch_size",                                  // -store.index-cache-write.memcached.batchsize
	"chunk_store.write_dedupe_cache_config.memcached.expiration",                                  // -store.index-cache-write.memcached.expiration
	"chunk_store.write_dedupe_cache_config.memcached.parallelism",                                 // -store.index-cache-write.memcached.parallelism
	"chunk_store.write_dedupe_cache_config.memcached_client.addresses",                            // -store.index-cache-write.memcached.addresses
	"chunk_store.write_dedupe_cache_config.memcached_client.circuit_breaker_consecutive_failures", // -store.index-cache-write.memcached.circuit-breaker-consecutive-failures
	"chunk_store.write_dedupe_cache_config.memcached_client.circuit_breaker_interval",             // -store.index-cache-write.memcached.circuit-breaker-interval
	"chunk_store.write_dedupe_cache_config.memcached_client.circuit_breaker_timeout",              // -store.index-cache-write.memcached.circuit-breaker-timeout
	"chunk_store.write_dedupe_cache_config.memcached_client.consistent_hash",                      // -store.index-cache-write.memcached.consistent-hash
	"chunk_store.write_dedupe_cache_config.memcached_client.host",                                 // -store.index-cache-write.memcached.hostname
	"chunk_store.write_dedupe_cache_config.memcached_client.max_idle_conns",                       // -store.index-cache-write.memcached.max-idle-conns
	"chunk_store.write_dedupe_cache_config.memcached_client.max_item_size",                        // -store.index-cache-write.memcached.max-item-size
	"chunk_store.write_dedupe_cache_config.memcached_client.service",                              // -store.index-cache-write.memcached.service
	"chunk_store.write_dedupe_cache_config.memcached_client.timeout",                              // -store.index-cache-write.memcached.timeout
	"chunk_store.write_dedupe_cache_config.memcached_client.update_interval",                      // -store.index-cache-write.memcached.update-interval
	"chunk_store.write_dedupe_cache_config.redis.db",                                              // -store.index-cache-write.redis.db
	"chunk_store.write_dedupe_cache_config.redis.endpoint",                                        // -store.index-cache-write.redis.endpoint
	"chunk_store.write_dedupe_cache_config.redis.expiration",                                      // -store.index-cache-write.redis.expiration
	"chunk_store.write_dedupe_cache_config.redis.idle_timeout",                                    // -store.index-cache-write.redis.idle-timeout
	"chunk_store.write_dedupe_cache_config.redis.master_name",                                     // -store.index-cache-write.redis.master-name
	"chunk_store.write_dedupe_cache_config.redis.max_connection_age",                              // -store.index-cache-write.redis.max-connection-age
	"chunk_store.write_dedupe_cache_config.redis.password",                                        // -store.index-cache-write.redis.password
	"chunk_store.write_dedupe_cache_config.redis.pool_size",                                       // -store.index-cache-write.redis.pool-size
	"chunk_store.write_dedupe_cache_config.redis.timeout",                                         // -store.index-cache-write.redis.timeout
	"chunk_store.write_dedupe_cache_config.redis.tls_enabled",                                     // -store.index-cache-write.redis.tls-enabled
	"chunk_store.write_dedupe_cache_config.redis.tls_insecure_skip_verify",                        // -store.index-cache-write.redis.tls-insecure-skip-verify
	"limits.cardinality_limit",                                                                    // -store.cardinality-limit
	"limits.max_chunks_per_query",                                                                 // -store.query-chunk-limit
	"storage.index_cache_validity",                                                                // -store.index-cache-validity
	"storage.index_queries_cache_config.background.writeback_buffer",                              // -store.index-cache-read.background.write-back-buffer
	"storage.index_queries_cache_config.background.writeback_goroutines",                          // -store.index-cache-read.background.write-back-concurrency
	"storage.index_queries_cache_config.default_validity",                                         // -store.index-cache-read.default-validity
	"storage.index_queries_cache_config.enable_fifocache",                                         // -store.index-cache-read.cache.enable-fifocache
	"storage.index_queries_cache_config.fifocache.max_size_bytes",                                 // -store.index-cache-read.fifocache.max-size-bytes
	"storage.index_queries_cache_config.fifocache.max_size_items",                                 // -store.index-cache-read.fifocache.max-size-items
	"storage.index_queries_cache_config.fifocache.size",                                           // -store.index-cache-read.fifocache.size
	"storage.index_queries_cache_config.fifocache.validity",                                       // -store.index-cache-read.fifocache.duration
	"storage.index_queries_cache_config.memcached.batch_size",                                     // -store.index-cache-read.memcached.batchsize
	"storage.index_queries_cache_config.memcached.expiration",                                     // -store.index-cache-read.memcached.expiration
	"storage.index_queries_cache_config.memcached.parallelism",                                    // -store.index-cache-read.memcached.parallelism
	"storage.index_queries_cache_config.memcached_client.addresses",                               // -store.index-cache-read.memcached.addresses
	"storage.index_queries_cache_config.memcached_client.circuit_breaker_consecutive_failures",    // -store.index-cache-read.memcached.circuit-breaker-consecutive-failures
	"storage.index_queries_cache_config.memcached_client.circuit_breaker_interval",                // -store.index-cache-read.memcached.circuit-breaker-interval
	"storage.index_queries_cache_config.memcached_client.circuit_breaker_timeout",                 // -store.index-cache-read.memcached.circuit-breaker-timeout
	"storage.index_queries_cache_config.memcached_client.consistent_hash",                         // -store.index-cache-read.memcached.consistent-hash
	"storage.index_queries_cache_config.memcached_client.host",                                    // -store.index-cache-read.memcached.hostname
	"storage.index_queries_cache_config.memcached_client.max_idle_conns",                          // -store.index-cache-read.memcached.max-idle-conns
	"storage.index_queries_cache_config.memcached_client.max_item_size",                           // -store.index-cache-read.memcached.max-item-size
	"storage.index_queries_cache_config.memcached_client.service",                                 // -store.index-cache-read.memcached.service
	"storage.index_queries_cache_config.memcached_client.timeout",                                 // -store.index-cache-read.memcached.timeout
	"storage.index_queries_cache_config.memcached_client.update_interval",                         // -store.index-cache-read.memcached.update-interval
	"storage.index_queries_cache_config.redis.db",                                                 // -store.index-cache-read.redis.db
	"storage.index_queries_cache_config.redis.endpoint",                                           // -store.index-cache-read.redis.endpoint
	"storage.index_queries_cache_config.redis.expiration",                                         // -store.index-cache-read.redis.expiration
	"storage.index_queries_cache_config.redis.idle_timeout",                                       // -store.index-cache-read.redis.idle-timeout
	"storage.index_queries_cache_config.redis.master_name",                                        // -store.index-cache-read.redis.master-name
	"storage.index_queries_cache_config.redis.max_connection_age",                                 // -store.index-cache-read.redis.max-connection-age
	"storage.index_queries_cache_config.redis.password",                                           // -store.index-cache-read.redis.password
	"storage.index_queries_cache_config.redis.pool_size",                                          // -store.index-cache-read.redis.pool-size
	"storage.index_queries_cache_config.redis.timeout",                                            // -store.index-cache-read.redis.timeout
	"storage.index_queries_cache_config.redis.tls_enabled",                                        // -store.index-cache-read.redis.tls-enabled
	"storage.index_queries_cache_config.redis.tls_insecure_skip_verify",                           // -store.index-cache-read.redis.tls-insecure-skip-verify

	// All `-grpc-store.*` flags
	"storage.grpc_store.server_address", // -grpc-store.server-address

	// Alertmanager legacy sharding removed
	"alertmanager.cluster.advertise_address",  // -alertmanager.cluster.advertise-address
	"alertmanager.cluster.gossip_interval",    // -alertmanager.cluster.gossip-interval
	"alertmanager.cluster.listen_address",     // -alertmanager.cluster.listen-address
	"alertmanager.cluster.peers",              // -alertmanager.cluster.peers
	"alertmanager.cluster.push_pull_interval", // -alertmanager.cluster.push-pull-interval
	"alertmanager.sharding_enabled",           // -alertmanager.sharding-enabled

	// alertmanager.storage removal
	"alertmanager.storage.azure.download_buffer_size", // -alertmanager.storage.azure.download-buffer-size
	"alertmanager.storage.azure.environment",          // -alertmanager.storage.azure.environment
	"alertmanager.storage.azure.max_retry_delay",      // -alertmanager.storage.azure.max-retry-delay
	"alertmanager.storage.azure.min_retry_delay",      // -alertmanager.storage.azure.min-retry-delay
	"alertmanager.storage.azure.request_timeout",      // -alertmanager.storage.azure.request-timeout
	"alertmanager.storage.azure.upload_buffer_count",  // -alertmanager.storage.azure.download-buffer-count
	"alertmanager.storage.azure.upload_buffer_size",   // -alertmanager.storage.azure.upload-buffer-size
	"alertmanager.storage.gcs.chunk_buffer_size",      // -alertmanager.storage.gcs.chunk-buffer-size
	"alertmanager.storage.gcs.enable_opencensus",      // -alertmanager.storage.gcs.enable-opencensus
	"alertmanager.storage.gcs.request_timeout",        // -alertmanager.storage.gcs.request-timeout
	"alertmanager.storage.s3.s3forcepathstyle",        // -alertmanager.storage.s3.force-path-style

	// ruler.storage removal
	"ruler.storage.azure.download_buffer_size", // -ruler.storage.azure.download-buffer-size
	"ruler.storage.azure.environment",          // -ruler.storage.azure.environment
	"ruler.storage.azure.max_retry_delay",      // -ruler.storage.azure.max-retry-delay
	"ruler.storage.azure.min_retry_delay",      // -ruler.storage.azure.min-retry-delay
	"ruler.storage.azure.request_timeout",      // -ruler.storage.azure.request-timeout
	"ruler.storage.azure.upload_buffer_count",  // -ruler.storage.azure.download-buffer-count
	"ruler.storage.azure.upload_buffer_size",   // -ruler.storage.azure.upload-buffer-size
	"ruler.storage.gcs.chunk_buffer_size",      // -ruler.storage.gcs.chunk-buffer-size
	"ruler.storage.gcs.enable_opencensus",      // -ruler.storage.gcs.enable-opencensus
	"ruler.storage.gcs.request_timeout",        // -ruler.storage.gcs.request-timeout
	"ruler.storage.s3.s3forcepathstyle",        // -ruler.storage.s3.force-path-style

	// Removed support for configdb
	"alertmanager.storage.configdb.client_timeout",           // -alertmanager.configs.client-timeout
	"alertmanager.storage.configdb.configs_api_url",          // -alertmanager.configs.url
	"alertmanager.storage.configdb.tls_ca_path",              // -alertmanager.configs.tls-ca-path
	"alertmanager.storage.configdb.tls_cert_path",            // -alertmanager.configs.tls-cert-path
	"alertmanager.storage.configdb.tls_insecure_skip_verify", // -alertmanager.configs.tls-insecure-skip-verify
	"alertmanager.storage.configdb.tls_key_path",             // -alertmanager.configs.tls-key-path
	"alertmanager.storage.configdb.tls_server_name",          // -alertmanager.configs.tls-server-name
	"alertmanager_storage.configdb.client_timeout",           // -alertmanager-storage.configs.client-timeout
	"alertmanager_storage.configdb.configs_api_url",          // -alertmanager-storage.configs.url
	"alertmanager_storage.configdb.tls_ca_path",              // -alertmanager-storage.configs.tls-ca-path
	"alertmanager_storage.configdb.tls_cert_path",            // -alertmanager-storage.configs.tls-cert-path
	"alertmanager_storage.configdb.tls_insecure_skip_verify", // -alertmanager-storage.configs.tls-insecure-skip-verify
	"alertmanager_storage.configdb.tls_key_path",             // -alertmanager-storage.configs.tls-key-path
	"alertmanager_storage.configdb.tls_server_name",          // -alertmanager-storage.configs.tls-server-name
	"ruler.storage.configdb.client_timeout",                  // -ruler-storage.configs.client-timeout
	"ruler.storage.configdb.configs_api_url",                 // -ruler-storage.configs.url
	"ruler.storage.configdb.tls_ca_path",                     // -ruler-storage.configs.tls-ca-path
	"ruler.storage.configdb.tls_cert_path",                   // -ruler-storage.configs.tls-cert-path
	"ruler.storage.configdb.tls_insecure_skip_verify",        // -ruler-storage.configs.tls-insecure-skip-verify
	"ruler.storage.configdb.tls_key_path",                    // -ruler-storage.configs.tls-key-path
	"ruler.storage.configdb.tls_server_name",                 // -ruler-storage.configs.tls-server-name
	"ruler_storage.configdb.client_timeout",                  // -ruler-storage.configs.client-timeout
	"ruler_storage.configdb.configs_api_url",                 // -ruler-storage.configs.url
	"ruler_storage.configdb.tls_ca_path",                     // -ruler-storage.configs.tls-ca-path
	"ruler_storage.configdb.tls_cert_path",                   // -ruler-storage.configs.tls-cert-path
	"ruler_storage.configdb.tls_insecure_skip_verify",        // -ruler-storage.configs.tls-insecure-skip-verify
	"ruler_storage.configdb.tls_key_path",                    // -ruler-storage.configs.tls-key-path
	"ruler_storage.configdb.tls_server_name",                 // -ruler-storage.configs.tls-server-name

	// query_range and frontend merged
	"query_range.results_cache.cache.default_validity",                                      // -frontend.default-validity
	"query_range.results_cache.cache.enable_fifocache",                                      // -frontend.cache.enable-fifocache
	"query_range.results_cache.cache.fifocache.max_size_bytes",                              // -frontend.fifocache.max-size-bytes
	"query_range.results_cache.cache.fifocache.max_size_items",                              // -frontend.fifocache.max-size-items
	"query_range.results_cache.cache.fifocache.size",                                        // -frontend.fifocache.size
	"query_range.results_cache.cache.fifocache.validity",                                    // -frontend.fifocache.duration
	"query_range.results_cache.cache.memcached.expiration",                                  // -frontend.memcached.expiration
	"query_range.results_cache.cache.memcached_client.circuit_breaker_consecutive_failures", // -frontend.memcached.circuit-breaker-consecutive-failures
	"query_range.results_cache.cache.memcached_client.circuit_breaker_interval",             // -frontend.memcached.circuit-breaker-interval
	"query_range.results_cache.cache.memcached_client.circuit_breaker_timeout",              // -frontend.memcached.circuit-breaker-timeout
	"query_range.results_cache.cache.memcached_client.consistent_hash",                      // -frontend.memcached.consistent-hash
	"query_range.results_cache.cache.memcached_client.update_interval",                      // -frontend.memcached.update-interval
	"query_range.results_cache.cache.redis.db",                                              // -frontend.redis.db
	"query_range.results_cache.cache.redis.endpoint",                                        // -frontend.redis.endpoint
	"query_range.results_cache.cache.redis.expiration",                                      // -frontend.redis.expiration
	"query_range.results_cache.cache.redis.idle_timeout",                                    // -frontend.redis.idle-timeout
	"query_range.results_cache.cache.redis.master_name",                                     // -frontend.redis.master-name
	"query_range.results_cache.cache.redis.max_connection_age",                              // -frontend.redis.max-connection-age
	"query_range.results_cache.cache.redis.password",                                        // -frontend.redis.password
	"query_range.results_cache.cache.redis.pool_size",                                       // -frontend.redis.pool-size
	"query_range.results_cache.cache.redis.timeout",                                         // -frontend.redis.timeout
	"query_range.results_cache.cache.redis.tls_enabled",                                     // -frontend.redis.tls-enabled
	"query_range.results_cache.cache.redis.tls_insecure_skip_verify",                        // -frontend.redis.tls-insecure-skip-verify

	// The rest
	"alertmanager.auto_webhook_root",         // -alertmanager.configs.auto-webhook-root
	"api.response_compression_enabled",       // -api.response-compression-enabled
	"compactor.sharding_enabled",             // -compactor.sharding-enabled
	"compactor.sharding_strategy",            // -compactor.sharding-strategy
	"distributor.extra_queue_delay",          // -distributor.extra-query-delay
	"distributor.shard_by_all_labels",        // -distributor.shard-by-all-labels
	"distributor.sharding_strategy",          // -distributor.sharding-strategy
	"frontend_worker.match_max_concurrent",   // -querier.worker-match-max-concurrent
	"frontend_worker.parallelism",            // -querier.worker-parallelism
	"http_prefix",                            // -http.prefix
	"limits.enforce_metric_name",             // -validation.enforce-metric-name
	"limits.ingestion_rate_strategy",         // -distributor.ingestion-rate-limit-strategy
	"limits.max_samples_per_query",           // -ingester.max-samples-per-query
	"limits.reject_old_samples",              // -validation.reject-old-samples
	"limits.reject_old_samples_max_age",      // -validation.reject-old-samples.max-age
	"querier.at_modifier_enabled",            // -querier.at-modifier-enabled
	"querier.ingester_streaming",             // -querier.ingester-streaming
	"querier.query_store_for_labels_enabled", // -querier.query-store-for-labels-enabled
	"querier.store_gateway_addresses",        // -querier.store-gateway-addresses
	"ruler.enable_alertmanager_v2",           // -ruler.alertmanager-use-v2
	"ruler.enable_sharding",                  // -ruler.enable-sharding
	"ruler.sharding_strategy",                // -ruler.sharding-strategy
	"store_gateway.sharding_enabled",         // -store-gateway.sharding-enabled
	"store_gateway.sharding_strategy",        // -store-gateway.sharding-strategy

	// Removed in 2.1, 2.2 and 2.3
	"distributor.extend_writes",                          // -distributor.extend-writes
	"querier.shuffle_sharding_ingesters_lookback_period", // -querier.shuffle-sharding-ingesters-lookback-period
	"ruler.flush_period",                                 // -ruler.flush-period
	"ruler.search_pending_for",                           // -ruler.search-pending-for

	// Removed in 2.4
	"ingester.lifecycler.join_after", // -ingester.join-after

	// Removed in 2.5
	"blocks_storage.tsdb.block_ranges_period", // -blocks-storage.tsdb.block-ranges-period
}...)

// CLI options removed since Cortex 1.10.0. These flags only existed as CLI Flags, and were not included in YAML Config.
var removedCLIOptions = []string{
	"event.sample-rate",
	"frontend.cache-split-interval",
	"ingester-client.expected-labels",
	"ingester-client.expected-samples-per-series",
	"ingester-client.expected-timeseries",
	"ingester.chunk-encoding",
	"ingester.lifecycler.addr",
	"ingester.lifecycler.id",
	"ingester.lifecycler.port",
	"querier.query-parallelism",
	"schema-config-file",
}
