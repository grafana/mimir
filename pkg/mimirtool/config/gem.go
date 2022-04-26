// SPDX-License-Identifier: AGPL-3.0-only

package config

// GEM170ToGEM200Mapper maps from gem-1.7.0 to gem-2.0.0 configurations
func GEM170ToGEM200Mapper() Mapper {
	nonExistentGEMPaths := map[string]struct{}{
		"blocks_storage.tsdb.max_exemplars":         {},
		"query_range.parallelise_shardable_queries": {},
	}
	gemRenames := map[string]Mapping{
		"graphite.querier.metric_name_cache.background.writeback_buffer":     RenameMapping("graphite.querier.metric_name_cache.memcached.max_async_buffer_size"),
		"graphite.querier.metric_name_cache.background.writeback_goroutines": RenameMapping("graphite.querier.metric_name_cache.memcached.max_async_concurrency"),
		"graphite.querier.metric_name_cache.memcached.batch_size":            RenameMapping("graphite.querier.metric_name_cache.memcached.max_get_multi_batch_size"),
		"graphite.querier.metric_name_cache.memcached.parallelism":           RenameMapping("graphite.querier.metric_name_cache.memcached.max_get_multi_concurrency"),
		"graphite.querier.metric_name_cache.memcached_client.addresses":      RenameMapping("graphite.querier.metric_name_cache.memcached.addresses"),
		"graphite.querier.metric_name_cache.memcached_client.max_idle_conns": RenameMapping("graphite.querier.metric_name_cache.memcached.max_idle_connections"),
		"graphite.querier.metric_name_cache.memcached_client.max_item_size":  RenameMapping("graphite.querier.metric_name_cache.memcached.max_item_size"),
		"graphite.querier.metric_name_cache.memcached_client.timeout":        RenameMapping("graphite.querier.metric_name_cache.memcached.timeout"),

		"graphite.querier.aggregation_cache.background.writeback_buffer":     RenameMapping("graphite.querier.aggregation_cache.memcached.max_async_buffer_size"),
		"graphite.querier.aggregation_cache.background.writeback_goroutines": RenameMapping("graphite.querier.aggregation_cache.memcached.max_async_concurrency"),
		"graphite.querier.aggregation_cache.memcached.batch_size":            RenameMapping("graphite.querier.aggregation_cache.memcached.max_get_multi_batch_size"),
		"graphite.querier.aggregation_cache.memcached.parallelism":           RenameMapping("graphite.querier.aggregation_cache.memcached.max_get_multi_concurrency"),
		"graphite.querier.aggregation_cache.memcached_client.addresses":      RenameMapping("graphite.querier.aggregation_cache.memcached.addresses"),
		"graphite.querier.aggregation_cache.memcached_client.max_idle_conns": RenameMapping("graphite.querier.aggregation_cache.memcached.max_idle_connections"),
		"graphite.querier.aggregation_cache.memcached_client.max_item_size":  RenameMapping("graphite.querier.aggregation_cache.memcached.max_item_size"),
		"graphite.querier.aggregation_cache.memcached_client.timeout":        RenameMapping("graphite.querier.aggregation_cache.memcached.timeout"),

		"query_range.cache_unaligned_requests": RenameMapping("frontend.cache_unaligned_requests"),

		"gateway.proxy.graphite.enable_keepalive":         RenameMapping("gateway.proxy.graphite_querier.enable_keepalive"),
		"gateway.proxy.graphite.read_timeout":             RenameMapping("gateway.proxy.graphite_querier.read_timeout"),
		"gateway.proxy.graphite.tls_ca_path":              RenameMapping("gateway.proxy.graphite_querier.tls_ca_path"),
		"gateway.proxy.graphite.tls_cert_path":            RenameMapping("gateway.proxy.graphite_querier.tls_cert_path"),
		"gateway.proxy.graphite.tls_enabled":              RenameMapping("gateway.proxy.graphite_querier.tls_enabled"),
		"gateway.proxy.graphite.tls_insecure_skip_verify": RenameMapping("gateway.proxy.graphite_querier.tls_insecure_skip_verify"),
		"gateway.proxy.graphite.tls_key_path":             RenameMapping("gateway.proxy.graphite_querier.tls_key_path"),
		"gateway.proxy.graphite.tls_server_name":          RenameMapping("gateway.proxy.graphite_querier.tls_server_name"),
		"gateway.proxy.graphite.url":                      RenameMapping("gateway.proxy.graphite_querier.url"),
		"gateway.proxy.graphite.write_timeout":            RenameMapping("gateway.proxy.graphite_querier.write_timeout"),
	}
	for path, mapping := range cortexRenameMappings {
		if _, notInGEM := nonExistentGEMPaths[path]; notInGEM {
			continue
		}
		gemRenames[path] = mapping
	}

	return MultiMapper{
		mapGEMInstanceInterfaceNames(),
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
		PathMapper{PathMappings: gemRenames},
		// Remap sharding configs
		MapperFunc(updateKVStoreValue),
		// Convert provided memcached service and host to the DNS service discovery format
		mapMemcachedAddresses("query_range.results_cache.cache.memcached_client", "frontend.results_cache.memcached"),
		mapMemcachedAddresses("graphite.querier.metric_name_cache.memcached_client", "graphite.querier.metric_name_cache.memcached"),
		mapMemcachedAddresses("graphite.querier.aggregation_cache.memcached_client", "graphite.querier.aggregation_cache.memcached"),

		// Map `-*.s3.url` to `-*.s3.(endpoint|access_key_id|secret_access_key)`
		mapRulerAlertmanagerS3URL("alertmanager.storage", "alertmanager_storage"), mapRulerAlertmanagerS3URL("ruler.storage", "ruler_storage"),
		// Map `-*.s3.bucketnames` and (maybe part of `-*s3.s3.url`) to `-*.s3.bucket-name`
		mapRulerAlertmanagerS3Buckets("alertmanager.storage", "alertmanager_storage"), mapRulerAlertmanagerS3Buckets("ruler.storage", "ruler_storage"),
		// Prevent server.http_listen_port from being updated with a new default and always output it.
		setOldDefaultExplicitly("server.http_listen_port"),
		// Prevent auth.type from being updated with a new default (enterprise) implicitly and always set it to the old default (trust)
		setOldDefaultExplicitly("auth.type"),
		// Set frontend.results_cache.backend when results cache was enabled in cortex
		MapperFunc(mapQueryFrontendBackend),
		// Manually override the dynamic fields' default values.
		MapperFunc(mapCortexRingInstanceIDDefaults),
		MapperFunc(mapAdminAPIRingInstanceIDDefaults),
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
		// Prevent graphite.querier.schemas.backend from being updated with a new default (filesystem) implicitly and always set it to the old default (s3)
		setOldDefaultExplicitly("graphite.querier.schemas.backend"),
	}
}

func mapGEMInstanceInterfaceNames() Mapper {
	ifaceNames := map[string]string{
		"admin_api.leader_election.ring.instance_interface_names": "admin_api.leader_election.ring.instance_interface_names",
		"alertmanager.sharding_ring.instance_interface_names":     "alertmanager.sharding_ring.instance_interface_names",
		"compactor.sharding_ring.instance_interface_names":        "compactor.sharding_ring.instance_interface_names",
		"distributor.ring.instance_interface_names":               "distributor.ring.instance_interface_names",
		"frontend.instance_interface_names":                       "frontend.instance_interface_names",
		"ingester.lifecycler.interface_names":                     "ingester.ring.instance_interface_names",
		"ruler.ring.instance_interface_names":                     "ruler.ring.instance_interface_names",
		"store_gateway.sharding_ring.instance_interface_names":    "store_gateway.sharding_ring.instance_interface_names",
	}
	return mapInstanceInterfaceNames(ifaceNames)
}

func mapAdminAPIRingInstanceIDDefaults(_, target Parameters) error {
	return target.SetDefaultValue("admin_api.leader_election.ring.instance_id", Nil)
}

var gemRemovedConfigPath = []string{
	"graphite.querier_remote_read_enabled", // -graphite.querier.remote-read-enabled

	// changed memcached config and dropped support for redis and fifocache
	"graphite.querier.metric_name_cache.cache.default_validity",                                      // -graphite.querier.metric-name-cache.default-validity
	"graphite.querier.metric_name_cache.cache.enable_fifocache",                                      // -graphite.querier.metric-name-cache.cache.enable-fifocache
	"graphite.querier.metric_name_cache.cache.fifocache.max_size_bytes",                              // -graphite.querier.metric-name-cache.fifocache.max-size-bytes
	"graphite.querier.metric_name_cache.cache.fifocache.max_size_items",                              // -graphite.querier.metric-name-cache.fifocache.max-size-items
	"graphite.querier.metric_name_cache.cache.fifocache.size",                                        // -graphite.querier.metric-name-cache.fifocache.size
	"graphite.querier.metric_name_cache.cache.fifocache.validity",                                    // -graphite.querier.metric-name-cache.fifocache.duration
	"graphite.querier.metric_name_cache.cache.memcached.expiration",                                  // -graphite.querier.metric-name-cache.memcached.expiration
	"graphite.querier.metric_name_cache.cache.memcached_client.circuit_breaker_consecutive_failures", // -graphite.querier.metric-name-cache.memcached.circuit-breaker-consecutive-failures
	"graphite.querier.metric_name_cache.cache.memcached_client.circuit_breaker_interval",             // -graphite.querier.metric-name-cache.memcached.circuit-breaker-interval
	"graphite.querier.metric_name_cache.cache.memcached_client.circuit_breaker_timeout",              // -graphite.querier.metric-name-cache.memcached.circuit-breaker-timeout
	"graphite.querier.metric_name_cache.cache.memcached_client.consistent_hash",                      // -graphite.querier.metric-name-cache.memcached.consistent-hash
	"graphite.querier.metric_name_cache.cache.memcached_client.update_interval",                      // -graphite.querier.metric-name-cache.memcached.update-interval
	"graphite.querier.metric_name_cache.cache.redis.db",                                              // -graphite.querier.metric-name-cache.redis.db
	"graphite.querier.metric_name_cache.cache.redis.endpoint",                                        // -graphite.querier.metric-name-cache.redis.endpoint
	"graphite.querier.metric_name_cache.cache.redis.expiration",                                      // -graphite.querier.metric-name-cache.redis.expiration
	"graphite.querier.metric_name_cache.cache.redis.idle_timeout",                                    // -graphite.querier.metric-name-cache.redis.idle-timeout
	"graphite.querier.metric_name_cache.cache.redis.master_name",                                     // -graphite.querier.metric-name-cache.redis.master-name
	"graphite.querier.metric_name_cache.cache.redis.max_connection_age",                              // -graphite.querier.metric-name-cache.redis.max-connection-age
	"graphite.querier.metric_name_cache.cache.redis.password",                                        // -graphite.querier.metric-name-cache.redis.password
	"graphite.querier.metric_name_cache.cache.redis.pool_size",                                       // -graphite.querier.metric-name-cache.redis.pool-size
	"graphite.querier.metric_name_cache.cache.redis.timeout",                                         // -graphite.querier.metric-name-cache.redis.timeout
	"graphite.querier.metric_name_cache.cache.redis.tls_enabled",                                     // -graphite.querier.metric-name-cache.redis.tls-enabled
	"graphite.querier.metric_name_cache.cache.redis.tls_insecure_skip_verify",                        // -graphite.querier.metric-name-cache.redis.tls-insecure-skip-verify

	"graphite.querier.aggregation_cache.cache.default_validity",                                      // -graphite.querier.aggregation-cache.default-validity
	"graphite.querier.aggregation_cache.cache.enable_fifocache",                                      // -graphite.querier.aggregation-cache.cache.enable-fifocache
	"graphite.querier.aggregation_cache.cache.fifocache.max_size_bytes",                              // -graphite.querier.aggregation-cache.fifocache.max-size-bytes
	"graphite.querier.aggregation_cache.cache.fifocache.max_size_items",                              // -graphite.querier.aggregation-cache.fifocache.max-size-items
	"graphite.querier.aggregation_cache.cache.fifocache.size",                                        // -graphite.querier.aggregation-cache.fifocache.size
	"graphite.querier.aggregation_cache.cache.fifocache.validity",                                    // -graphite.querier.aggregation-cache.fifocache.duration
	"graphite.querier.aggregation_cache.cache.memcached.expiration",                                  // -graphite.querier.aggregation-cache.memcached.expiration
	"graphite.querier.aggregation_cache.cache.memcached_client.circuit_breaker_consecutive_failures", // -graphite.querier.aggregation-cache.memcached.circuit-breaker-consecutive-failures
	"graphite.querier.aggregation_cache.cache.memcached_client.circuit_breaker_interval",             // -graphite.querier.aggregation-cache.memcached.circuit-breaker-interval
	"graphite.querier.aggregation_cache.cache.memcached_client.circuit_breaker_timeout",              // -graphite.querier.aggregation-cache.memcached.circuit-breaker-timeout
	"graphite.querier.aggregation_cache.cache.memcached_client.consistent_hash",                      // -graphite.querier.aggregation-cache.memcached.consistent-hash
	"graphite.querier.aggregation_cache.cache.memcached_client.update_interval",                      // -graphite.querier.aggregation-cache.memcached.update-interval
	"graphite.querier.aggregation_cache.cache.redis.db",                                              // -graphite.querier.aggregation-cache.redis.db
	"graphite.querier.aggregation_cache.cache.redis.endpoint",                                        // -graphite.querier.aggregation-cache.redis.endpoint
	"graphite.querier.aggregation_cache.cache.redis.expiration",                                      // -graphite.querier.aggregation-cache.redis.expiration
	"graphite.querier.aggregation_cache.cache.redis.idle_timeout",                                    // -graphite.querier.aggregation-cache.redis.idle-timeout
	"graphite.querier.aggregation_cache.cache.redis.master_name",                                     // -graphite.querier.aggregation-cache.redis.master-name
	"graphite.querier.aggregation_cache.cache.redis.max_connection_age",                              // -graphite.querier.aggregation-cache.redis.max-connection-age
	"graphite.querier.aggregation_cache.cache.redis.password",                                        // -graphite.querier.aggregation-cache.redis.password
	"graphite.querier.aggregation_cache.cache.redis.pool_size",                                       // -graphite.querier.aggregation-cache.redis.pool-size
	"graphite.querier.aggregation_cache.cache.redis.timeout",                                         // -graphite.querier.aggregation-cache.redis.timeout
	"graphite.querier.aggregation_cache.cache.redis.tls_enabled",                                     // -graphite.querier.aggregation-cache.redis.tls-enabled
	"graphite.querier.aggregation_cache.cache.redis.tls_insecure_skip_verify",                        // -graphite.querier.aggregation-cache.redis.tls-insecure-skip-verify

	"compactor.compaction_strategy", // -compactor.compaction-strategy

	"querier.query_label_names_with_matchers_enabled", // -querier.query-label-names-with-matchers-enabled
}
