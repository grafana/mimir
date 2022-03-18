// SPDX-License-Identifier: AGPL-3.0-only

package config

// GEM170ToGEM200Mapper maps from gem-1.7.0 to gem-2.0.0 configurations
func GEM170ToGEM200Mapper() Mapper {
	nonExistentGEMPaths := map[string]struct{}{
		"blocks_storage.tsdb.max_exemplars":         {},
		"query_range.parallelise_shardable_queries": {},
	}
	gemRenames := make(map[string]Mapping, len(cortexRenameMappings))
	for path, mapping := range cortexRenameMappings {
		if _, notInGEM := nonExistentGEMPaths[path]; notInGEM {
			continue
		}
		gemRenames[path] = mapping
	}

	return MultiMapper{
		mapGEMInstanceInterfaceNames(),
		// first try to naively map keys from old config to same keys from new config
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
		MapperFunc(mapMemcachedAddresses),
		// Map `-*.s3.url` to `-*.s3.(endpoint|access_key_id|secret_access_key)`
		mapRulerAlertmanagerS3URL("alertmanager.storage", "alertmanager_storage"), mapRulerAlertmanagerS3URL("ruler.storage", "ruler_storage"),
		// Map `-*.s3.bucketnames` and (maybe part of `-*s3.s3.url`) to `-*.s3.bucket-name`
		mapRulerAlertmanagerS3Buckets("alertmanager.storage", "alertmanager_storage"), mapRulerAlertmanagerS3Buckets("ruler.storage", "ruler_storage"),
		// Prevent server.http_listen_port from being updated with a new default and always output it.
		MapperFunc(mapServerHTTPListenPort),
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
