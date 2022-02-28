// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	_ "embed" // need this for oldCortexConfig
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/tools/doc-generator/parse"
)

// CortexToMimirMapper maps from cortex-1.11.0 to mimir-2.0.0 configurations
var CortexToMimirMapper = MultiMapper{
	// first try to naively map keys from old config to same keys from new config
	BestEffortDirectMapper{},
	// next map
	MapperFunc(func(source, target *InspectedEntry) error {
		amDiscovery, err := source.GetValue("ruler.enable_alertmanager_discovery")
		if err != nil {
			return errors.Wrap(err, "could not convert ruler.enable_alertmanager_discovery")
		}
		if amDiscovery == nil || !amDiscovery.(bool) {
			return nil
		}

		amURL, err := source.GetValue("ruler.alertmanager_url")
		if err != nil {
			return errors.Wrap(err, "could not get ruler.alertmanager_url")
		}

		amURLs := strings.Split(amURL.(string), ",")
		for i := range amURLs {
			amURLs[i] = "dnssrvnoa+" + amURLs[i]
		}
		return target.SetValue("ruler.alertmanager_url", strings.Join(amURLs, ","))
	}),
	// last apply any special treatment to other parameters
	PathMapper{
		PathMappings: map[string]Mapping{
			"blocks_storage.tsdb.max_exemplars": RenameMapping("limits.max_global_exemplars_per_user"),

			"query_range.results_cache.cache.background.writeback_buffer":     RenameMapping("frontend.results_cache.memcached.max_async_buffer_size"),
			"query_range.results_cache.cache.background.writeback_goroutines": RenameMapping("frontend.results_cache.memcached.max_async_concurrency"),
			"query_range.results_cache.cache.memcached.batch_size":            RenameMapping("frontend.results_cache.memcached.max_get_multi_batch_size"),
			"query_range.results_cache.cache.memcached.parallelism":           RenameMapping("frontend.results_cache.memcached.max_get_multi_concurrency"),
			"query_range.results_cache.cache.memcached_client.max_idle_conns": RenameMapping("frontend.results_cache.memcached.max_idle_connections"),
			"query_range.results_cache.cache.memcached_client.max_item_size":  RenameMapping("frontend.results_cache.memcached.max_item_size"),
			"query_range.results_cache.cache.memcached_client.timeout":        RenameMapping("frontend.results_cache.memcached.timeout"),
			"query_range.results_cache.compression":                           RenameMapping("frontend.results_cache.compression"),

			"alertmanager.cluster.peer_timeout": RenameMapping("alertmanager.peer_timeout"),
			"alertmanager.enable_api":           RenameMapping("alertmanager.enable_api"), // added because CLI flag was renamed
			"ruler.enable_api":                  RenameMapping("ruler.enable_api"),        // added because CLI flag was renamed

			// Removed `-alertmanager.storage.*` configuration options, use `-alertmanager-storage.*` instead.
			"alertmanager.storage.azure.account_key":                      RenameMapping("alertmanager_storage.azure.account_key"),
			"alertmanager.storage.azure.account_name":                     RenameMapping("alertmanager_storage.azure.account_name"),
			"alertmanager.storage.azure.container_name":                   RenameMapping("alertmanager_storage.azure.container_name"),
			"alertmanager.storage.azure.max_retries":                      RenameMapping("alertmanager_storage.azure.max_retries"),
			"alertmanager.storage.gcs.bucket_name":                        RenameMapping("alertmanager_storage.gcs.bucket_name"),
			"alertmanager.storage.local.path":                             RenameMapping("alertmanager_storage.local.path"),
			"alertmanager.storage.s3.access_key_id":                       RenameMapping("alertmanager_storage.s3.access_key_id"),
			"alertmanager.storage.s3.bucketnames":                         RenameMapping("alertmanager_storage.s3.bucket_name"), // if it is comma-delimited, then it's invalid
			"alertmanager.storage.s3.endpoint":                            RenameMapping("alertmanager_storage.s3.endpoint"),    // if it is already set by the previous mapping, then err
			"alertmanager.storage.s3.http_config.idle_conn_timeout":       RenameMapping("alertmanager_storage.s3.http.idle_conn_timeout"),
			"alertmanager.storage.s3.http_config.insecure_skip_verify":    RenameMapping("alertmanager_storage.s3.http.insecure_skip_verify"),
			"alertmanager.storage.s3.http_config.response_header_timeout": RenameMapping("alertmanager_storage.s3.http.response_header_timeout"),
			"alertmanager.storage.s3.insecure":                            RenameMapping("alertmanager_storage.s3.insecure"),
			"alertmanager.storage.s3.region":                              RenameMapping("alertmanager_storage.s3.region"),
			"alertmanager.storage.s3.s3":                                  RenameMapping("alertmanager_storage.s3.endpoint"), // if it contains "inmemory://" this should be invalid, also how do we know if the URL contains "escaped Key and Secret encoded"?
			"alertmanager.storage.s3.secret_access_key":                   RenameMapping("alertmanager_storage.s3.secret_access_key"),
			"alertmanager.storage.s3.signature_version":                   RenameMapping("alertmanager_storage.s3.signature_version"),
			"alertmanager.storage.s3.sse.kms_encryption_context":          RenameMapping("alertmanager_storage.s3.sse.kms_encryption_context"),
			"alertmanager.storage.s3.sse.kms_key_id":                      RenameMapping("alertmanager_storage.s3.sse.kms_key_id"),
			"alertmanager.storage.s3.sse.type":                            RenameMapping("alertmanager_storage.s3.sse.type"), // should not override if is already set
			"alertmanager.storage.type":                                   RenameMapping("alertmanager_storage.backend"),

			// Removed the support for `-ruler.storage.*`, use `-ruler-storage.*` instead.
			"ruler.storage.azure.account_key":                      RenameMapping("ruler_storage.azure.account_key"),
			"ruler.storage.azure.account_name":                     RenameMapping("ruler_storage.azure.account_name"),
			"ruler.storage.azure.container_name":                   RenameMapping("ruler_storage.azure.container_name"),
			"ruler.storage.azure.max_retries":                      RenameMapping("ruler_storage.azure.max_retries"),
			"ruler.storage.gcs.bucket_name":                        RenameMapping("ruler_storage.gcs.bucket_name"),
			"ruler.storage.local.directory":                        RenameMapping("ruler_storage.local.path"),
			"ruler.storage.s3.access_key_id":                       RenameMapping("ruler_storage.s3.access_key_id"),
			"ruler.storage.s3.bucketnames":                         RenameMapping("ruler_storage.s3.bucket_name"), // if it is comma-delimited, then it's invalid
			"ruler.storage.s3.endpoint":                            RenameMapping("ruler_storage.s3.endpoint"),    // if it is already set by the previous mapping, then err
			"ruler.storage.s3.http_config.idle_conn_timeout":       RenameMapping("ruler_storage.s3.http.idle_conn_timeout"),
			"ruler.storage.s3.http_config.insecure_skip_verify":    RenameMapping("ruler_storage.s3.http.insecure_skip_verify"),
			"ruler.storage.s3.http_config.response_header_timeout": RenameMapping("ruler_storage.s3.http.response_header_timeout"),
			"ruler.storage.s3.insecure":                            RenameMapping("ruler_storage.s3.insecure"),
			"ruler.storage.s3.region":                              RenameMapping("ruler_storage.s3.region"),
			"ruler.storage.s3.s3":                                  RenameMapping("ruler_storage.s3.endpoint"), // if it contains "inmemory://" this should be invalid, also how do we know if the URL contains "escaped Key and Secret encoded"?
			"ruler.storage.s3.secret_access_key":                   RenameMapping("ruler_storage.s3.secret_access_key"),
			"ruler.storage.s3.signature_version":                   RenameMapping("ruler_storage.s3.signature_version"),
			"ruler.storage.s3.sse.kms_encryption_context":          RenameMapping("ruler_storage.s3.sse.kms_encryption_context"),
			"ruler.storage.s3.sse.kms_key_id":                      RenameMapping("ruler_storage.s3.sse.kms_key_id"),
			"ruler.storage.s3.sse.type":                            RenameMapping("ruler_storage.s3.sse.type"), // should not override if is already set
			"ruler.storage.swift.auth_url":                         RenameMapping("ruler_storage.swift.auth_url"),
			"ruler.storage.swift.auth_version":                     RenameMapping("ruler_storage.swift.auth_version"),
			"ruler.storage.swift.connect_timeout":                  RenameMapping("ruler_storage.swift.connect_timeout"),
			"ruler.storage.swift.container_name":                   RenameMapping("ruler_storage.swift.container_name"),
			"ruler.storage.swift.domain_id":                        RenameMapping("ruler_storage.swift.domain_id"),
			"ruler.storage.swift.domain_name":                      RenameMapping("ruler_storage.swift.domain_name"),
			"ruler.storage.swift.max_retries":                      RenameMapping("ruler_storage.swift.max_retries"),
			"ruler.storage.swift.password":                         RenameMapping("ruler_storage.swift.password"),
			"ruler.storage.swift.project_domain_id":                RenameMapping("ruler_storage.swift.project_domain_id"),
			"ruler.storage.swift.project_domain_name":              RenameMapping("ruler_storage.swift.project_domain_name"),
			"ruler.storage.swift.project_id":                       RenameMapping("ruler_storage.swift.project_id"),
			"ruler.storage.swift.project_name":                     RenameMapping("ruler_storage.swift.project_name"),
			"ruler.storage.swift.region_name":                      RenameMapping("ruler_storage.swift.region_name"),
			"ruler.storage.swift.request_timeout":                  RenameMapping("ruler_storage.swift.request_timeout"),
			"ruler.storage.swift.user_domain_id":                   RenameMapping("ruler_storage.swift.user_domain_id"),
			"ruler.storage.swift.user_domain_name":                 RenameMapping("ruler_storage.swift.user_domain_name"),
			"ruler.storage.swift.user_id":                          RenameMapping("ruler_storage.swift.user_id"),
			"ruler.storage.swift.username":                         RenameMapping("ruler_storage.swift.username"),
			"ruler.storage.type":                                   RenameMapping("ruler_storage.backend"),

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
			"frontend.instance_interface_names":                      RenameMapping("frontend.instance_interface_names"),
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
			"ingester.lifecycler.interface_names":                            RenameMapping("ingester.ring.instance_interface_names"),
			"ingester.lifecycler.join_after":                                 RenameMapping("ingester.ring.join_after"),
			"ingester.lifecycler.min_ready_duration":                         RenameMapping("ingester.ring.min_ready_duration"),
			"ingester.lifecycler.num_tokens":                                 RenameMapping("ingester.ring.num_tokens"),
			"ingester.lifecycler.observe_period":                             RenameMapping("ingester.ring.observe_period"),
			"ingester.lifecycler.readiness_check_ring_health":                RenameMapping("ingester.ring.readiness_check_ring_health"),
			"ingester.lifecycler.ring.excluded_zones":                        RenameMapping("ingester.ring.excluded_zones"),
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

			"auth_enabled": RenameMapping("multitenancy_enabled"),
		},
	},
}

type InspectedEntryFactory func() *InspectedEntry

// Convert converts the passed YAML contents and CLI flags in the source schema to a YAML config and CLI flags
// in the target schema. sourceFactory and targetFactory are assumed to return
// InspectedEntries where the FieldValue is the default value of the configuration parameter.
// Convert uses sourceFactory and targetFactory to also prune the default values from the resulting config.
// Convert returns the marshalled YAML config in the target schema.
func Convert(contents []byte, flags []string, m Mapper, sourceFactory, targetFactory InspectedEntryFactory) ([]byte, []string, error) {
	removedFieldPaths, removedFlags, err := reportDeletedFlags(contents, flags, sourceFactory)
	if err != nil {
		return nil, nil, err
	}

	source, target := sourceFactory(), targetFactory()

	err = yaml.Unmarshal(contents, &source)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not unmarshal old Cortex configuration file")
	}

	err = addFlags(source, flags)
	if err != nil {
		return nil, nil, err
	}

	err = m.DoMap(source, target)
	if err != nil {
		return nil, nil, err
	}

	sourceDefaults, targetDefaults, err := prepareDefaults(m, sourceFactory, targetFactory)
	if err != nil {
		return nil, nil, err
	}

	pruneDefaults(target, sourceDefaults, targetDefaults)

	var newFlags []string
	if len(flags) > 0 {
		newFlags, err = convertFlags(flags, m, target, sourceFactory, targetFactory)
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "could not convert passed CLI args: "+err.Error())
		}
	}

	yamlBytes, err := yaml.Marshal(target)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not marshal converted config to YAML")
	}

	for _, f := range removedFieldPaths {
		_, _ = fmt.Fprintln(os.Stderr, "field", f, "is no longer supported")
	}
	for _, f := range removedFlags {
		_, _ = fmt.Fprintf(os.Stderr, "flag -%s is no longer supported", f)
	}

	return yamlBytes, newFlags, err
}

func convertFlags(flags []string, m Mapper, target *InspectedEntry, sourceFactory, targetFactory InspectedEntryFactory) ([]string, error) {
	flagsNewPaths, err := mapOldFlagsToNewPaths(flags, m, sourceFactory, targetFactory)
	if err != nil {
		return nil, err
	}
	var newFlagsWithValues []string
	err = target.Walk(func(path string, value interface{}) error {
		if _, ok := flagsNewPaths[path]; !ok {
			return nil
		}
		flagName, err := target.GetFlag(path)
		if err != nil {
			return err
		}

		newFlagsWithValues = append(newFlagsWithValues, fmt.Sprintf("-%s=%v", flagName, value))
		return nil
	})
	if err != nil {
		return nil, err
	}

	// remove the parameters from the YAML, only keep the flags
	for f := range flagsNewPaths {
		err = target.Delete(f)
		if err != nil {
			return nil, err
		}
	}

	return newFlagsWithValues, nil
}

// addFlags parses the flags and add their values to the config
func addFlags(entry *InspectedEntry, flags []string) error {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	entry.RegisterFlags(fs, nil)
	return fs.Parse(flags)
}

// mapOldFlagsToNewPaths works by creating a new source and target parameters. It uses the values from the passed flags
// to populate the source config. Then it deletes all other parameters from the source but the ones set by the provided flags.
// It sets all values in the target config to nil. It uses the mapper to convert the source to the target.
// It assumes that any parameter that has a non-nil value in the target config
// must have been set by the flags. It returns the paths in the target of the remaining parameters.
//
// mapOldFlagsToNewPaths does not return values along with the flag names because it does not have the rest of the
// config values from the YAML file, so it's likely that the Mapper didn't have enough information to do a correct mapping.
// Renames and other trivial mappings should not be affected by this.
func mapOldFlagsToNewPaths(flags []string, m Mapper, sourceFactory, targetFactory InspectedEntryFactory) (map[string]struct{}, error) {
	source, target := sourceFactory(), targetFactory()

	err := addFlags(source, flags)
	if err != nil {
		return nil, err
	}
	flagIsSet := parseFlagNames(flags)

	var parametersWithoutProvidedFlags []string
	err = source.Walk(func(path string, value interface{}) error {
		flagName, err := source.GetFlag(path)
		if err != nil {
			if !errors.Is(err, ErrParameterNotFound) {
				return err
			}
			return nil
		}

		if !flagIsSet[flagName] {
			parametersWithoutProvidedFlags = append(parametersWithoutProvidedFlags, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// delete any parameters from the source that are not set by the flags
	for _, path := range parametersWithoutProvidedFlags {
		err = source.Delete(path)
		if err != nil {
			return nil, err
		}
	}

	var allTargetParams []string
	err = target.Walk(func(path string, value interface{}) error {
		allTargetParams = append(allTargetParams, path)
		return nil
	})
	if err != nil {
		panic("walk returned an error, even though the walking function didn't return any, not sure what to do: " + err.Error())
	}

	for _, path := range allTargetParams {
		err = target.SetValue(path, nil)
		if err != nil {
			return nil, err
		}
	}

	// swallow the error, because we deleted some parameters from the source, not all mappings will work
	_ = m.DoMap(source, target)

	remainingFlags := map[string]struct{}{}
	err = target.Walk(func(path string, value interface{}) error {
		if value != nil {
			remainingFlags[path] = struct{}{}
		}
		return nil
	})
	if err != nil {
		panic("walk returned an error, even though the walking function didn't return any, not sure what to do: " + err.Error())
	}
	return remainingFlags, nil
}

// returns map where keys are flag names found in flags, and value is "true".
func parseFlagNames(flags []string) map[string]bool {
	names := map[string]bool{}
	for _, f := range flags {
		name := f
		name = strings.TrimPrefix(name, "-")
		name = strings.TrimPrefix(name, "-") // trim the prefix twice in case the flag was passed as --flag instead of -flag
		name = strings.SplitN(name, "=", 2)[0]
		name = strings.SplitN(name, " ", 2)[0]
		names[name] = true
	}

	return names
}

// prepareDefaults maps source defaults to target defaults the same way regular source config is mapped to target config.
// This enables lookups of cortex default values using their mimir paths.
func prepareDefaults(m Mapper, sourceFactory, targetFactory InspectedEntryFactory) (cortexDefaults, mimirDefaults *InspectedEntry, err error) {
	oldCortexDefaults, mappedCortexDefaults := sourceFactory(), targetFactory()

	err = m.DoMap(oldCortexDefaults, mappedCortexDefaults)
	return mappedCortexDefaults, DefaultMimirConfig(), err
}

// TODO dimitarvdimitrov convert this to a Mapper, ideally splitting default pruning from "default changed" warnings
// pruneDefaults removes the defaults from fullParams and prints to os.Stderr any changed defaults
// which haven't been overwritten. pruneDefaults swallows prints any errors during pruning to os.Stderr
func pruneDefaults(fullParams, oldDefaults, newDefaults *InspectedEntry) {
	var pathsToDelete []string

	err := fullParams.Walk(func(path string, value interface{}) error {
		newDefault, err := newDefaults.GetValue(path)
		if err != nil {
			return errors.Wrap(err, "expecting value "+path+" to have a default")
		}

		oldDefault, err := oldDefaults.GetValue(path)
		if err != nil {
			// We don't expect new fields exist in the old struct.
			if errors.Is(err, ErrParameterNotFound) {
				return err
			}
			oldDefault = nil
		}

		// Use reflect.DeepEqual to easily compare different type aliases that resolve to the same underlying type,
		// fields with interface types, and maps and slices.
		if value == nil || reflect.DeepEqual(value, oldDefault) || reflect.DeepEqual(value, newDefault) {
			if !reflect.DeepEqual(oldDefault, newDefault) {
				_, _ = fmt.Fprintf(os.Stderr, "using a new default for %s: %v (used to be %v)\n", path, newDefault, oldDefault)
			}

			pathsToDelete = append(pathsToDelete, path)
		}
		return nil
	})
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	for _, p := range pathsToDelete {
		err = fullParams.Delete(p)
		if err != nil {
			err = errors.Wrap(err, "cloud not delete parameter with default value from config")
			_, _ = fmt.Fprintln(os.Stderr, err)
		}
	}
}

func reportDeletedFlags(contents []byte, flags []string, sourceFactory InspectedEntryFactory) (removedFieldPaths, removedFlags []string, _ error) {
	cortexConfigWithNoValues := func() (*InspectedEntry, error) {
		s := sourceFactory()

		return s, s.Walk(func(path string, value interface{}) error {
			return s.SetValue(path, nil)
		})
	}

	// Find YAML options that user is using, but are no longer supported.
	{
		s, err := cortexConfigWithNoValues()
		if err != nil {
			return nil, nil, err
		}

		if err := yaml.Unmarshal(contents, &s); err != nil {
			return nil, nil, errors.Wrap(err, "could not unmarshal Cortex configuration file")
		}

		for _, path := range removedConfigPaths {
			val, _ := s.GetValue(path)
			if val != nil {
				removedFieldPaths = append(removedFieldPaths, path)
			}
		}
	}

	// Find CLI flags that user is using, but are no longer supported.
	{
		s, err := cortexConfigWithNoValues()
		if err != nil {
			return nil, nil, err
		}

		if err := addFlags(s, flags); err != nil {
			return nil, nil, err
		}

		for _, path := range removedConfigPaths {
			val, _ := s.GetValue(path)
			if val != nil {
				fl, _ := s.GetFlag(path)
				if fl != "" {
					removedFlags = append(removedFlags, fl)
				}
			}
		}
	}

	// Report any provided CLI flags that cannot be found in YAML, and that are not supported anymore.
	providedFlags := parseFlagNames(flags)
	for _, f := range removedCLIOptions {
		if providedFlags[f] {
			removedFlags = append(removedFlags, f)
		}
	}

	return
}

func DefaultMimirConfig() *InspectedEntry {
	cfg, err := DefaultValueInspector.InspectConfig(&mimir.Config{})
	if err != nil {
		panic(err)
	}
	return cfg
}

//go:embed descriptors/cortex-v1.11.0.json
var oldCortexConfig []byte

//go:embed descriptors/cortex-v1.11.0-flags-only.json
var oldCortexConfigFlagsOnly []byte

const notInYaml = "not-in-yaml"

func DefaultCortexConfig() *InspectedEntry {
	cfg := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfig, cfg); err != nil {
		panic(err)
	}

	cfgFlagsOnly := &InspectedEntry{}
	if err := json.Unmarshal(oldCortexConfigFlagsOnly, cfgFlagsOnly); err != nil {
		panic(err)
	}

	cfg.BlockEntries = append(cfg.BlockEntries, &InspectedEntry{
		Kind:         parse.KindBlock,
		Name:         notInYaml,
		Required:     false,
		Desc:         "Flags not available in YAML file.",
		BlockEntries: cfgFlagsOnly.BlockEntries,
	})
	return cfg
}
