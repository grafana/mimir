// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type conversionInput struct {
	useNewDefaults bool
	outputDefaults bool
	inYAML         []byte
	inFlags        []string
}

func testCortexAndGEM(t *testing.T, tc conversionInput, assert func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error)) {
	t.Parallel()
	t.Run("cortex->mimir", func(t *testing.T) {
		t.Parallel()
		mimirYAML, mimirFlags, mimirNotices, mimirErr := Convert(tc.inYAML, tc.inFlags, CortexToMimirMapper(), DefaultCortexConfig, DefaultMimirConfig, tc.useNewDefaults, tc.outputDefaults)
		assert(t, mimirYAML, mimirFlags, mimirNotices, mimirErr)
	})

	t.Run("gem170->gem200", func(t *testing.T) {
		t.Parallel()
		gemYAML, gemFlags, gemNotices, gemErr := Convert(tc.inYAML, tc.inFlags, GEM170ToGEM200Mapper(), DefaultGEM170Config, DefaultGEM200COnfig, tc.useNewDefaults, tc.outputDefaults)
		assert(t, gemYAML, gemFlags, gemNotices, gemErr)
	})
}

func TestConvert_Cortex(t *testing.T) {
	testCases := []struct {
		name                      string
		useNewDefaults            bool
		inFile, outFile           string
		inFlagsFile, outFlagsFile string
	}{
		{
			name:    "shouldn't need any conversion",
			inFile:  "testdata/noop-old.yaml",
			outFile: "testdata/noop-new.yaml",
		},
		{
			name:         "shouldn't need any conversion with flags",
			inFlagsFile:  "testdata/noop-flags-old.flags.txt",
			outFlagsFile: "testdata/noop-flags-new.flags.txt",
		},
		{
			name:    "simple rename",
			inFile:  "testdata/rename-old.yaml",
			outFile: "testdata/rename-new.yaml",
		},
		{
			name:    "alertmanager URL has dnssrvnoa+ prepended if alertmanager discovery ",
			inFile:  "testdata/alertmanager-url-old.yaml",
			outFile: "testdata/alertmanager-url-new.yaml",
		},
		{
			name:    "query_range cache params are renamed",
			inFile:  "testdata/query-range-old.yaml",
			outFile: "testdata/query-range-new.yaml",
		},
		{
			name:         "with non-primitive flags",
			inFlagsFile:  "testdata/value-flags-old.flags.txt",
			outFlagsFile: "testdata/value-flags-new.flags.txt",
		},
		{
			name:         "with renamed flags",
			inFlagsFile:  "testdata/renamed-flags-old.flags.txt",
			outFlagsFile: "testdata/renamed-flags-new.flags.txt",
		},
		{
			name:         "config flags have precedence",
			inFile:       "testdata/noop-old.yaml",
			inFlagsFile:  "testdata/flags-precedence-old.flags.txt",
			outFlagsFile: "testdata/flags-precedence-new.flags.txt",
			outFile:      "testdata/common-options.yaml",
		},
		{
			name:    "ruler.storage maps to ruler_storage",
			inFile:  "testdata/ruler.storage-old.yaml",
			outFile: "testdata/ruler_storage-new.yaml",
		},
		{
			name:    "ruler_storage maps to ruler_storage",
			inFile:  "testdata/ruler_storage-old.yaml",
			outFile: "testdata/ruler_storage-new.yaml",
		},
		{
			name:    "ruler_storage has precedence over ruler.storage",
			inFile:  "testdata/ruler_storage-precedence-old.yaml",
			outFile: "testdata/ruler_storage-new.yaml",
		},
		{
			name:    "alertmanager.storage has precedence over alertmanager_storage",
			inFile:  "testdata/am-storage-precedence-old.yaml",
			outFile: "testdata/am-storage-precedence-new.yaml",
		},
		{
			name:    "ruler S3 SSE conversion work",
			inFile:  "testdata/ruler_storage-s3-sse-old.yaml",
			outFile: "testdata/ruler_storage-s3-sse-new.yaml",
		},
		{
			name:    "alertmanager S3 SSE conversion work",
			inFile:  "testdata/am_storage-s3-sse-old.yaml",
			outFile: "testdata/am_storage-s3-sse-new.yaml",
		},
		{
			name:    "S3 SSE conversion doesn't overwrite existing values",
			inFile:  "testdata/am_storage-s3-sse-repeated-old.yaml",
			outFile: "testdata/am_storage-s3-sse-repeated-new.yaml",
		},
		{
			name:    "new memcached addresses is constructed from old hostname and service",
			inFile:  "testdata/frontend.memcached.addresses-old.yaml",
			outFile: "testdata/frontend.memcached.addresses-new.yaml",
		},
		{
			name:    "old memcached addresses take precedence over hostname and service",
			inFile:  "testdata/frontend.memcached.addresses-existing-old.yaml",
			outFile: "testdata/frontend.memcached.addresses-existing-new.yaml",
		},
		{
			name:         "not-in-yaml flags don't show in output YAML",
			inFlagsFile:  "testdata/not-in-yaml-old.flags.txt",
			outFlagsFile: "testdata/not-in-yaml-new.flags.txt",
		},
		{
			name:    "ingester ring config",
			inFile:  "testdata/ingester-ring-old.yaml",
			outFile: "testdata/ingester-ring-new.yaml",
		},
		{
			name:    "sharding with consul enabled",
			inFile:  "testdata/sharding-consul-old.yaml",
			outFile: "testdata/sharding-consul-new.yaml",
		},
		{
			name:    "sharding disabled",
			inFile:  "testdata/sharding-disabled-old.yaml",
			outFile: "testdata/sharding-disabled-new.yaml",
		},
		{
			name:    "ruler S3 URL",
			inFile:  "testdata/ruler-s3-url-old.yaml",
			outFile: "testdata/ruler-s3-url-new.yaml",
		},
		{
			name:    "ruler S3 URL: existing access_key and secret_key take precedence",
			inFile:  "testdata/ruler-s3-url-with-secret-key-old.yaml",
			outFile: "testdata/ruler-s3-url-with-secret-key-new.yaml",
		},
		{
			name:    "ruler S3 URL: non-trivial user part, pt.1",
			inFile:  "testdata/ruler-s3-non-trivial-1-url-old.yaml",
			outFile: "testdata/ruler-s3-non-trivial-1-url-new.yaml",
		},
		{
			name:    "ruler S3 URL: non-trivial user part, pt.2",
			inFile:  "testdata/ruler-s3-non-trivial-2-url-old.yaml",
			outFile: "testdata/ruler-s3-non-trivial-2-url-new.yaml",
		},
		{
			name:    "ruler S3 URL: non-trivial user part, pt.3",
			inFile:  "testdata/ruler-s3-non-trivial-3-url-old.yaml",
			outFile: "testdata/ruler-s3-non-trivial-3-url-new.yaml",
		},
		{
			name:    "alertmanager S3 URL",
			inFile:  "testdata/am-s3-url-old.yaml",
			outFile: "testdata/am-s3-url-new.yaml",
		},
		{
			name:    "alertmanager S3 URL: existing endpoint isn't overwritten",
			inFile:  "testdata/am-s3-url-with-endpoint-old.yaml",
			outFile: "testdata/am-s3-url-with-endpoint-new.yaml",
		},
		{
			name:         "CSV string slice with single value",
			inFlagsFile:  "testdata/string-slice-single-old.flags.txt",
			outFlagsFile: "testdata/string-slice-single-new.flags.txt",
		},
		{
			name:         "CSV string slice",
			inFlagsFile:  "testdata/string-slice-old.flags.txt",
			outFlagsFile: "testdata/string-slice-new.flags.txt",
		},
		{
			name:           "instance_interface_names using explicit old default and useNewDefaults=true gets pruned",
			useNewDefaults: true,
			inFile:         "testdata/instance-interface-names-explicit-old.yaml",
			// The old config was using the previous default.
			// The new default will be dynamically detected when mimir boots, so no need to set it in the out YAML.
			outFile: "testdata/empty.yaml",
		},
		{
			name:    "instance_interface_names using explicit old default and useNewDefaults=false stays",
			inFile:  "testdata/instance-interface-names-explicit-old.yaml",
			outFile: "testdata/instance-interface-names-explicit-new.yaml",
		},
		{
			name:        "server.http-listen-port old default is printed even when implicitly using the old default",
			inFlagsFile: "testdata/empty.txt", // prevent the test from using common-flags.txt
			inFile:      "testdata/empty.yaml",
			outFile:     "testdata/server-listen-http-port-new.yaml",
		},
		{
			name:           "server.http-listen-port old default is retained with useNewDefaults=true",
			inFlagsFile:    "testdata/empty.txt", // prevent the test from using common-flags.txt
			useNewDefaults: true,
			inFile:         "testdata/server-listen-http-port-old.yaml",
			outFile:        "testdata/server-listen-http-port-new.yaml",
		},
		{
			name:        "server.http-listen-port old default is retained with useNewDefaults=false",
			inFlagsFile: "testdata/empty.txt", // prevent the test from using common-flags.txt
			inFile:      "testdata/server-listen-http-port-old.yaml",
			outFile:     "testdata/server-listen-http-port-new.yaml",
		},
		{
			name:        "server.http-listen-port random value is retained with useNewDefaults=false",
			inFlagsFile: "testdata/empty.txt", // prevent the test from using common-flags.txt
			inFile:      "testdata/server-listen-http-port-random-old.yaml",
			outFile:     "testdata/server-listen-http-port-random-new.yaml",
		},
		{
			name:         "flags with quotes and JSON don't get interpreted escaped",
			inFlagsFile:  "testdata/uncommon-flag-values.txt",
			outFlagsFile: "testdata/uncommon-flag-values.txt",
		},
		{
			name:         "duration list flags",
			inFlagsFile:  "testdata/duration-slice-old.flags.txt",
			outFlagsFile: "testdata/duration-slice-new.flags.txt",
		},
		{
			name:    "duration list YAML",
			inFile:  "testdata/duration-list-old.yaml",
			outFile: "testdata/duration-list-new.yaml",
		},
		{
			name:    "new frontend.results_cache.backend == memcached when old query_range.cache_results == true",
			inFile:  "testdata/query-frontend-results-cache-old.yaml",
			outFile: "testdata/query-frontend-results-cache-new.yaml",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			inBytes, expectedOut := loadFile(t, tc.inFile), loadFile(t, tc.outFile)
			inFlags, expectedOutFlags := loadFlags(t, tc.inFlagsFile), loadFlags(t, tc.outFlagsFile)
			if inFlags == nil {
				inFlags = loadFlags(t, "testdata/common-flags.txt")
				expectedOutFlags = inFlags
			}
			if inBytes == nil {
				inBytes = loadFile(t, "testdata/common-options.yaml")
				expectedOut = inBytes
			}

			in := conversionInput{
				useNewDefaults: tc.useNewDefaults,
				outputDefaults: false,
				inYAML:         inBytes,
				inFlags:        inFlags,
			}

			testCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error) {
				assert.NoError(t, err)

				assert.ElementsMatch(t, expectedOutFlags, outFlags)
				if expectedOut == nil {
					expectedOut = []byte("{}")
				}
				assert.YAMLEq(t, string(expectedOut), string(outYAML))
			})
		})
	}
}

func TestConvert_GEM(t *testing.T) {
	testCases := []struct {
		name                      string
		useNewDefaults            bool
		inFile, outFile           string
		inFlagsFile, outFlagsFile string
	}{
		{
			name:    "proxy_targets get translated",
			inFile:  "testdata/proxy-targets.yaml",
			outFile: "testdata/proxy-targets.yaml",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inBytes, expectedOut := loadFile(t, tc.inFile), loadFile(t, tc.outFile)
			inFlags, expectedOutFlags := loadFlags(t, tc.inFlagsFile), loadFlags(t, tc.outFlagsFile)
			if inFlags == nil {
				inFlags = loadFlags(t, "testdata/common-flags.txt")
				expectedOutFlags = inFlags
			}
			if inBytes == nil {
				inBytes = loadFile(t, "testdata/common-options.yaml")
				expectedOut = inBytes
			}

			outYAML, outFlags, _, err := Convert(inBytes, inFlags, GEM170ToGEM200Mapper(), DefaultGEM170Config, DefaultGEM200COnfig, tc.useNewDefaults, false)
			assert.NoError(t, err)

			assert.ElementsMatch(t, expectedOutFlags, outFlags)
			if expectedOut == nil {
				expectedOut = []byte("{}")
			}
			assert.YAMLEq(t, string(expectedOut), string(outYAML))

		})
	}
}

func TestConvert_InvalidConfigs(t *testing.T) {
	testCases := []struct {
		name        string
		inFile      string
		inFlagsFile string

		expectedErr string
	}{
		{
			name:        "alertmanager S3 URL cannot contain inmemory",
			inFile:      "testdata/am-s3-invalid-url-old.yaml",
			expectedErr: "could not map old config to new config: alertmanager.storage.s3.s3: inmemory s3 storage is no longer supported, please provide a real s3 endpoint",
		},
		{
			name:        "alertmanager S3 bucketnames contains multiple buckets",
			inFile:      "testdata/am-s3-multiple-buckets-old.yaml",
			expectedErr: "could not map old config to new config: alertmanager.storage.s3.bucketnames: multiple bucket names cannot be converted, please provide only a single bucket name",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			inBytes := loadFile(t, tc.inFile)
			inFlags := loadFlags(t, tc.inFlagsFile)

			in := conversionInput{
				inFlags: inFlags,
				inYAML:  inBytes,
			}
			testCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error) {
				assert.EqualError(t, err, tc.expectedErr)
			})
		})
	}
}

func TestReportDeletedFlags(t *testing.T) {
	testCases := map[string]struct {
		cortexConfigFile string
		flags            []string

		expectedRemovedPaths []string
		expectedRemovedFlags []string
	}{
		"no unsupported options": {
			cortexConfigFile: "testdata/noop-old.yaml",
			flags:            []string{"-target=10", "-auth.enabled"},

			expectedRemovedPaths: nil,
			expectedRemovedFlags: nil,
		},
		"unsupported config option": {
			cortexConfigFile: "testdata/unsupported-config.yaml",
			flags:            []string{"-target=10", "-auth.enabled"},

			expectedRemovedPaths: []string{"storage.engine"},
			expectedRemovedFlags: nil,
		},
		"unsupported CLI flag": {
			cortexConfigFile: "testdata/noop-old.yaml",
			flags:            []string{"-store.engine=chunks"},

			expectedRemovedFlags: []string{"store.engine"},
			expectedRemovedPaths: nil,
		},
		"unsupported config options and flags": {
			cortexConfigFile: "testdata/unsupported-config.yaml",
			flags:            []string{"-store.engine=chunks"},

			expectedRemovedPaths: []string{"storage.engine"},
			expectedRemovedFlags: []string{"store.engine"},
		},
		"flags without YAML equivalents": {
			cortexConfigFile: "testdata/noop-old.yaml",
			flags:            []string{"-schema-config-file=test.yaml"},

			expectedRemovedPaths: nil,
			expectedRemovedFlags: []string{"schema-config-file"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			inBytes := loadFile(t, tc.cortexConfigFile)

			removedFieldPaths, removedFlags, err := reportDeletedFlags(inBytes, tc.flags, DefaultCortexConfig)
			require.NoError(t, err)

			assert.ElementsMatch(t, tc.expectedRemovedPaths, removedFieldPaths, "YAML paths")
			assert.ElementsMatch(t, tc.expectedRemovedFlags, removedFlags, "CLI flags")
		})
	}
}

var changedCortexDefaults = []ChangedDefault{
	{Path: "activity_tracker.filepath", OldDefault: "./active-query-tracker", NewDefault: "./metrics-activity.log"},
	{Path: "alertmanager.data_dir", OldDefault: "data/", NewDefault: "./data-alertmanager/"},
	{Path: "alertmanager.enable_api", OldDefault: "false", NewDefault: "true"},
	{Path: "alertmanager.external_url", OldDefault: "", NewDefault: "http://localhost:8080/alertmanager"},
	{Path: "alertmanager.sharding_ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "alertmanager.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "alertmanager_storage.backend", OldDefault: "s3", NewDefault: "filesystem"},
	{Path: "alertmanager_storage.filesystem.dir", OldDefault: "", NewDefault: "alertmanager"},
	{Path: "blocks_storage.backend", OldDefault: "s3", NewDefault: "filesystem"},
	{Path: "blocks_storage.bucket_store.bucket_index.enabled", OldDefault: "false", NewDefault: "true"},
	{Path: "blocks_storage.bucket_store.chunks_cache.memcached.max_async_buffer_size", OldDefault: "10000", NewDefault: "25000"},
	{Path: "blocks_storage.bucket_store.chunks_cache.memcached.max_get_multi_batch_size", OldDefault: "0", NewDefault: "100"},
	{Path: "blocks_storage.bucket_store.chunks_cache.memcached.max_idle_connections", OldDefault: "16", NewDefault: "100"},
	{Path: "blocks_storage.bucket_store.chunks_cache.memcached.timeout", OldDefault: "100ms", NewDefault: "200ms"},
	{Path: "blocks_storage.bucket_store.ignore_deletion_mark_delay", OldDefault: "6h0m0s", NewDefault: "1h0m0s"},
	{Path: "blocks_storage.bucket_store.index_cache.memcached.max_async_buffer_size", OldDefault: "10000", NewDefault: "25000"},
	{Path: "blocks_storage.bucket_store.index_cache.memcached.max_get_multi_batch_size", OldDefault: "0", NewDefault: "100"},
	{Path: "blocks_storage.bucket_store.index_cache.memcached.max_idle_connections", OldDefault: "16", NewDefault: "100"},
	{Path: "blocks_storage.bucket_store.index_cache.memcached.timeout", OldDefault: "100ms", NewDefault: "200ms"},
	{Path: "blocks_storage.bucket_store.index_header_lazy_loading_enabled", OldDefault: "false", NewDefault: "true"},
	{Path: "blocks_storage.bucket_store.index_header_lazy_loading_idle_timeout", OldDefault: "20m0s", NewDefault: "1h0m0s"},
	{Path: "blocks_storage.bucket_store.metadata_cache.memcached.max_async_buffer_size", OldDefault: "10000", NewDefault: "25000"},
	{Path: "blocks_storage.bucket_store.metadata_cache.memcached.max_get_multi_batch_size", OldDefault: "0", NewDefault: "100"},
	{Path: "blocks_storage.bucket_store.metadata_cache.memcached.max_idle_connections", OldDefault: "16", NewDefault: "100"},
	{Path: "blocks_storage.bucket_store.metadata_cache.memcached.timeout", OldDefault: "100ms", NewDefault: "200ms"},
	{Path: "blocks_storage.bucket_store.sync_dir", OldDefault: "tsdb-sync", NewDefault: "./tsdb-sync/"},
	{Path: "blocks_storage.filesystem.dir", OldDefault: "", NewDefault: "blocks"},
	{Path: "blocks_storage.tsdb.close_idle_tsdb_timeout", OldDefault: "0s", NewDefault: "13h0m0s"},
	{Path: "blocks_storage.tsdb.dir", OldDefault: "tsdb", NewDefault: "./tsdb/"},
	{Path: "blocks_storage.tsdb.retention_period", OldDefault: "6h0m0s", NewDefault: "24h0m0s"},
	{Path: "compactor.block_sync_concurrency", OldDefault: "20", NewDefault: "8"},
	{Path: "compactor.data_dir", OldDefault: "./data", NewDefault: "./data-compactor/"},
	{Path: "compactor.sharding_ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "compactor.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "compactor.sharding_ring.wait_stability_min_duration", OldDefault: "1m0s", NewDefault: "0s"},
	{Path: "distributor.instance_limits.max_inflight_push_requests", OldDefault: "0", NewDefault: "2000"},
	{Path: "distributor.remote_timeout", OldDefault: "2s", NewDefault: "20s"},
	{Path: "distributor.ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "distributor.ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "frontend.grpc_client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "frontend.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "frontend.query_stats_enabled", OldDefault: "false", NewDefault: "true"},
	// frontend.results_cache.memcached.addresses can be included or not.
	// The perceived default for cortex was "dnssrvnoa+_memcached._tcp." because
	// cortex used .hostname and .service to do DNS service discovery.
	// The old default is kind of a result of the default values of two other fields (.hostname and .service)
	{Path: "frontend.results_cache.memcached.addresses", OldDefault: "dnssrvnoa+_memcached._tcp.", NewDefault: ""},
	{Path: "frontend.results_cache.memcached.max_async_buffer_size", OldDefault: "10000", NewDefault: "25000"},
	{Path: "frontend.results_cache.memcached.max_async_concurrency", OldDefault: "10", NewDefault: "50"},
	{Path: "frontend.results_cache.memcached.max_get_multi_batch_size", OldDefault: "1024", NewDefault: "100"},
	{Path: "frontend.results_cache.memcached.max_idle_connections", OldDefault: "16", NewDefault: "100"},
	{Path: "frontend.results_cache.memcached.max_item_size", OldDefault: "0", NewDefault: "1048576"},
	{Path: "frontend.results_cache.memcached.timeout", OldDefault: "100ms", NewDefault: "200ms"},
	{Path: "frontend.split_queries_by_interval", OldDefault: "0s", NewDefault: "24h0m0s"},
	{Path: "frontend_worker.grpc_client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "ingester.instance_limits.max_inflight_push_requests", OldDefault: "0", NewDefault: "30000"},
	{Path: "ingester.ring.final_sleep", OldDefault: "30s", NewDefault: "0s"},
	{Path: "ingester.ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "ingester.ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "ingester.ring.min_ready_duration", OldDefault: "1m0s", NewDefault: "15s"},
	{Path: "ingester_client.grpc_client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "limits.ingestion_burst_size", OldDefault: "50000", NewDefault: "200000"},
	{Path: "limits.ingestion_rate", OldDefault: "25000", NewDefault: "10000"},
	{Path: "limits.max_global_series_per_metric", OldDefault: "0", NewDefault: "20000"},
	{Path: "limits.max_global_series_per_user", OldDefault: "0", NewDefault: "150000"},
	{Path: "limits.ruler_max_rule_groups_per_tenant", OldDefault: "0", NewDefault: "70"},
	{Path: "limits.ruler_max_rules_per_rule_group", OldDefault: "0", NewDefault: "20"},
	{Path: "querier.query_ingesters_within", OldDefault: "0s", NewDefault: "13h0m0s"},
	{Path: "query_scheduler.grpc_client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "ruler.enable_api", OldDefault: "false", NewDefault: "true"},
	{Path: "ruler.ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "ruler.ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "ruler.rule_path", OldDefault: "/rules", NewDefault: "./data-ruler/"},
	{Path: "ruler.ruler_client.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "ruler_storage.backend", OldDefault: "s3", NewDefault: "filesystem"},
	{Path: "ruler_storage.filesystem.dir", OldDefault: "", NewDefault: "ruler"},
	{Path: "store_gateway.sharding_ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "store_gateway.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "store_gateway.sharding_ring.wait_stability_min_duration", OldDefault: "1m0s", NewDefault: "0s"},
}

func TestChangedCortexDefaults(t *testing.T) {
	// Create cortex config where all params have explicitly set default values
	params := DefaultCortexConfig()
	err := params.Walk(func(path string, _ Value) error {
		return params.SetValue(path, params.MustGetDefaultValue(path))
	})
	require.NoError(t, err)
	config, err := yaml.Marshal(params)
	require.NoError(t, err)

	// Create cortex config where all params have explicitly set default values so that all of them can be changed and reported as changed
	_, _, notices, err := Convert(config, nil, CortexToMimirMapper(), DefaultCortexConfig, DefaultMimirConfig, true, false)
	require.NoError(t, err)
	assert.ElementsMatch(t, changedCortexDefaults, notices.ChangedDefaults)
}

func TestChangedGEMDefaults(t *testing.T) {
	changedGEMSpecificDefaults := []ChangedDefault{
		{Path: "admin_api.leader_election.client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
		{Path: "admin_api.leader_election.enabled", OldDefault: "false", NewDefault: "true"},
		{Path: "admin_api.leader_election.ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
		{Path: "auth.type", OldDefault: "trust", NewDefault: "enterprise"},
		{Path: "graphite.enabled", OldDefault: "false", NewDefault: "true"},
		{Path: "graphite.querier.schemas.backend", OldDefault: "s3", NewDefault: "filesystem"},
		{Path: "instrumentation.enabled", OldDefault: "false", NewDefault: "true"},
		{Path: "limits.compactor_split_groups", OldDefault: "4", NewDefault: "1"},
		{Path: "limits.compactor_tenant_shard_size", OldDefault: "1", NewDefault: "0"},
	}

	// These slipped through from Mimir into GEM 1.7.0
	changedDefaultsOnlyInCortex := map[string]struct{}{
		"frontend.query_stats_enabled":                        {},
		"ingester.instance_limits.max_inflight_push_requests": {},
		"ingester.ring.min_ready_duration":                    {},
	}

	expectedChangedDefaults := changedGEMSpecificDefaults
	for _, def := range changedCortexDefaults {
		if _, notInGEM := changedDefaultsOnlyInCortex[def.Path]; notInGEM {
			continue
		}
		expectedChangedDefaults = append(expectedChangedDefaults, def)
	}

	// Create cortex config where all params have explicitly set default values so that all of them can be changed and reported as changed
	params := DefaultGEM170Config()
	err := params.Walk(func(path string, _ Value) error {
		return params.SetValue(path, params.MustGetDefaultValue(path))
	})
	require.NoError(t, err)
	config, err := yaml.Marshal(params)
	require.NoError(t, err)

	// Convert while also converting explicitly set defaults to new defaults
	_, _, notices, err := Convert(config, nil, GEM170ToGEM200Mapper(), DefaultGEM170Config, DefaultGEM200COnfig, true, false)
	require.NoError(t, err)
	assert.ElementsMatch(t, expectedChangedDefaults, notices.ChangedDefaults)
}

func TestConvert_UseNewDefaults(t *testing.T) {
	distributorTimeoutNotice := ChangedDefault{
		Path:       "distributor.remote_timeout",
		OldDefault: "2s",
		NewDefault: "20s",
	}

	testCases := []struct {
		name                 string
		useNewDefaults       bool
		inYAML, expectedYAML []byte

		expectedNotice       ChangedDefault
		valueShouldBeChanged bool
	}{
		{
			name:                 "replaces explicitly set old defaults when useNewDefaults=true",
			useNewDefaults:       true,
			inYAML:               []byte("distributor: { remote_timeout: 2s }"),
			expectedYAML:         []byte("distributor: { remote_timeout: 20s }"),
			expectedNotice:       distributorTimeoutNotice,
			valueShouldBeChanged: true,
		},
		{
			name:                 "keeps explicitly set old defaults useNewDefaults=false",
			useNewDefaults:       false,
			inYAML:               []byte("distributor: { remote_timeout: 2s }"),
			expectedYAML:         []byte("distributor: { remote_timeout: 2s }"),
			expectedNotice:       distributorTimeoutNotice,
			valueShouldBeChanged: false,
		},
		{
			name:                 "keeps explicitly set old non-default value when useNewDefaults=true",
			useNewDefaults:       true,
			inYAML:               []byte("distributor: { remote_timeout: 15s }"),
			expectedYAML:         []byte("distributor: { remote_timeout: 15s }"),
			expectedNotice:       distributorTimeoutNotice,
			valueShouldBeChanged: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// We pass the common flags in, but ignore the output.
			// This is so that the always-present options in common-flags.txt get output in the flags instead of
			// in the out YAML. This helps to keep the test cases and expected YAML clean of
			// unrelated config options (e.g. server.http_listen_port)
			inFlags := loadFlags(t, "testdata/common-flags.txt")
			in := conversionInput{
				inYAML:         tc.inYAML,
				inFlags:        inFlags,
				useNewDefaults: tc.useNewDefaults,
			}

			testCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error) {
				require.NoError(t, err)

				assert.YAMLEq(t, string(tc.expectedYAML), string(outYAML))
				if tc.valueShouldBeChanged {
					assert.Contains(t, notices.ChangedDefaults, tc.expectedNotice)
					assert.NotContains(t, notices.SkippedChangedDefaults, tc.expectedNotice)
				} else {
					assert.Contains(t, notices.SkippedChangedDefaults, tc.expectedNotice)
					assert.NotContains(t, notices.ChangedDefaults, tc.expectedNotice)
				}
			})
		})
	}
}

func TestConvert_NotInYAMLIsNotPrinted(t *testing.T) {
	for _, useNewDefaults := range []bool{true, false} {
		for _, showDefaults := range []bool{true, false} {
			showDefaults, useNewDefaults := showDefaults, useNewDefaults
			t.Run(fmt.Sprintf("useNewDefault=%t_showDefaults=%t", useNewDefaults, showDefaults), func(t *testing.T) {
				in := conversionInput{
					useNewDefaults: useNewDefaults,
					outputDefaults: showDefaults,
					inYAML:         nil,
					inFlags:        nil,
				}
				testCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error) {
					assert.NoError(t, err)
					assert.NotContains(t, string(outYAML), notInYaml)
				})
			})
		}
	}
}

func TestConvert_PassingOnlyYAMLReturnsOnlyYAML(t *testing.T) {
	inYAML := []byte("distributor: { remote_timeout: 11s }")
	expectedOutYAML := []byte(`{distributor: { remote_timeout: 11s }, server: { http_listen_port: 80 }}`)

	in := conversionInput{
		inYAML: inYAML,
	}

	testCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error) {
		assert.NoError(t, err)
		assert.YAMLEq(t, string(expectedOutYAML), string(outYAML))
		assert.Empty(t, outFlags)
	})
}

func TestConvert_PassingOnlyFlagsReturnsOnlyFlags(t *testing.T) {
	inFlags := []string{"-distributor.remote-timeout=11s"}
	expectedOutFlags := append([]string{"-server.http-listen-port=80"}, inFlags...)

	in := conversionInput{
		inFlags: inFlags,
	}

	testCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error) {
		assert.NoError(t, err)
		assert.YAMLEq(t, "{}", string(outYAML))
		assert.ElementsMatch(t, expectedOutFlags, outFlags)
	})
}

func loadFile(t testing.TB, fileName string) []byte {
	t.Helper()

	if fileName == "" {
		return nil
	}
	bytes, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)
	return bytes
}

func loadFlags(t testing.TB, fileName string) []string {
	t.Helper()

	if fileName == "" {
		return nil
	}
	flagBytes, err := ioutil.ReadFile(fileName)
	require.NoError(t, err)
	return strings.Split(string(flagBytes), "\n")
}
