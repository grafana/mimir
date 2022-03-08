// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConvert(t *testing.T) {
	testCases := []struct {
		name                      string
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
			name:    "exemplars limit rename",
			inFile:  "testdata/exemplars-old.yaml",
			outFile: "testdata/exemplars-new.yaml",
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
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inBytes := loadFile(t, tc.inFile)
			inFlags := loadFlags(t, tc.inFlagsFile)

			actualOut, actualOutFlags, _, err := Convert(inBytes, inFlags, CortexToMimirMapper, DefaultCortexConfig, DefaultMimirConfig, false, false)
			assert.NoError(t, err)

			expectedOut := loadFile(t, tc.outFile)
			expectedOutFlags := loadFlags(t, tc.outFlagsFile)

			assert.ElementsMatch(t, expectedOutFlags, actualOutFlags)
			if expectedOut == nil {
				expectedOut = []byte("{}")
			}
			assert.YAMLEq(t, string(expectedOut), string(actualOut))
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

func TestChangedDefaults(t *testing.T) {
	expectedChangedDefaults := []ChangedDefault{
		{Path: "activity_tracker.filepath", OldDefault: "./active-query-tracker", NewDefault: "./metrics-activity.log"},
		{Path: "alertmanager.data_dir", OldDefault: "data/", NewDefault: "./data-alertmanager/"},
		{Path: "alertmanager.enable_api", OldDefault: "false", NewDefault: "true"},
		{Path: "alertmanager.external_url", OldDefault: "", NewDefault: "http://localhost:8080/alertmanager"},
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
		{Path: "compactor.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
		{Path: "compactor.sharding_ring.wait_stability_min_duration", OldDefault: "1m0s", NewDefault: "0s"},
		{Path: "distributor.instance_limits.max_inflight_push_requests", OldDefault: "0", NewDefault: "2000"},
		{Path: "distributor.remote_timeout", OldDefault: "2s", NewDefault: "20s"},
		{Path: "distributor.ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
		{Path: "frontend.grpc_client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
		{Path: "frontend.query_stats_enabled", OldDefault: "false", NewDefault: "true"},
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
		{Path: "ruler.ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
		{Path: "ruler.rule_path", OldDefault: "/rules", NewDefault: "./data-ruler/"},
		{Path: "ruler.ruler_client.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
		{Path: "ruler_storage.backend", OldDefault: "s3", NewDefault: "filesystem"},
		{Path: "ruler_storage.filesystem.dir", OldDefault: "", NewDefault: "ruler"},
		{Path: "server.http_listen_port", OldDefault: "80", NewDefault: "8080"},
		{Path: "store_gateway.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
		{Path: "store_gateway.sharding_ring.wait_stability_min_duration", OldDefault: "1m0s", NewDefault: "0s"},
	}

	// Create cortex config where all params have explicitly set default values
	params := DefaultCortexConfig()
	err := params.Walk(func(path string, value interface{}) error {
		return params.SetValue(path, params.MustGetDefaultValue(path))
	})
	require.NoError(t, err)
	config, err := yaml.Marshal(params)
	require.NoError(t, err)

	// Convert while also converting explicitly set defaults to new defaults
	_, _, notices, err := Convert(config, nil, CortexToMimirMapper, DefaultCortexConfig, DefaultMimirConfig, true, false)
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
			t.Parallel()
			outYAML, _, notices, err := Convert(tc.inYAML, nil, CortexToMimirMapper, DefaultCortexConfig, DefaultMimirConfig, tc.useNewDefaults, false)
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
	}
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
