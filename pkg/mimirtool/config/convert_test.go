// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type conversionInput struct {
	useNewDefaults     bool
	outputDefaults     bool
	yaml               []byte
	flags              []string
	dontLoadCommonOpts bool
}

func (in *conversionInput) loadCommonOpts(t *testing.T, oldYAMLFile, newYAMLFile, oldFlagsFile, newFlagsFile string) (newCommonYAML []byte, newCommonFlags []string) {
	if in.dontLoadCommonOpts {
		return nil, nil
	}
	if in.yaml == nil {
		in.yaml = loadFile(t, oldYAMLFile)
		newCommonYAML = loadFile(t, newYAMLFile)
	}
	if in.flags == nil {
		in.flags = loadFlags(t, oldFlagsFile)
		newCommonFlags = loadFlags(t, newFlagsFile)
	}

	return
}

func testConvertCortexAndGEM(t *testing.T, tc conversionInput, test func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error)) {
	testConvertCortex(t, tc, test)
	testConvertGEM(t, tc, test)
}

func testConvertGEM(t *testing.T, tc conversionInput, test func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error)) {
	t.Run("gem170->gem", func(t *testing.T) {
		t.Parallel()

		expectedCommonYAML, expectedCommonFlags := tc.loadCommonOpts(t, "testdata/gem/common-options-old.yaml", "testdata/gem/common-options-new.yaml", "testdata/gem/common-flags-old.txt", "testdata/gem/common-flags-new.txt")
		outYAML, outFlags, notices, err := Convert(tc.yaml, tc.flags, GEM170ToGEMMapper(), DefaultGEM170Config, DefaultGEMConfig, tc.useNewDefaults, tc.outputDefaults)

		if expectedCommonYAML != nil {
			assert.YAMLEq(t, string(expectedCommonYAML), string(outYAML), "common config options did not map correctly")
			outYAML = nil
		}
		if expectedCommonFlags != nil {
			assert.ElementsMatch(t, expectedCommonFlags, outFlags, "common config options did not map correctly")
			outFlags = []string{}
		}

		test(t, outYAML, outFlags, notices, err)
	})
}

func testConvertCortex(t *testing.T, tc conversionInput, test func(t *testing.T, outYAML []byte, outFlags []string, notices ConversionNotices, err error)) {
	t.Run("cortex->mimir", func(t *testing.T) {
		t.Parallel()

		expectedCommonYAML, expectedCommonFlags := tc.loadCommonOpts(t, "testdata/common-options-old.yaml", "testdata/common-options-new.yaml", "testdata/common-flags-old.txt", "testdata/common-flags-new.txt")
		outYAML, outFlags, notices, err := Convert(tc.yaml, tc.flags, CortexToMimirMapper(), DefaultCortexConfig, DefaultMimirConfig, tc.useNewDefaults, tc.outputDefaults)

		if expectedCommonYAML != nil {
			assert.YAMLEq(t, string(expectedCommonYAML), string(outYAML), "common config options did not map correctly")
			outYAML = nil
		}
		if expectedCommonFlags != nil {
			assert.ElementsMatch(t, expectedCommonFlags, outFlags, "common config options did not map correctly")
			outFlags = []string{}
		}

		test(t, outYAML, outFlags, notices, err)
	})
}

func TestConvert_Cortex(t *testing.T) {
	testCases := []struct {
		name                      string
		useNewDefaults            bool
		inFile, outFile           string
		inFlagsFile, outFlagsFile string
		skipGEMTest               bool
		dontAddCommonOpts         bool
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
			outFile:      "testdata/common-options-new.yaml",
			skipGEMTest:  true, // no need to test this in GEM too; plus, output for GEM also includes GEM common opts
		},
		{
			name:    "ruler.storage maps to ruler_storage",
			inFile:  "testdata/ruler.storage-old.yaml",
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
			name:              "values where the old default should be retained are printed even when implicitly using the old default",
			skipGEMTest:       true,
			dontAddCommonOpts: true, // The common opts are in the outFile. That's the same reason why this test case doesn't work for GEM
			inFile:            "testdata/empty.yaml",
			outFile:           "testdata/common-options-new.yaml",
		},
		{
			name:              "values where the old default should be retained are retained even with useNewDefaults=true",
			skipGEMTest:       true,
			dontAddCommonOpts: true, // The common opts are in the outFile. That's the same reason why this test case doesn't work for GEM
			useNewDefaults:    true,
			inFile:            "testdata/common-options-old.yaml",
			outFile:           "testdata/common-options-new.yaml",
		},
		{
			name:              "values where the old default should be retained are retained with useNewDefaults=false",
			skipGEMTest:       true,
			dontAddCommonOpts: true, // The common opts are in the outFile. That's the same reason why this test case doesn't work for GEM
			useNewDefaults:    true,
			inFile:            "testdata/common-options-old.yaml",
			outFile:           "testdata/common-options-new.yaml",
		},
		{
			name:              "values where the old default should be retained but are with random values are retained with useNewDefaults=false",
			skipGEMTest:       true,
			dontAddCommonOpts: true, // The common opts are in the outFile. That's the same reason why this test case doesn't work for GEM
			inFile:            "testdata/params-with-special-old-defaults-custom-values-old.yaml",
			outFile:           "testdata/params-with-special-old-defaults-custom-values-new.yaml",
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
			name:         "instance_id is preserved",
			inFlagsFile:  "testdata/ring-instance-id-old.flags.txt",
			outFlagsFile: "testdata/ring-instance-id-new.flags.txt",
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
			t.Parallel()
			expectedOut := loadFile(t, tc.outFile)
			expectedOutFlags := loadFlags(t, tc.outFlagsFile)

			in := conversionInput{
				useNewDefaults:     tc.useNewDefaults,
				dontLoadCommonOpts: tc.dontAddCommonOpts,
				yaml:               loadFile(t, tc.inFile),
				flags:              loadFlags(t, tc.inFlagsFile),
			}

			assertion := func(t *testing.T, outYAML []byte, outFlags []string, _ ConversionNotices, err error) {
				assert.NoError(t, err)
				assert.ElementsMatch(t, expectedOutFlags, outFlags)
				assert.YAMLEq(t, string(expectedOut), string(outYAML))
			}
			testConvertCortex(t, in, assertion)
			if !tc.skipGEMTest {
				testConvertGEM(t, in, assertion)
			}
		})
	}
}

func TestConvert_GEM(t *testing.T) {
	testCases := []struct {
		name                      string
		useNewDefaults            bool
		inFile, outFile           string
		inFlagsFile, outFlagsFile string
		dontAddCommonOpts         bool
	}{
		{
			name:    "proxy_targets get translated",
			inFile:  "testdata/gem/proxy-targets.yaml",
			outFile: "testdata/gem/proxy-targets.yaml",
		},
		{
			name:              "values where the old default should be retained are printed even when implicitly using the old default",
			dontAddCommonOpts: true, // The common opts are in the outFile.
			inFile:            "testdata/empty.yaml",
			outFile:           "testdata/gem/common-options-new.yaml",
		},
		{
			name:              "values where the old default should be retained are retained even with useNewDefaults=true",
			dontAddCommonOpts: true, // The common opts are in the outFile.
			useNewDefaults:    true,
			inFile:            "testdata/gem/common-options-old.yaml",
			outFile:           "testdata/gem/common-options-new.yaml",
		},
		{
			name:              "values where the old default should be retained are retained with useNewDefaults=false",
			dontAddCommonOpts: true, // The common opts are in the outFile.
			useNewDefaults:    true,
			inFile:            "testdata/gem/common-options-old.yaml",
			outFile:           "testdata/gem/common-options-new.yaml",
		},
		{
			name:              "values where the old default should be retained but are with random values are retained with useNewDefaults=false",
			dontAddCommonOpts: true, // The common opts are in the outFile.
			inFile:            "testdata/gem/params-with-special-old-defaults-custom-values-old.yaml",
			outFile:           "testdata/gem/params-with-special-old-defaults-custom-values-new.yaml",
		},
		{
			name:         "instance_id is preserved",
			inFlagsFile:  "testdata/gem/ring-instance-id-old.flags.txt",
			outFlagsFile: "testdata/gem/ring-instance-id-new.flags.txt",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			expectedOut := loadFile(t, tc.outFile)
			expectedOutFlags := loadFlags(t, tc.outFlagsFile)

			in := conversionInput{
				useNewDefaults:     tc.useNewDefaults,
				dontLoadCommonOpts: tc.dontAddCommonOpts,
				yaml:               loadFile(t, tc.inFile),
				flags:              loadFlags(t, tc.inFlagsFile),
			}

			testConvertGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, _ ConversionNotices, err error) {
				assert.NoError(t, err)
				assert.ElementsMatch(t, expectedOutFlags, outFlags)
				assert.YAMLEq(t, string(expectedOut), string(outYAML))
			})
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
			t.Parallel()
			in := conversionInput{
				flags:              loadFlags(t, tc.inFlagsFile),
				yaml:               loadFile(t, tc.inFile),
				dontLoadCommonOpts: true,
			}
			testConvertCortexAndGEM(t, in, func(t *testing.T, _ []byte, _ []string, _ ConversionNotices, err error) {
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
	{Path: "alertmanager.enable_api", OldDefault: "false", NewDefault: "true"},
	{Path: "alertmanager.external_url", OldDefault: "", NewDefault: "http://localhost:8080/alertmanager"},
	{Path: "alertmanager.sharding_ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "alertmanager.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "alertmanager_storage.filesystem.dir", OldDefault: "", NewDefault: "alertmanager"},
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
	{Path: "blocks_storage.tsdb.close_idle_tsdb_timeout", OldDefault: "0s", NewDefault: "13h0m0s"},
	{Path: "blocks_storage.tsdb.dir", OldDefault: "tsdb", NewDefault: "./tsdb/"},
	{Path: "blocks_storage.tsdb.retention_period", OldDefault: "6h0m0s", NewDefault: "24h0m0s"},
	{Path: "compactor.block_sync_concurrency", OldDefault: "20", NewDefault: "8"},
	{Path: "compactor.sharding_ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "compactor.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "compactor.sharding_ring.wait_stability_min_duration", OldDefault: "1m0s", NewDefault: "0s"},
	{Path: "distributor.instance_limits.max_inflight_push_requests", OldDefault: "0", NewDefault: "2000"},
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
	{Path: "limits.max_global_series_per_user", OldDefault: "0", NewDefault: "150000"},
	{Path: "limits.ruler_max_rule_groups_per_tenant", OldDefault: "0", NewDefault: "70"},
	{Path: "limits.ruler_max_rules_per_rule_group", OldDefault: "0", NewDefault: "20"},
	{Path: "querier.query_ingesters_within", OldDefault: "0s", NewDefault: "13h0m0s"},
	{Path: "query_scheduler.grpc_client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "ruler.enable_api", OldDefault: "false", NewDefault: "true"},
	{Path: "ruler.ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "ruler.ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "ruler.ruler_client.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "store_gateway.sharding_ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
	{Path: "store_gateway.sharding_ring.kvstore.store", OldDefault: "consul", NewDefault: "memberlist"},
	{Path: "store_gateway.sharding_ring.wait_stability_min_duration", OldDefault: "1m0s", NewDefault: "0s"},

	// Changed in 2.1, 2.2 and 2.3
	{Path: "alertmanager.max_recv_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
	{Path: "limits.ha_max_clusters", OldDefault: "0", NewDefault: "100"},
	{Path: "memberlist.abort_if_cluster_join_fails", OldDefault: "true", NewDefault: "false"},
	{Path: "querier.query_store_after", OldDefault: "0s", NewDefault: "12h0m0s"},
	{Path: "server.grpc_server_max_recv_msg_size", OldDefault: "4194304", NewDefault: "104857600"},
	{Path: "server.grpc_server_max_send_msg_size", OldDefault: "4194304", NewDefault: "104857600"},
	{Path: "memberlist.leave_timeout", OldDefault: "5s", NewDefault: "20s"},
	{Path: "memberlist.packet_dial_timeout", OldDefault: "5s", NewDefault: "2s"},

	// Changed in 2.4, 2.5, 2.6
	{Path: "server.http_server_write_timeout", OldDefault: "30s", NewDefault: "2m0s"},
	{Path: "distributor.ring.heartbeat_period", OldDefault: "5s", NewDefault: "15s"},
	{Path: "ingester.ring.heartbeat_period", OldDefault: "5s", NewDefault: "15s"},
	{Path: "blocks_storage.tsdb.head_compaction_concurrency", OldDefault: "5", NewDefault: "1"},
	{Path: "compactor.sharding_ring.heartbeat_period", OldDefault: "5s", NewDefault: "15s"},
	{Path: "ruler.for_grace_period", OldDefault: "10m0s", NewDefault: "2m0s"},
	{Path: "ruler.ring.heartbeat_period", OldDefault: "5s", NewDefault: "15s"},
}

func TestChangedCortexDefaults(t *testing.T) {
	// Create cortex config where all params have explicitly set default values so that all of them can be changed and reported as changed
	params := DefaultCortexConfig()
	err := params.Walk(func(path string, _ Value) error {
		return params.SetValue(path, params.MustGetDefaultValue(path))
	})
	require.NoError(t, err)
	config, err := yaml.Marshal(params)
	require.NoError(t, err)

	in := conversionInput{
		useNewDefaults: true,
		yaml:           config,
	}

	testConvertCortex(t, in, func(t *testing.T, _ []byte, _ []string, notices ConversionNotices, err error) {
		require.NoError(t, err)
		assert.ElementsMatch(t, changedCortexDefaults, notices.ChangedDefaults)
	})
}

func TestChangedGEMDefaults(t *testing.T) {
	changedGEMSpecificDefaults := []ChangedDefault{
		{Path: "admin_api.leader_election.client_config.max_send_msg_size", OldDefault: "16777216", NewDefault: "104857600"},
		{Path: "admin_api.leader_election.enabled", OldDefault: "false", NewDefault: "true"},
		{Path: "admin_api.leader_election.ring.instance_interface_names", OldDefault: "eth0,en0", NewDefault: "<nil>"},
		{Path: "graphite.enabled", OldDefault: "false", NewDefault: "true"},
		{Path: "graphite.querier.aggregation_cache.memcached.addresses", OldDefault: "dnssrvnoa+_memcached._tcp.", NewDefault: ""},
		{Path: "graphite.querier.aggregation_cache.memcached.max_async_buffer_size", OldDefault: "10000", NewDefault: "25000"},
		{Path: "graphite.querier.aggregation_cache.memcached.max_async_concurrency", OldDefault: "10", NewDefault: "50"},
		{Path: "graphite.querier.aggregation_cache.memcached.max_get_multi_batch_size", OldDefault: "1024", NewDefault: "100"},
		{Path: "graphite.querier.aggregation_cache.memcached.max_idle_connections", OldDefault: "16", NewDefault: "100"},
		{Path: "graphite.querier.aggregation_cache.memcached.max_item_size", OldDefault: "0", NewDefault: "1048576"},
		{Path: "graphite.querier.aggregation_cache.memcached.timeout", OldDefault: "100ms", NewDefault: "200ms"},
		{Path: "graphite.querier.metric_name_cache.memcached.addresses", OldDefault: "dnssrvnoa+_memcached._tcp.", NewDefault: ""},
		{Path: "graphite.querier.metric_name_cache.memcached.max_async_buffer_size", OldDefault: "10000", NewDefault: "25000"},
		{Path: "graphite.querier.metric_name_cache.memcached.max_async_concurrency", OldDefault: "10", NewDefault: "50"},
		{Path: "graphite.querier.metric_name_cache.memcached.max_get_multi_batch_size", OldDefault: "1024", NewDefault: "100"},
		{Path: "graphite.querier.metric_name_cache.memcached.max_idle_connections", OldDefault: "16", NewDefault: "100"},
		{Path: "graphite.querier.metric_name_cache.memcached.max_item_size", OldDefault: "0", NewDefault: "1048576"},
		{Path: "graphite.querier.metric_name_cache.memcached.timeout", OldDefault: "100ms", NewDefault: "200ms"},
		{Path: "instrumentation.enabled", OldDefault: "false", NewDefault: "true"},
		{Path: "limits.compactor_split_groups", OldDefault: "4", NewDefault: "1"},
		{Path: "limits.compactor_tenant_shard_size", OldDefault: "1", NewDefault: "0"},

		// Changed in 2.1
		{Path: "blocks_storage.bucket_store.chunks_cache.attributes_in_memory_max_items", OldDefault: "0", NewDefault: "50000"},

		// Changed in 2.5 (not in GEM changelog)
		{Path: "graphite.querier.query_handling_concurrency", OldDefault: "8", NewDefault: "32"},
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

	in := conversionInput{
		useNewDefaults: true,
		yaml:           config,
	}

	testConvertGEM(t, in, func(t *testing.T, _ []byte, _ []string, notices ConversionNotices, err error) {
		require.NoError(t, err)
		assert.ElementsMatch(t, expectedChangedDefaults, notices.ChangedDefaults)
	})
}

func TestConvert_UseNewDefaults(t *testing.T) {
	distributorTimeoutNotice := ChangedDefault{
		Path:       "blocks_storage.tsdb.close_idle_tsdb_timeout",
		OldDefault: "0s",
		NewDefault: "13h0m0s",
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
			inYAML:               []byte("blocks_storage: { tsdb: { close_idle_tsdb_timeout: 0s } }"),
			expectedYAML:         []byte("blocks_storage: { tsdb: { close_idle_tsdb_timeout: 13h0m0s } }"),
			expectedNotice:       distributorTimeoutNotice,
			valueShouldBeChanged: true,
		},
		{
			name:                 "keeps explicitly set old defaults useNewDefaults=false",
			useNewDefaults:       false,
			inYAML:               []byte("blocks_storage: { tsdb: { close_idle_tsdb_timeout: 0s } }"),
			expectedYAML:         []byte("blocks_storage: { tsdb: { close_idle_tsdb_timeout: 0s } }"),
			expectedNotice:       distributorTimeoutNotice,
			valueShouldBeChanged: false,
		},
		{
			name:                 "keeps explicitly set old non-default value when useNewDefaults=true",
			useNewDefaults:       true,
			inYAML:               []byte("blocks_storage: { tsdb: { close_idle_tsdb_timeout: 1h0m0s } }"),
			expectedYAML:         []byte("blocks_storage: { tsdb: { close_idle_tsdb_timeout: 1h0m0s } }"),
			expectedNotice:       distributorTimeoutNotice,
			valueShouldBeChanged: false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := conversionInput{
				yaml:           tc.inYAML,
				useNewDefaults: tc.useNewDefaults,
			}

			testConvertCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, _ []string, notices ConversionNotices, err error) {
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
				t.Parallel()
				in := conversionInput{
					useNewDefaults:     useNewDefaults,
					outputDefaults:     showDefaults,
					dontLoadCommonOpts: true,
				}
				testConvertCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, _ []string, _ ConversionNotices, err error) {
					assert.NoError(t, err)
					assert.NotContains(t, string(outYAML), notInYaml)
				})
			})
		}
	}
}

func TestConvert_PassingOnlyYAMLReturnsOnlyYAML(t *testing.T) {
	inYAML := []byte("distributor: { remote_timeout: 11s }")
	expectedOutYAML := inYAML

	in := conversionInput{
		yaml: inYAML,
	}

	testConvertCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, _ ConversionNotices, err error) {
		assert.NoError(t, err)
		assert.YAMLEq(t, string(expectedOutYAML), string(outYAML))
		assert.Empty(t, outFlags)
	})
}

func TestConvert_PassingOnlyFlagsReturnsOnlyFlags(t *testing.T) {
	inFlags := []string{"-distributor.remote-timeout=11s"}
	expectedOutFlags := inFlags

	in := conversionInput{
		flags: inFlags,
	}

	testConvertCortexAndGEM(t, in, func(t *testing.T, outYAML []byte, outFlags []string, _ ConversionNotices, err error) {
		assert.NoError(t, err)
		assert.Empty(t, outYAML)
		assert.ElementsMatch(t, expectedOutFlags, outFlags)
	})
}

// TestRemovedParamsAndFlagsAreCorrect checks if what we have in removedCLIOptions and removedConfigPaths makes sense.
// It cannot test for all flags that were present at some point but now don't exist because some of them could be renamed
// or merged with other flags.
func TestRemovedParamsAndFlagsAreCorrect(t *testing.T) {
	t.Parallel()

	allParameterPaths := func(p Parameters) map[string]bool {
		paths := map[string]bool{}
		assert.NoError(t, p.Walk(func(path string, _ Value) error {
			paths[path] = true
			return nil
		}))
		return paths
	}

	allCLIFlagsNames := func(p Parameters) map[string]bool {
		flags := map[string]bool{}
		assert.NoError(t, p.Walk(func(path string, _ Value) error {
			flagName, err := p.GetFlag(path)
			assert.NoError(t, err)
			flags[flagName] = true
			return nil
		}))
		return flags
	}

	// Test that whatever we claim to be removed is actually not present now
	gemPaths := allParameterPaths(DefaultGEMConfig())
	mimirPaths := allParameterPaths(DefaultMimirConfig())
	for _, path := range removedConfigPaths {
		assert.NotContains(t, gemPaths, path)
		assert.NotContains(t, mimirPaths, path)
	}

	gemFlags := allCLIFlagsNames(DefaultGEMConfig())
	mimirFlags := allCLIFlagsNames(DefaultMimirConfig())
	for _, path := range removedCLIOptions {
		assert.NotContains(t, mimirFlags, path)
		assert.NotContains(t, gemFlags, path)
	}

	// Test that whatever we claim to be removed was actually present in either GEM or cortex previously
	oldGEMPaths := allParameterPaths(DefaultGEM170Config())
	cortexPaths := allParameterPaths(DefaultCortexConfig())
	for _, path := range removedConfigPaths {
		cortexHas := cortexPaths[path]
		gemHas := oldGEMPaths[path]
		assert.Truef(t, cortexHas || gemHas, "path %s is expected to exist in either old GEM or old Cortex config, but was found in neither", path)
	}

	oldGEMFlags := allCLIFlagsNames(DefaultGEM170Config())
	cortexFlags := allCLIFlagsNames(DefaultCortexConfig())
	for _, flag := range removedCLIOptions {
		cortexHas := oldGEMFlags[flag]
		gemHas := cortexFlags[flag]
		assert.Truef(t, cortexHas || gemHas, "CLI flag %s is expected to exist in either old GEM or old Cortex config, but was found in neither", flag)
	}
}

func loadFile(t testing.TB, fileName string) []byte {
	t.Helper()

	if fileName == "" {
		return nil
	}
	bytes, err := os.ReadFile(fileName)
	require.NoError(t, err)
	return bytes
}

func loadFlags(t testing.TB, fileName string) []string {
	t.Helper()

	if fileName == "" {
		return nil
	}
	flagBytes, err := os.ReadFile(fileName)
	require.NoError(t, err)
	return strings.Split(string(flagBytes), "\n")
}
