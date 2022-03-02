// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name:    "ruler.storage maps to ruler.storage",
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
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inBytes := loadFile(t, tc.inFile)
			inFlags := loadFlags(t, tc.inFlagsFile)

			actualOut, actualOutFlags, err := Convert(inBytes, inFlags, CortexToMimirMapper, DefaultCortexConfig, DefaultMimirConfig)
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
