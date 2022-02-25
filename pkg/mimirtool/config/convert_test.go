// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvert(t *testing.T) {
	testCases := []struct {
		name            string
		inFile, outFile string
	}{
		{
			name:    "shouldn't need any conversion",
			inFile:  "testdata/noop-old.yaml",
			outFile: "testdata/noop-new.yaml",
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
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inBytes, err := ioutil.ReadFile(tc.inFile)
			require.NoError(t, err)

			outBytes, err := Convert(inBytes, CortexToMimirMapper, DefaultCortexConfig, DefaultMimirConfig)
			assert.NoError(t, err)

			expectedOut, err := ioutil.ReadFile(tc.outFile)
			require.NoError(t, err)

			assert.YAMLEq(t, string(expectedOut), string(outBytes))
		})
	}
}
