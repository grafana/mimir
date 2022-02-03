// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMigrate(t *testing.T) {
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
			name:    "simple exemplars rename",
			inFile:  "testdata/exemplars-old.yaml",
			outFile: "testdata/exemplars-new.yaml",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			inBytes, err := ioutil.ReadFile(tc.inFile)
			require.NoError(t, err)

			outBytes, err := Migrator{}.Migrate(inBytes)
			assert.NoError(t, err)

			expectedOut, err := ioutil.ReadFile(tc.outFile)
			require.NoError(t, err)

			assert.YAMLEq(t, string(expectedOut), string(outBytes))
		})
	}
}
