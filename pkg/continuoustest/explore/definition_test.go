// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadDefinition_Default(t *testing.T) {
	def, err := LoadDefinition("")
	require.NoError(t, err)
	require.Equal(t, []Protocol{ProtocolV1, ProtocolV2, ProtocolOTLP}, def.Protocols)
	require.Equal(t, []int{1, 2, 5, 10}, def.WriteSizes)
	require.Equal(t, []Charset{CharsetLegacy, CharsetUTF8}, def.Charsets)
	require.Equal(t, []SampleType{SampleTypeInt, SampleTypeFloat}, def.NativeHistograms.SampleTypes)
	require.Equal(t, []Kind{KindCounter, KindGauge}, def.NativeHistograms.Kinds)
	require.ElementsMatch(t, allSchemas(), def.NativeHistograms.Schemas)
	require.Equal(t, allLayouts, def.NativeHistograms.Layouts)
	require.Equal(t, map[string]Pace{"production": {WriteInterval: time.Minute}, "dev": {WriteInterval: 10 * time.Second}}, def.Paces)
}

func TestLoadDefinition_File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "definition.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validDefinition), 0o600))

	def, err := LoadDefinition(path)
	require.NoError(t, err)
	require.Equal(t, []Protocol{ProtocolV2}, def.Protocols)
	require.Equal(t, []int{3}, def.WriteSizes)
	require.Equal(t, []Charset{CharsetUTF8}, def.Charsets)
	require.Equal(t, []int32{0, 8, -53}, def.NativeHistograms.Schemas)
	require.Equal(t, []Layout{LayoutZeroOnly}, def.NativeHistograms.Layouts)
	require.Equal(t, Pace{WriteInterval: 10 * time.Second}, def.Paces["fast"])
}

const validDefinition = `
protocols: [v2]
write_sizes: [3]
charsets: [utf8]
native_histograms:
  sample_types: [int]
  kinds: [gauge]
  schemas: [0, 8, -53]
  layouts: [zero_only]
paces:
  fast:
    write_interval: 10s
`

func TestParseDefinition_Invalid(t *testing.T) {
	const paces = `
paces:
  fast: {write_interval: 10s}
`
	const protocols = "protocols: [v1]\nwrite_sizes: [1]\ncharsets: [legacy]\n"
	const nativeHistograms = protocols + `
native_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}
`
	tests := map[string]struct {
		definition  string
		expectedErr string
	}{
		"unknown field": {
			definition:  nativeHistograms + paces + "unknown: 1\n",
			expectedErr: "field unknown not found",
		},
		"no protocols": {
			definition:  "write_sizes: [1]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "protocols: at least one value is required",
		},
		"unknown protocol": {
			definition:  "protocols: [v1, otlp-http]\nwrite_sizes: [1]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "protocols: unknown value otlp-http",
		},
		"no write sizes": {
			definition:  "protocols: [v1]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "write_sizes: at least one value is required",
		},
		"duplicate write size": {
			definition:  "protocols: [v1]\nwrite_sizes: [2, 2]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "write_sizes: duplicate value 2",
		},
		"write size not positive": {
			definition:  "protocols: [v1]\nwrite_sizes: [1, 0]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "write_sizes: 0 is not positive",
		},
		"no charsets": {
			definition:  "protocols: [v1]\nwrite_sizes: [1]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "charsets: at least one value is required",
		},
		"unknown charset": {
			definition:  "protocols: [v1]\nwrite_sizes: [1]\ncharsets: [ascii]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "charsets: unknown value ascii",
		},
		"empty dimension": {
			definition:  protocols + "native_histograms: {sample_types: [], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "sample_types: at least one value is required",
		},
		"unknown value": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [summary], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "kinds: unknown value summary",
		},
		"duplicate value": {
			definition:  protocols + "native_histograms: {sample_types: [int, int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n" + paces,
			expectedErr: "sample_types: duplicate value int",
		},
		"schema out of range": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [9], layouts: [gaps]}\n" + paces,
			expectedErr: "schemas: 9 is neither an exponential schema",
		},
		"no layouts": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [0]}\n" + paces,
			expectedErr: "layouts: at least one value is required",
		},
		"unknown layout": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [dense]}\n" + paces,
			expectedErr: "layouts: unknown value dense",
		},
		"no paces": {
			definition:  nativeHistograms,
			expectedErr: "at least one pace is required",
		},
		"no write interval": {
			definition:  nativeHistograms + "paces:\n  p: {}\n",
			expectedErr: "paces: p: write_interval must be a positive number of seconds",
		},
		"write interval not a number of seconds": {
			definition:  nativeHistograms + "paces:\n  p: {write_interval: 1500ms}\n",
			expectedErr: "paces: p: write_interval must be a positive number of seconds",
		},
		"pace of the previous format": {
			definition:  nativeHistograms + "paces:\n  p: {slot_duration: 1m, write_interval: 10s}\n",
			expectedErr: "field slot_duration not found",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDefinition([]byte(tc.definition))
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
