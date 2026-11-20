// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadDefinition_Default(t *testing.T) {
	def, err := LoadDefinition("")
	require.NoError(t, err)
	require.Equal(t, []Protocol{ProtocolV1, ProtocolV2, ProtocolOTLP}, def.Protocols)
	require.Equal(t, []int{1, 2, 5, 10}, def.WriteSizes)
	require.Equal(t, []Charset{CharsetLegacy, CharsetUTF8}, def.Charsets)
	require.Equal(t, MetadataDefinition{Units: []Unit{UnitNone, UnitSeconds}, Helps: []Help{HelpNone, HelpASCII, HelpUTF8}}, def.Metadata)
	require.Equal(t, allMetricTypes, def.Floats.Types)
	require.Equal(t, []SampleType{SampleTypeInt, SampleTypeFloat}, def.NativeHistograms.SampleTypes)
	require.Equal(t, []Kind{KindCounter, KindGauge}, def.NativeHistograms.Kinds)
	require.ElementsMatch(t, allSchemas(), def.NativeHistograms.Schemas)
	require.Equal(t, allLayouts, def.NativeHistograms.Layouts)
}

func TestLoadDefinition_File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "definition.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validDefinition), 0o600))

	def, err := LoadDefinition(path)
	require.NoError(t, err)
	require.Equal(t, []Protocol{ProtocolV2}, def.Protocols)
	require.Equal(t, []int{3}, def.WriteSizes)
	require.Equal(t, []Charset{CharsetUTF8}, def.Charsets)
	require.Equal(t, MetadataDefinition{Units: []Unit{UnitSeconds, UnitBytes}, Helps: []Help{HelpUTF8}}, def.Metadata)
	require.Nil(t, def.Floats, "float series aren't explored")
	require.Equal(t, []int32{0, 8, -53}, def.NativeHistograms.Schemas)
	require.Equal(t, []Layout{LayoutZeroOnly}, def.NativeHistograms.Layouts)
}

func TestParseDefinition_OnlyFloats(t *testing.T) {
	def, err := ParseDefinition([]byte("protocols: [v1]\nwrite_sizes: [1]\ncharsets: [legacy]\nmetadata: {units: [none], helps: [ascii]}\nfloats: {types: [gauge]}\n"))
	require.NoError(t, err)
	require.Equal(t, []MetricType{MetricTypeGauge}, def.Floats.Types)
	require.Nil(t, def.NativeHistograms, "native histogram series aren't explored")
}

const validDefinition = `
protocols: [v2]
write_sizes: [3]
charsets: [utf8]
metadata:
  units: [seconds, bytes]
  helps: [utf8]
native_histograms:
  sample_types: [int]
  kinds: [gauge]
  schemas: [0, 8, -53]
  layouts: [zero_only]
`

func TestParseDefinition_Invalid(t *testing.T) {
	const charsets = "protocols: [v1]\nwrite_sizes: [1]\ncharsets: [legacy]\n"
	const protocols = charsets + "metadata: {units: [none], helps: [ascii]}\n"
	const histograms = "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n"
	const nativeHistograms = protocols + histograms
	tests := map[string]struct {
		definition  string
		expectedErr string
	}{
		"unknown field": {
			definition:  nativeHistograms + "unknown: 1\n",
			expectedErr: "field unknown not found",
		},
		"no protocols": {
			definition:  "write_sizes: [1]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "protocols: at least one value is required",
		},
		"unknown protocol": {
			definition:  "protocols: [v1, otlp-http]\nwrite_sizes: [1]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "protocols: unknown value otlp-http",
		},
		"no write sizes": {
			definition:  "protocols: [v1]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "write_sizes: at least one value is required",
		},
		"duplicate write size": {
			definition:  "protocols: [v1]\nwrite_sizes: [2, 2]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "write_sizes: duplicate value 2",
		},
		"write size not positive": {
			definition:  "protocols: [v1]\nwrite_sizes: [1, 0]\ncharsets: [legacy]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "write_sizes: 0 is not positive",
		},
		"no charsets": {
			definition:  "protocols: [v1]\nwrite_sizes: [1]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "charsets: at least one value is required",
		},
		"unknown charset": {
			definition:  "protocols: [v1]\nwrite_sizes: [1]\ncharsets: [ascii]\nnative_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "charsets: unknown value ascii",
		},
		"no metadata": {
			definition:  charsets + histograms,
			expectedErr: "metadata: units: at least one value is required",
		},
		"unknown unit": {
			definition:  charsets + "metadata: {units: [none, minutes], helps: [ascii]}\n" + histograms,
			expectedErr: "metadata: units: unknown value minutes",
		},
		"no helps": {
			definition:  charsets + "metadata: {units: [none]}\n" + histograms,
			expectedErr: "metadata: helps: at least one value is required",
		},
		"duplicate help": {
			definition:  charsets + "metadata: {units: [none], helps: [utf8, utf8]}\n" + histograms,
			expectedErr: "metadata: helps: duplicate value utf8",
		},
		"no family": {
			definition:  protocols,
			expectedErr: "at least one of floats and native_histograms is required",
		},
		"no float types": {
			definition:  protocols + "floats: {types: []}\n",
			expectedErr: "floats: types: at least one value is required",
		},
		"unknown float type": {
			definition:  protocols + "floats: {types: [counter, summary]}\n",
			expectedErr: "floats: types: unknown value summary",
		},
		"empty dimension": {
			definition:  protocols + "native_histograms: {sample_types: [], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "sample_types: at least one value is required",
		},
		"unknown value": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [summary], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "kinds: unknown value summary",
		},
		"duplicate value": {
			definition:  protocols + "native_histograms: {sample_types: [int, int], kinds: [gauge], schemas: [0], layouts: [gaps]}\n",
			expectedErr: "sample_types: duplicate value int",
		},
		"schema out of range": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [9], layouts: [gaps]}\n",
			expectedErr: "schemas: 9 is neither an exponential schema",
		},
		"no layouts": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [0]}\n",
			expectedErr: "layouts: at least one value is required",
		},
		"unknown layout": {
			definition:  protocols + "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [0], layouts: [dense]}\n",
			expectedErr: "layouts: unknown value dense",
		},
		"paces of the previous format": {
			definition:  nativeHistograms + "paces:\n  dev: {write_interval: 10s}\n",
			expectedErr: "field paces not found",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDefinition([]byte(tc.definition))
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
