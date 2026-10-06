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
	require.Equal(t, []SampleType{SampleTypeInt, SampleTypeFloat}, def.NativeHistograms.SampleTypes)
	require.Equal(t, []Kind{KindCounter, KindGauge}, def.NativeHistograms.Kinds)
	require.Len(t, def.NativeHistograms.Schemas, 13)
	require.Contains(t, def.Paces, "production")
	require.Contains(t, def.Paces, "dev")
	require.Equal(t, time.Hour, def.Paces["production"].SlotDuration)
}

func TestLoadDefinition_File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "definition.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validDefinition), 0o600))

	def, err := LoadDefinition(path)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 8}, def.NativeHistograms.Schemas)
	require.Equal(t, Pace{SlotDuration: time.Minute, ScenarioLifetime: 2 * time.Minute, WriteInterval: 10 * time.Second, SeriesPerScenario: 2, MaxActiveSeries: 4}, def.Paces["fast"])
}

const validDefinition = `
native_histograms:
  sample_types: [int]
  kinds: [gauge]
  schemas: [0, 8]
paces:
  fast:
    slot_duration: 1m
    scenario_lifetime: 2m
    write_interval: 10s
    series_per_scenario: 2
    max_active_series: 4
`

func TestParseDefinition_Invalid(t *testing.T) {
	const paces = `
paces:
  fast: {slot_duration: 1m, scenario_lifetime: 2m, write_interval: 10s, series_per_scenario: 2, max_active_series: 4}
`
	const nativeHistograms = `
native_histograms: {sample_types: [int], kinds: [gauge], schemas: [0]}
`
	tests := map[string]struct {
		definition  string
		expectedErr string
	}{
		"unknown field": {
			definition:  nativeHistograms + paces + "unknown: 1\n",
			expectedErr: "field unknown not found",
		},
		"empty dimension": {
			definition:  "native_histograms: {sample_types: [], kinds: [gauge], schemas: [0]}\n" + paces,
			expectedErr: "sample_types: at least one value is required",
		},
		"unknown value": {
			definition:  "native_histograms: {sample_types: [int], kinds: [summary], schemas: [0]}\n" + paces,
			expectedErr: "kinds: unknown value summary",
		},
		"duplicate value": {
			definition:  "native_histograms: {sample_types: [int, int], kinds: [gauge], schemas: [0]}\n" + paces,
			expectedErr: "sample_types: duplicate value int",
		},
		"schema out of range": {
			definition:  "native_histograms: {sample_types: [int], kinds: [gauge], schemas: [9]}\n" + paces,
			expectedErr: "schemas: 9 is not an exponential schema",
		},
		"no paces": {
			definition:  nativeHistograms,
			expectedErr: "at least one pace is required",
		},
		"slot not a multiple of write interval": {
			definition:  nativeHistograms + "paces:\n  p: {slot_duration: 15s, scenario_lifetime: 20s, write_interval: 10s, series_per_scenario: 1, max_active_series: 10}\n",
			expectedErr: "must be multiples of write_interval",
		},
		"too many active series": {
			definition:  nativeHistograms + "paces:\n  p: {slot_duration: 1m, scenario_lifetime: 3m, write_interval: 10s, series_per_scenario: 2, max_active_series: 5}\n",
			expectedErr: "up to 6 series can be active at the same time, which exceeds max_active_series 5",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDefinition([]byte(tc.definition))
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
