// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/stretchr/testify/require"
)

func TestExplorer_DefaultDefinition(t *testing.T) {
	type counts struct{ v1, v2, otlp, series int }
	for scheme, expected := range map[model.ValidationScheme]counts{
		// Remote write writes the 5 float types and every native histogram combination but the 8 of
		// custom buckets with negative or zero buckets, of the legacy charset. OTLP writes the float
		// counters and gauges, and the 52 native histogram int counters with exponential buckets, of
		// each charset. Each of them has the 6 combinations of a unit and a help text.
		model.LegacyValidation: {v1: 6 * (5 + 216), v2: 6 * (5 + 216), otlp: 6 * (4 + 104), series: 6 * (5 + 216 + 2 + 52)},
		model.UTF8Validation:   {v1: 6 * (10 + 432), v2: 6 * (10 + 432), otlp: 6 * (4 + 104), series: 6 * (10 + 432)},
	} {
		t.Run(scheme.String(), func(t *testing.T) {
			e := testDefaultExplorer(t, scheme)
			require.Len(t, e.allSeries(ProtocolV1), expected.v1)
			require.Len(t, e.allSeries(ProtocolV2), expected.v2)
			require.Len(t, e.allSeries(ProtocolOTLP), expected.otlp)
			require.Equal(t, expected.series, e.numSeries())

			for _, p := range e.protocols {
				for _, s := range e.allSeries(p) {
					if histogram.IsCustomBucketsSchema(s.Schema) {
						require.Contains(t, []Layout{LayoutContiguous, LayoutGaps}, s.Layout, "series %s", s)
					}
					if p == ProtocolOTLP && s.Family == FamilyFloat {
						require.Contains(t, []MetricType{MetricTypeCounter, MetricTypeGauge}, s.Type, "series %s", s)
					} else if p == ProtocolOTLP {
						require.Equal(t, SampleTypeInt, s.SampleType, "series %s", s)
						require.Equal(t, KindCounter, s.Kind, "series %s", s)
						require.False(t, histogram.IsCustomBucketsSchema(s.Schema), "series %s", s)
					} else if scheme == model.LegacyValidation {
						require.Equal(t, CharsetLegacy, s.Charset, "series %s", s)
					}
				}
			}
		})
	}
}

func TestExplorer_Pick(t *testing.T) {
	e := testDefaultExplorer(t, model.LegacyValidation)
	rng := newRand(1, 0)
	// Every draw is a choice: values that can't be written with the values drawn before aren't
	// offered, and neither are single values.
	choose := func(n int) int {
		require.Greater(t, n, 1)
		return rng.IntN(n)
	}
	protocols, sizes := map[Protocol]bool{}, map[int]bool{}
	for range 1000 {
		protocol, series := e.pick(choose)
		protocols[protocol] = true
		sizes[len(series)] = true
		seen := map[string]bool{}
		for _, s := range series {
			require.False(t, seen[s.String()], "the series of a write must be distinct")
			seen[s.String()] = true
			require.Contains(t, e.allSeries(protocol), s, "protocol %s can't write series %s", protocol, s)
		}
	}
	require.Equal(t, map[Protocol]bool{ProtocolV1: true, ProtocolV2: true, ProtocolOTLP: true}, protocols)
	require.Equal(t, map[int]bool{1: true, 2: true, 5: true, 10: true}, sizes)
}

// Each series of a write is of a family drawn with the same chance for every family, even though
// there are far fewer float series than native histogram series.
func TestExplorer_PickDrawsTheFamilyFirst(t *testing.T) {
	def, err := LoadDefinition("")
	require.NoError(t, err)
	def.WriteSizes = []int{1}
	e, err := newExplorer(1, def, exclusions(model.LegacyValidation))
	require.NoError(t, err)
	require.Len(t, e.allSeries(ProtocolV1), 6*(5+216))

	rng := newRand(1, 0)
	families := map[Family]int{}
	for range 10000 {
		_, series := e.pick(rng.IntN)
		families[series[0].Family]++
	}
	require.InDelta(t, 5000, families[FamilyFloat], 200)
	require.InDelta(t, 5000, families[FamilyNativeHistogram], 200)
}

func TestExplorer_PickFillsTheWriteWithTheOtherFamilies(t *testing.T) {
	// v1 writes a float gauge and 2 native histogram counters.
	def := testDefinitionWith(t, "protocols: [v1]\nwrite_sizes: [3]\ncharsets: [legacy]\nfloats: {types: [gauge]}\n")
	e, err := newExplorer(1, def, exclusions(model.LegacyValidation))
	require.NoError(t, err)
	for stream := range uint64(10) {
		_, series := e.pick(newRand(1, stream).IntN)
		require.ElementsMatch(t, e.allSeries(ProtocolV1), series)
	}
}

func TestExplorer_PickIsDeterministic(t *testing.T) {
	e := testDefaultExplorer(t, model.LegacyValidation)
	differ := false
	for stream := range uint64(10) {
		p1, s1 := e.pick(newRand(1, stream).IntN)
		p2, s2 := e.pick(newRand(1, stream).IntN)
		require.Equal(t, p1, p2)
		require.Equal(t, s1, s2)

		p3, s3 := e.pick(newRand(2, stream).IntN)
		differ = differ || p1 != p3 || len(s1) != len(s3) || s1[0].String() != s3[0].String()
	}
	require.True(t, differ, "another seed should make other choices")
}

func TestExplorer_PicksEverySeries(t *testing.T) {
	e := testDefaultExplorer(t, model.LegacyValidation)
	rng := newRand(1, 0)
	picked := map[Protocol]map[string]bool{}
	for range 30000 {
		protocol, series := e.pick(rng.IntN)
		if picked[protocol] == nil {
			picked[protocol] = map[string]bool{}
		}
		for _, s := range series {
			picked[protocol][s.String()] = true
		}
	}
	for _, protocol := range e.protocols {
		require.Len(t, picked[protocol], len(e.allSeries(protocol)), "protocol %s", protocol)
	}
}

func TestExplorer_WriteSizeIsCappedByTheSeries(t *testing.T) {
	def := testDefinitionWith(t, "protocols: [otlp]\nwrite_sizes: [10]\ncharsets: [legacy, utf8]\n")
	e, err := newExplorer(1, def, exclusions(model.LegacyValidation))
	require.NoError(t, err)
	// OTLP writes the int counters of each charset with schema 0 and the gaps layout.
	_, series := e.pick(newRand(1, 0).IntN)
	require.Len(t, series, 2)
}

func TestExplorer_ProtocolThatCantWriteAnySeries(t *testing.T) {
	def := testDefinitionWith(t, "protocols: [v1, otlp]\nwrite_sizes: [1]\ncharsets: [utf8]\n")
	def.NativeHistograms.Kinds = []Kind{KindGauge}
	_, err := newExplorer(1, def, exclusions(model.LegacyValidation))
	require.EqualError(t, err, "protocol v1 can't write any combination of dimension values")
}

func TestAllCombinations(t *testing.T) {
	dims := []dimension{{name: "a", values: []string{"1", "2"}}, {name: "b", values: []string{"x", "y", "z"}}}
	require.Equal(t, [][]string{{"1", "x"}, {"1", "y"}, {"1", "z"}, {"2", "x"}, {"2", "y"}, {"2", "z"}}, allCombinations(dims))
}

func testDefaultExplorer(t *testing.T, scheme model.ValidationScheme) *explorer {
	def, err := LoadDefinition("")
	require.NoError(t, err)
	e, err := newExplorer(1, def, exclusions(scheme))
	require.NoError(t, err)
	return e
}

// testDefinitionWith parses a definition made of the given protocols, write sizes and charsets,
// series without unit and with an ASCII help, and int and float counters with schema 0 and the
// gaps layout.
func testDefinitionWith(t *testing.T, head string) Definition {
	def, err := ParseDefinition([]byte(head + `
metadata: {units: [none], helps: [ascii]}
native_histograms: {sample_types: [int, float], kinds: [counter], schemas: [0], layouts: [gaps]}
`))
	require.NoError(t, err)
	return def
}
