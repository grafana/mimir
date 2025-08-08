// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
)

func TestSymbolsTable(t *testing.T) {
	// Ensure that the Mimir and Prometheus implementations share the same properties.
	promImplBuilder := func() StringSymbolizer {
		st := writev2.NewSymbolTable()
		return &st
	}
	fastImplBuilder := func() StringSymbolizer {
		st := NewFastSymbolsTable(minPreallocatedSymbolsPerRequest)
		return st
	}
	impls := [](func() StringSymbolizer){promImplBuilder, fastImplBuilder}

	for i, impl := range impls {
		t.Run(fmt.Sprintf("impl %d", i), func(t *testing.T) {
			s := impl()

			require.Equal(t, []string{""}, s.Symbols(), "required empty reference does not exist")
			require.Equal(t, uint32(0), s.Symbolize(""))
			require.Equal(t, []string{""}, s.Symbols())

			require.Equal(t, uint32(1), s.Symbolize("abc"))
			require.Equal(t, []string{"", "abc"}, s.Symbols())

			require.Equal(t, uint32(2), s.Symbolize("__name__"))
			require.Equal(t, []string{"", "abc", "__name__"}, s.Symbols())

			require.Equal(t, uint32(3), s.Symbolize("foo"))
			require.Equal(t, []string{"", "abc", "__name__", "foo"}, s.Symbols())

			s.Reset()
			require.Equal(t, []string{""}, s.Symbols(), "required empty reference does not exist")
			require.Equal(t, uint32(0), s.Symbolize(""))

			require.Equal(t, uint32(1), s.Symbolize("__name__"))
			require.Equal(t, []string{"", "__name__"}, s.Symbols())

			require.Equal(t, uint32(2), s.Symbolize("abc"))
			require.Equal(t, []string{"", "__name__", "abc"}, s.Symbols())

			ls := labels.FromStrings("__name__", "qwer", "zxcv", "1234")

			encoded := make([]uint32, ls.Len()*2)
			i := 0
			ls.Range(func(l labels.Label) {
				encoded[(i * 2)] = s.Symbolize(l.Name)
				encoded[(i*2)+1] = s.Symbolize(l.Value)
				i++
			})
			require.Equal(t, []uint32{1, 3, 4, 5}, encoded)
			decoded, _ := desymbolizeLabelsDirect(encoded, s.Symbols())
			require.Equal(t, ls, FromLabelAdaptersToLabels(decoded))
		})
	}

	// Tests specific to the Mimir implementation, which is a superset of the Prometheus one.
	t.Run("symbols offset, no common symbols", func(t *testing.T) {
		s := NewFastSymbolsTable(minPreallocatedSymbolsPerRequest)
		s.ConfigureCommonSymbols(128, nil)

		require.Equal(t, []string{""}, s.Symbols(), "required empty reference does not exist")
		require.Equal(t, uint32(0), s.Symbolize(""))
		require.Equal(t, []string{""}, s.Symbols())

		require.Equal(t, uint32(129), s.Symbolize("abc"))
		require.Equal(t, []string{"", "abc"}, s.Symbols())

		require.Equal(t, uint32(130), s.Symbolize("__name__"))
		require.Equal(t, []string{"", "abc", "__name__"}, s.Symbols())

		s.Reset()
		require.Equal(t, uint32(0), s.offset)
	})

	t.Run("common symbols", func(t *testing.T) {
		commonSymbols := NewCommonSymbols([]string{"", "__name__", "__aggregation__"})
		s := NewFastSymbolsTable(minPreallocatedSymbolsPerRequest)
		s.ConfigureCommonSymbols(8, commonSymbols)

		require.Equal(t, []string{""}, s.Symbols(), "required empty reference does not exist")
		require.Equal(t, uint32(0), s.Symbolize(""))
		require.Equal(t, []string{""}, s.Symbols())

		require.Equal(t, uint32(9), s.Symbolize("abc"))
		require.Equal(t, []string{"", "abc"}, s.Symbols())

		require.Equal(t, uint32(1), s.Symbolize("__name__"))
		require.Equal(t, []string{"", "abc"}, s.Symbols())

		require.Equal(t, uint32(10), s.Symbolize("def"))
		require.Equal(t, []string{"", "abc", "def"}, s.Symbols())

		require.Equal(t, uint32(2), s.Symbolize("__aggregation__"))
		require.Equal(t, []string{"", "abc", "def"}, s.Symbols())

		s.Reset()
		require.Nil(t, s.commonSymbols)
	})

	t.Run("capacity estimation", func(t *testing.T) {
		s := NewFastSymbolsTable(50)
		require.Equal(t, 50, s.CapLowerBound())

		s.Symbolize("abc")
		s.Symbolize("def")
		s.Symbolize("ghi")
		require.Equal(t, 50, s.CapLowerBound())

		s.Reset()
		require.Equal(t, 50, s.CapLowerBound())

		for i := range 50 {
			s.Symbolize(fmt.Sprintf("%d", i))
		}
		require.Equal(t, 50, s.CapLowerBound())

		s.Reset()
		require.Equal(t, 50, s.CapLowerBound())

		s.Symbolize("abc")
		s.Symbolize("def")
		s.Symbolize("ghi")
		require.Equal(t, 50, s.CapLowerBound())
	})

	t.Run("symbols size proto", func(t *testing.T) {
		s := NewFastSymbolsTable(0)

		wr := &WriteRequest{}
		require.Equal(t, wr.SymbolsRW2Size(), s.SymbolsSizeProto())

		s.Symbolize("abc")
		s.Symbolize("def")
		s.Symbolize("test some other longer one")

		wr = &WriteRequest{SymbolsRW2: []string{"abc", "def", "test some other longer one"}}
		require.Equal(t, wr.SymbolsRW2Size(), s.SymbolsSizeProto())
	})
}

func desymbolizeLabelsDirect(labelRefs []uint32, symbols []string) ([]LabelAdapter, string) {
	name := ""
	las := make([]LabelAdapter, len(labelRefs)/2)
	for i := 0; i < len(labelRefs); i += 2 {
		las[i/2].Name = symbols[labelRefs[i]]
		las[i/2].Value = symbols[labelRefs[i+1]]
		if las[i/2].Name == labels.MetricName {
			name = las[i/2].Value
		}
	}
	return las, name
}

func BenchmarkSymbolizer(b *testing.B) {
	b.Run("2k labels unique values, common symbols", func(b *testing.B) {
		lbls := make([]string, 2*2000)
		for i := range 2000 {
			lbls[i*2] = "__name__"
			lbls[(i*2)+1] = fmt.Sprintf("series_%d", i)
		}
		commonSymbols := NewCommonSymbols(benchmarkCommonSymbols)

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			st := symbolsTableFromPool()
			st.ConfigureCommonSymbols(64, commonSymbols)

			for _, l := range lbls {
				_ = st.Symbolize(l)
			}

			symbols := symbolsSliceFromPool()
			symbols = st.SymbolsPrealloc(symbols)
			if len(symbols) != 2001 {
				b.Fatalf("unexpected number of symbols: %d", len(symbols))
			}
			reuseSymbolsSlice(symbols)
			reuseSymbolsTable(st)
		}
	})

	b.Run("7 series with some shared label keys, common symbols", func(b *testing.B) {
		numSeries := 7
		generatedLabels := 15
		series := [][]string{}
		gen := rand.New(rand.NewSource(789456123))
		for i := range numSeries {
			s := []string{"__name__", fmt.Sprintf("series_%d", i), "namespace", "my-namespace", "job", fmt.Sprintf("namespace/job-%d", i)}
			for i := range generatedLabels {
				s = append(s, fmt.Sprintf("generated_label_%d", i), fmt.Sprintf("%d", gen.Uint64()))
			}
			series = append(series, s)
		}
		commonSymbols := NewCommonSymbols(benchmarkCommonSymbols)

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			st := symbolsTableFromPool()
			st.ConfigureCommonSymbols(64, commonSymbols)

			for _, s := range series {
				for _, l := range s {
					_ = st.Symbolize(l)
				}
			}

			symbols := symbolsSliceFromPool()
			symbols = st.SymbolsPrealloc(symbols)
			if len(symbols) != 136 {
				b.Fatalf("unexpected number of symbols: %d", len(symbols))
			}
			reuseSymbolsSlice(symbols)
			reuseSymbolsTable(st)
		}
	})
}

var (
	benchmarkCommonSymbols = []string{
		// RW2.0 Spec: The first element of the symbols table MUST be an empty string.
		// This ensures that empty/missing refs still map to empty string.
		"",
		// Prometheus/Mimir symbols
		"__name__",
		"__aggregation__",
		"<aggregated>",
		"le",
		"component",
		"cortex_request_duration_seconds_bucket",
		"storage_operation_duration_seconds_bucket",
		// Grafana Labs products
		"grafana",
		"asserts_env",
		"asserts_request_context",
		"asserts_source",
		"asserts_entity_type",
		"asserts_request_type",
		// General symbols
		"name",
		"image",
		// Kubernetes symbols
		"cluster",
		"namespace",
		"pod",
		"job",
		"instance",
		"container",
		"replicaset",
		// Networking, HTTP
		"interface",
		"status_code",
		"resource",
		"operation",
		"method",
		// Common tools
		"kube-system",
		"kube-system/cadvisor",
		"node-exporter",
		"node-exporter/node-exporter",
		"kube-system/kubelet",
		"kube-system/node-local-dns",
		"kube-state-metrics/kube-state-metrics",
		"default/kubernetes",
	}
)
