package mimirpb

import (
	"fmt"
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
		st := NewFastSymbolsTable(baseSymbolsMapCapacity)
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
			encoded := make([]uint32, len(ls)*2)
			for i, l := range ls {
				encoded[(i * 2)] = s.Symbolize(l.Name)
				encoded[(i*2)+1] = s.Symbolize(l.Value)
			}
			require.Equal(t, []uint32{1, 3, 4, 5}, encoded)
			decoded, _ := desymbolizeLabelsDirect(encoded, s.Symbols())
			require.Equal(t, ls, FromLabelAdaptersToLabels(decoded))
		})
	}

	// Tests specific to the Mimir implementation, which is a superset of the Prometheus one.
	t.Run("symbols offset, no common symbols", func(t *testing.T) {
		s := NewFastSymbolsTable(baseSymbolsMapCapacity)
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
		commonSymbols := []string{"", "__name__", "__aggregation__"}
		s := NewFastSymbolsTable(baseSymbolsMapCapacity)
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
	b.Run("prom symbolizer: 10k labels unique values", func(b *testing.B) {
		lbls := make([]string, 2*10000)
		for i := range 10000 {
			lbls[i*2] = "__name__"
			lbls[(i*2)+1] = fmt.Sprintf("series_%d", i)
		}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			st := writev2.NewSymbolTable()

			for _, l := range lbls {
				_ = st.Symbolize(l)
			}
			symbols := st.Symbols()
			if len(symbols) != 10002 {
				b.Fatalf("unexpected number of symbols: %d", len(symbols))
			}
		}
	})

	b.Run("mimir symbolizer: 10k labels unique values", func(b *testing.B) {
		lbls := make([]string, 2*10000)
		for i := range 10000 {
			lbls[i*2] = "__name__"
			lbls[(i*2)+1] = fmt.Sprintf("series_%d", i)
		}

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			// st := NewFastSymbolsTable(baseSymbolsMapCapacity)
			st := symbolsTableFromPool()

			for _, l := range lbls {
				_ = st.Symbolize(l)
			}
			symbols := st.Symbols()
			if len(symbols) != 10002 {
				b.Fatalf("unexpected number of symbols: %d", len(symbols))
			}
			reuseSymbolsTable(st)
		}
	})
}
