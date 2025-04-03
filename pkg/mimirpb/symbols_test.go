package mimirpb

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/stretchr/testify/require"
)

func TestSymbolsTable(t *testing.T) {
	promImplBuilder := func() StringSymbolizer {
		st := writev2.NewSymbolTable()
		return &st
	}
	fastImplBuilder := func() StringSymbolizer {
		st := NewFastSymbolsTable()
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
}

func BenchmarkSymbolizer(b *testing.B) {
	b.Run("10k labels unique values", func(b *testing.B) {
		lbls := make([]string, 2*10000)
		for i := range 10000 {
			lbls[i*2] = "__name__"
			lbls[(i*2)+1] = fmt.Sprintf("series_%d", i)
		}
		st := NewFastSymbolsTable()

		b.ResetTimer()

		for n := 0; n < b.N; n++ {
			for _, l := range lbls {
				_ = st.Symbolize(l)
			}
			symbols := st.Symbols()
			if len(symbols) != 10002 {
				b.Fatalf("unexpected number of symbols: %d", len(symbols))
			}
		}
	})
}
