package tsdb

import (
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
)

func TestIntersectTwoPostings(t *testing.T) {
	a := newListPostings(1, 2, 3)
	b := newListPostings(2, 3, 4)

	cases := []struct {
		a, b index.Postings

		res index.Postings
	}{
		{
			a:   a,
			b:   b,
			res: newListPostings(2, 3),
		},
		{
			a:   a,
			b:   index.EmptyPostings(),
			res: index.EmptyPostings(),
		},
		{
			a:   index.EmptyPostings(),
			b:   b,
			res: index.EmptyPostings(),
		},
		{
			a:   index.EmptyPostings(),
			b:   index.EmptyPostings(),
			res: index.EmptyPostings(),
		},
		{
			a:   newListPostings(1, 2, 3, 4, 5),
			b:   newListPostings(6, 7, 8, 9, 10),
			res: newListPostings(),
		},
		{
			a:   newListPostings(1, 2, 3, 4, 5),
			b:   newListPostings(4, 5, 6, 7, 8),
			res: newListPostings(4, 5),
		},
		{
			a:   newListPostings(1, 2, 3, 4, 9, 10),
			b:   newListPostings(1, 4, 5, 6, 7, 8, 10, 11),
			res: newListPostings(1, 4, 10),
		},
		{
			a:   newListPostings(1),
			b:   newListPostings(0, 1),
			res: newListPostings(1),
		},
		{
			a:   newListPostings(1),
			b:   newListPostings(),
			res: newListPostings(),
		},
		{
			a:   newListPostings(),
			b:   newListPostings(),
			res: newListPostings(),
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			if c.res == nil {
				t.Fatal("intersect result expectancy cannot be nil")
			}

			expected, err := index.ExpandPostings(c.res)
			require.NoError(t, err)

			i := index.Intersect(c.a, c.b)

			if c.res == index.EmptyPostings() {
				require.Equal(t, index.EmptyPostings(), i)
				return
			}

			if i == index.EmptyPostings() {
				t.Fatal("intersect unexpected result: EmptyPostings sentinel")
			}

			res, err := index.ExpandPostings(i)
			require.NoError(t, err)
			require.Equal(t, expected, res)
		})
	}
}

func BenchmarkIntersectTwoPostings(t *testing.B) {
	// intersect extracts the benchmarked function to easily replace it by index.Intersect
	intersect := func(a, b index.Postings) index.Postings {
		return IntersectTwoPostings(a, b)
	}

	t.Run("Few common postings", func(bench *testing.B) {
		var a, b []storage.SeriesRef

		for i := 0; i < 12500000; i++ {
			if i%2 == 0 || i%100 == 0 {
				a = append(a, storage.SeriesRef(i))
			}
			if i%2 == 1 || i%100 == 0 {
				b = append(b, storage.SeriesRef(i))
			}
		}

		i1 := newListPostings(a...)
		i2 := newListPostings(b...)

		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < bench.N; i++ {
			if _, err := drainPostings(intersect(i1, i2)); err != nil {
				bench.Fatal(err)
			}
		}
	})

	t.Run("All common postings", func(bench *testing.B) {
		var a []storage.SeriesRef
		for i := 0; i < 12500000; i++ {
			a = append(a, storage.SeriesRef(i))
		}

		i1 := newListPostings(a...)
		i2 := newListPostings(a...)

		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < bench.N; i++ {
			if _, err := drainPostings(intersect(i1, i2)); err != nil {
				bench.Fatal(err)
			}
		}
	})

	t.Run("LongPostings1", func(bench *testing.B) {
		var a, b []storage.SeriesRef

		for i := 0; i < 10000000; i += 2 {
			a = append(a, storage.SeriesRef(i))
		}
		for i := 5000000; i < 5000100; i += 4 {
			b = append(b, storage.SeriesRef(i))
		}
		for i := 5090000; i < 5090600; i += 4 {
			b = append(b, storage.SeriesRef(i))
		}

		i1 := newListPostings(a...)
		i2 := newListPostings(b...)

		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < bench.N; i++ {
			if _, err := drainPostings(intersect(i1, i2)); err != nil {
				bench.Fatal(err)
			}
		}
	})

	t.Run("LongPostings2", func(bench *testing.B) {
		var a, b []storage.SeriesRef

		for i := 0; i < 12500000; i++ {
			a = append(a, storage.SeriesRef(i))
		}
		for i := 7500000; i < 12500000; i++ {
			b = append(b, storage.SeriesRef(i))
		}

		i1 := newListPostings(a...)
		i2 := newListPostings(b...)

		bench.ResetTimer()
		bench.ReportAllocs()
		for i := 0; i < bench.N; i++ {
			if _, err := drainPostings(intersect(i1, i2)); err != nil {
				bench.Fatal(err)
			}
		}
	})
}

func newListPostings(list ...storage.SeriesRef) index.Postings {
	return index.NewListPostings(list)
}

// ExpandPostings returns the postings expanded as a slice.
func drainPostings(p index.Postings) (s storage.SeriesRef, err error) {
	for p.Next() {
		s += p.At()
	}
	return s, p.Err()
}
