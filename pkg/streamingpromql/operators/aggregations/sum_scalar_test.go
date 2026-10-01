// SPDX-License-Identifier: AGPL-3.0-only

package aggregations

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
)

func TestAccumulateFloat64sScalar(t *testing.T) {
	testAccumulateFloat64s(t, accumulateFloat64sScalar, 2)
}

func testAccumulateFloat64s(t *testing.T, accumulate func([]float64, []float64, []float64), offset int, lengths ...int) {
	if len(lengths) == 0 {
		lengths = []int{0, 1, 2, 3, 7, 8, 9, 31, 32, 33}
	}
	inf := math.Inf(1)
	negativeZero := math.Copysign(0, -1)
	testCases := []struct {
		name         string
		sum          float64
		compensation float64
		terms        []float64
	}{
		{name: "ordinary", terms: []float64{3.14159, 2.71828, 1, -3}},
		{name: "cancellation", terms: []float64{1e16, 1, 1, -1e16}},
		{name: "larger_input", terms: []float64{1, 1e100, 1, -1e100}},
		{name: "zeros", terms: []float64{0, negativeZero, negativeZero, 0}},
		{name: "subnormals", terms: []float64{math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64, 2 * math.SmallestNonzeroFloat64}},
		{name: "positive_infinity", terms: []float64{1e16, 1, inf, 2, inf}},
		{name: "negative_infinity", terms: []float64{-1e16, 1, -inf, 2, -inf}},
		{name: "opposite_infinities", terms: []float64{inf, -inf, 1}},
		{name: "positive_overflow", terms: []float64{math.MaxFloat64, math.MaxFloat64, -math.MaxFloat64}},
		{name: "negative_overflow", terms: []float64{-math.MaxFloat64, -math.MaxFloat64, math.MaxFloat64}},
		{name: "nan", terms: []float64{math.NaN(), 1, inf}},
		{name: "signaling_nan", terms: []float64{math.Float64frombits(0x7ff0000000000001), 1}},
		{name: "seeded_negative_zero", sum: negativeZero, compensation: negativeZero, terms: []float64{negativeZero, 0}},
		{name: "seeded_compensation", sum: 1e16, compensation: 1, terms: []float64{1, -1e16}},
		{name: "seeded_infinite_sum", sum: inf, compensation: math.NaN(), terms: []float64{1, inf}},
		{name: "seeded_infinite_compensation", sum: 1, compensation: inf, terms: []float64{-1, inf}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, steps := range lengths {
				t.Run(fmt.Sprintf("steps=%d", steps), func(t *testing.T) {
					const guard = -123.5
					sumStorage := slices.Repeat([]float64{guard}, steps+offset+2)
					compensationStorage := slices.Clone(sumStorage)
					sums := sumStorage[offset : offset+steps]
					compensations := compensationStorage[offset : offset+steps]
					for i := range steps {
						sums[i], compensations[i] = tc.sum, tc.compensation
					}
					wantSums, wantCompensations := slices.Clone(sums), slices.Clone(compensations)

					accumulate(nil, sums, compensations)
					assertFloat64State(t, wantSums, sums)
					assertFloat64State(t, wantCompensations, compensations)

					valueStorage := slices.Repeat([]float64{guard}, steps+offset+2)
					values := valueStorage[offset : offset+steps]
					for series := range tc.terms {
						for step := range steps {
							// Adjacent steps exercise different magnitude and non-finite branches.
							values[step] = tc.terms[(series+step)%len(tc.terms)]
							wantSums[step], wantCompensations[step] = floats.KahanSumInc(values[step], wantSums[step], wantCompensations[step])
						}
						before := slices.Clone(values)
						accumulate(values, sums, compensations)
						assertFloat64State(t, wantSums, sums)
						assertFloat64State(t, wantCompensations, compensations)
						for i := range values {
							require.Equal(t, math.Float64bits(before[i]), math.Float64bits(values[i]), "input at step %d", i)
						}
					}

					for _, storage := range [][]float64{valueStorage, sumStorage, compensationStorage} {
						require.Equal(t, slices.Repeat([]float64{guard}, offset), storage[:offset])
						require.Equal(t, []float64{guard, guard}, storage[offset+steps:])
					}
				})
			}
		})
	}
}

func TestAccumulateFloat64sScalarRandom(t *testing.T) {
	testAccumulateFloat64sRandom(t, accumulateFloat64sScalar)
}

func testAccumulateFloat64sRandom(t *testing.T, accumulate func([]float64, []float64, []float64)) {
	rng := rand.New(rand.NewPCG(0, 1))
	const steps = 129
	sums, compensations := make([]float64, steps), make([]float64, steps)
	for step := range steps {
		sums[step] = rng.Float64() - 0.5
		compensations[step] = rng.Float64() - 0.5
	}
	wantSums, wantCompensations := slices.Clone(sums), slices.Clone(compensations)
	values := make([]float64, steps)
	// Keep every update finite so NaNs cannot hide later state mismatches.
	for range 256 {
		for step := range steps {
			values[step] = math.Ldexp(rng.Float64()-0.5, rng.IntN(2001)-1000)
			wantSums[step], wantCompensations[step] = floats.KahanSumInc(values[step], wantSums[step], wantCompensations[step])
		}
		accumulate(values, sums, compensations)
		assertFloat64State(t, wantSums, sums)
		assertFloat64State(t, wantCompensations, compensations)
	}
}

func assertFloat64State(t testing.TB, want, got []float64) {
	t.Helper()
	require.Len(t, got, len(want))
	for i, expected := range want {
		if math.IsNaN(expected) {
			require.True(t, math.IsNaN(got[i]), "step %d: expected NaN, got %g", i, got[i])
		} else {
			require.Equal(t, math.Float64bits(expected), math.Float64bits(got[i]), "step %d: expected %g, got %g", i, expected, got[i])
		}
	}
}

func TestPackFloat64s(t *testing.T) {
	values := []float64{1, math.Copysign(0, -1), math.SmallestNonzeroFloat64, math.Inf(1), math.Inf(-1), math.NaN(), math.Float64frombits(0x7ff0000000000001)}
	for n := 0; n <= len(values); n++ {
		t.Run(fmt.Sprintf("points=%d", n), func(t *testing.T) {
			points := make([]promql.FPoint, n)
			for i := range points {
				points[i] = promql.FPoint{T: int64(i), F: values[i]}
			}
			before := slices.Clone(points)
			const guard = -123.5
			storage := slices.Repeat([]float64{guard}, n+2)
			packFloat64s(storage[1:], points)
			for i := range points {
				require.Equal(t, math.Float64bits(values[i]), math.Float64bits(storage[i+1]))
				require.Equal(t, before[i].T, points[i].T)
				require.Equal(t, math.Float64bits(before[i].F), math.Float64bits(points[i].F))
			}
			require.Equal(t, guard, storage[0])
			require.Equal(t, guard, storage[n+1])
		})
	}
}
