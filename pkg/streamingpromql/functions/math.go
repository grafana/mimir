// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"
)

var Abs = FloatTransformationDropHistogramsFunc(math.Abs)
var Acos = FloatTransformationDropHistogramsFunc(math.Acos)
var Acosh = FloatTransformationDropHistogramsFunc(math.Acosh)
var Asin = FloatTransformationDropHistogramsFunc(math.Asin)
var Asinh = FloatTransformationDropHistogramsFunc(math.Asinh)
var Atan = FloatTransformationDropHistogramsFunc(math.Atan)
var Atanh = FloatTransformationDropHistogramsFunc(math.Atanh)
var Ceil = FloatTransformationDropHistogramsFunc(math.Ceil)
var Cos = FloatTransformationDropHistogramsFunc(math.Cos)
var Cosh = FloatTransformationDropHistogramsFunc(math.Cosh)
var Exp = FloatTransformationDropHistogramsFunc(math.Exp)
var Floor = FloatTransformationDropHistogramsFunc(math.Floor)
var Ln = FloatTransformationDropHistogramsFunc(math.Log)
var Log10 = FloatTransformationDropHistogramsFunc(math.Log10)
var Log2 = FloatTransformationDropHistogramsFunc(math.Log2)
var Sin = FloatTransformationDropHistogramsFunc(math.Sin)
var Sinh = FloatTransformationDropHistogramsFunc(math.Sinh)
var Sqrt = FloatTransformationDropHistogramsFunc(math.Sqrt)
var Tan = FloatTransformationDropHistogramsFunc(math.Tan)
var Tanh = FloatTransformationDropHistogramsFunc(math.Tanh)

var Deg = FloatTransformationDropHistogramsFunc(func(f float64) float64 {
	return f * 180 / math.Pi
})

var Rad = FloatTransformationDropHistogramsFunc(func(f float64) float64 {
	return f * math.Pi / 180
})

var Sgn = FloatTransformationDropHistogramsFunc(func(f float64) float64 {
	if f < 0 {
		return -1
	}

	if f > 0 {
		return 1
	}

	// This behaviour is undocumented, but if f is +/- NaN, Prometheus' engine returns that value.
	// Otherwise, if the value is 0, we should return 0.
	return f
})
