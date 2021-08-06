// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package encoding

import (
	"math"

	"github.com/prometheus/common/model"
)

type deltaBytes byte

const (
	d0 deltaBytes = 0
	d1 deltaBytes = 1
	d2 deltaBytes = 2
	d4 deltaBytes = 4
	d8 deltaBytes = 8
)

func bytesNeededForUnsignedTimestampDelta(deltaT model.Time) deltaBytes {
	switch {
	case deltaT > math.MaxUint32:
		return d8
	case deltaT > math.MaxUint16:
		return d4
	case deltaT > math.MaxUint8:
		return d2
	default:
		return d1
	}
}

func bytesNeededForSignedTimestampDelta(deltaT model.Time) deltaBytes {
	switch {
	case deltaT > math.MaxInt32 || deltaT < math.MinInt32:
		return d8
	case deltaT > math.MaxInt16 || deltaT < math.MinInt16:
		return d4
	case deltaT > math.MaxInt8 || deltaT < math.MinInt8:
		return d2
	default:
		return d1
	}
}

func bytesNeededForIntegerSampleValueDelta(deltaV model.SampleValue) deltaBytes {
	switch {
	case deltaV < math.MinInt32 || deltaV > math.MaxInt32:
		return d8
	case deltaV < math.MinInt16 || deltaV > math.MaxInt16:
		return d4
	case deltaV < math.MinInt8 || deltaV > math.MaxInt8:
		return d2
	case deltaV != 0:
		return d1
	default:
		return d0
	}
}

func max(a, b deltaBytes) deltaBytes {
	if a > b {
		return a
	}
	return b
}

// isInt64 returns true if v can be represented as an int64.
func isInt64(v model.SampleValue) bool {
	// Note: Using math.Modf is slower than the conversion approach below.
	return model.SampleValue(int64(v)) == v
}
