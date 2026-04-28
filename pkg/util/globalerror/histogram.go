// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"errors"

	"github.com/prometheus/prometheus/model/histogram"
)

func MapNativeHistogramErr(err error) (ID, bool) {
	switch {
	case errors.Is(err, histogram.ErrHistogramCountMismatch):
		return NativeHistogramCountMismatch, true
	case errors.Is(err, histogram.ErrHistogramCountNotBigEnough):
		return NativeHistogramCountNotBigEnough, true
	case errors.Is(err, histogram.ErrHistogramNegativeBucketCount):
		return NativeHistogramNegativeBucketCount, true
	case errors.Is(err, histogram.ErrHistogramSpanNegativeOffset):
		return NativeHistogramSpanNegativeOffset, true
	case errors.Is(err, histogram.ErrHistogramSpansBucketsMismatch):
		return NativeHistogramSpansBucketsMismatch, true
	case errors.Is(err, histogram.ErrHistogramCustomBucketsMismatch):
		return NativeHistogramCustomBucketsMismatch, true
	case errors.Is(err, histogram.ErrHistogramCustomBucketsInvalid):
		return NativeHistogramCustomBucketsInvalid, true
	case errors.Is(err, histogram.ErrHistogramCustomBucketsInfinite):
		return NativeHistogramCustomBucketsInfinite, true
	default:
		return "", false
	}
}
