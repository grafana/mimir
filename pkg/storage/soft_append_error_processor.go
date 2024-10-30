package storage

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

// SoftAppendErrorProcessor helps identify soft errors and run appropriate callbacks.
// This also helps keep the soft error checks consistent between ingesters and block builders
// by keeping it all in a single place.
type SoftAppendErrorProcessor struct {
	// CommonCallback is called irrespective of soft or hard error.
	CommonCallback                   func()
	ErrOutOfBounds                   func(ts int64, labels []mimirpb.LabelAdapter)
	ErrOutOfOrderSample              func(ts int64, labels []mimirpb.LabelAdapter)
	ErrTooOldSample                  func(ts int64, labels []mimirpb.LabelAdapter)
	SampleTooFarInFuture             func(ts int64, labels []mimirpb.LabelAdapter)
	ErrDuplicateSampleForTimestamp   func(ts int64, labels []mimirpb.LabelAdapter)
	MaxSeriesPerUser                 func()
	MaxSeriesPerMetric               func(labels []mimirpb.LabelAdapter)
	ErrOOONativeHistogramsDisabled   func(ts int64, labels []mimirpb.LabelAdapter)
	ErrHistogramCountMismatch        func(ts int64, labels []mimirpb.LabelAdapter)
	ErrHistogramCountNotBigEnough    func(ts int64, labels []mimirpb.LabelAdapter)
	ErrHistogramNegativeBucketCount  func(ts int64, labels []mimirpb.LabelAdapter)
	ErrHistogramSpanNegativeOffset   func(ts int64, labels []mimirpb.LabelAdapter)
	ErrHistogramSpansBucketsMismatch func(ts int64, labels []mimirpb.LabelAdapter)
}

// ProcessErr returns true if the err is a soft append error and calls appropriate callback function.
// In case a soft error is encountered, we can continue ingesting other samples without aborting
// the whole request. ProcessErr always calls the CommonCallback() if it exists.
// err must be non-nil.
func (e *SoftAppendErrorProcessor) ProcessErr(err error, ts int64, labels []mimirpb.LabelAdapter) bool {
	e.CommonCallback()
	switch {
	case errors.Is(err, storage.ErrOutOfBounds):
		e.ErrOutOfBounds(ts, labels)
		return true
	case errors.Is(err, storage.ErrOutOfOrderSample):
		e.ErrOutOfOrderSample(ts, labels)
		return true
	case errors.Is(err, storage.ErrTooOldSample):
		e.ErrTooOldSample(ts, labels)
		return true
	case errors.Is(err, globalerror.SampleTooFarInFuture):
		e.SampleTooFarInFuture(ts, labels)
		return true
	case errors.Is(err, storage.ErrDuplicateSampleForTimestamp):
		e.ErrDuplicateSampleForTimestamp(ts, labels)
		return true
	case errors.Is(err, globalerror.MaxSeriesPerUser):
		e.MaxSeriesPerUser()
		return true
	case errors.Is(err, globalerror.MaxSeriesPerMetric):
		e.MaxSeriesPerMetric(labels)
		return true

	// Map TSDB native histogram validation errors to soft errors.
	case errors.Is(err, storage.ErrOOONativeHistogramsDisabled):
		e.ErrOOONativeHistogramsDisabled(ts, labels)
		return true
	case errors.Is(err, histogram.ErrHistogramCountMismatch):
		e.ErrHistogramCountMismatch(ts, labels)
		return true
	case errors.Is(err, histogram.ErrHistogramCountNotBigEnough):
		e.ErrHistogramCountNotBigEnough(ts, labels)
		return true
	case errors.Is(err, histogram.ErrHistogramNegativeBucketCount):
		e.ErrHistogramNegativeBucketCount(ts, labels)
		return true
	case errors.Is(err, histogram.ErrHistogramSpanNegativeOffset):
		e.ErrHistogramSpanNegativeOffset(ts, labels)
		return true
	case errors.Is(err, histogram.ErrHistogramSpansBucketsMismatch):
		e.ErrHistogramSpansBucketsMismatch(ts, labels)
		return true
	}
	return false
}
