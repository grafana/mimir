// SPDX-License-Identifier: AGPL-3.0-only

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
	// commonCallback is called irrespective of soft or hard error.
	commonCallback                 func()
	errOutOfBounds                 func(int64, []mimirpb.LabelAdapter)
	errOutOfOrderSample            func(int64, []mimirpb.LabelAdapter)
	errTooOldSample                func(int64, []mimirpb.LabelAdapter)
	sampleTooFarInFuture           func(int64, []mimirpb.LabelAdapter)
	errDuplicateSampleForTimestamp func(string, int64, []mimirpb.LabelAdapter)
	maxSeriesPerUser               func(labels []mimirpb.LabelAdapter)
	maxSeriesPerMetric func(labels []mimirpb.LabelAdapter)
	// Native histogram errors.
	errHistogram       func(error, int64, []mimirpb.LabelAdapter) bool
	errLabelsNotSorted func([]mimirpb.LabelAdapter)
}

func NewSoftAppendErrorProcessor(
	commonCallback func(),
	errOutOfBounds func(int64, []mimirpb.LabelAdapter),
	errOutOfOrderSample func(int64, []mimirpb.LabelAdapter),
	errTooOldSample func(int64, []mimirpb.LabelAdapter),
	sampleTooFarInFuture func(int64, []mimirpb.LabelAdapter),
	errDuplicateSampleForTimestamp func(string, int64, []mimirpb.LabelAdapter),
	maxSeriesPerUser func([]mimirpb.LabelAdapter),
	maxSeriesPerMetric func(labels []mimirpb.LabelAdapter),
	errHistogram func(error, int64, []mimirpb.LabelAdapter) bool,
	errLabelsNotSorted func([]mimirpb.LabelAdapter),
) SoftAppendErrorProcessor {
	return SoftAppendErrorProcessor{
		commonCallback:                 commonCallback,
		errOutOfBounds:                 errOutOfBounds,
		errOutOfOrderSample:            errOutOfOrderSample,
		errTooOldSample:                errTooOldSample,
		sampleTooFarInFuture:           sampleTooFarInFuture,
		errDuplicateSampleForTimestamp: errDuplicateSampleForTimestamp,
		maxSeriesPerUser:               maxSeriesPerUser,
		maxSeriesPerMetric:             maxSeriesPerMetric,
		errHistogram:                   errHistogram,
		errLabelsNotSorted:             errLabelsNotSorted,
	}
}

// ProcessErr returns true if the err is a soft append error and calls appropriate callback function.
// In case a soft error is encountered, we can continue ingesting other samples without aborting
// the whole request. ProcessErr always calls the commonCallback() if it exists.
// err must be non-nil.
func (e *SoftAppendErrorProcessor) ProcessErr(err error, ts int64, labels []mimirpb.LabelAdapter) bool {
	e.commonCallback()
	var histErr histogram.Error
	switch {
	case errors.Is(err, storage.ErrOutOfBounds):
		e.errOutOfBounds(ts, labels)
		return true
	case errors.Is(err, storage.ErrOutOfOrderSample):
		e.errOutOfOrderSample(ts, labels)
		return true
	case errors.Is(err, storage.ErrTooOldSample):
		e.errTooOldSample(ts, labels)
		return true
	case errors.Is(err, globalerror.SampleTooFarInFuture):
		e.sampleTooFarInFuture(ts, labels)
		return true
	case errors.Is(err, storage.ErrDuplicateSampleForTimestamp):
		e.errDuplicateSampleForTimestamp(err.Error(), ts, labels)
		return true
	case errors.Is(err, globalerror.MaxSeriesPerUser):
		e.maxSeriesPerUser(labels)
		return true
	case errors.Is(err, globalerror.MaxSeriesPerMetric):
		e.maxSeriesPerMetric(labels)
		return true
	// Map native histogram validation errors to soft errors.
	case errors.As(err, &histErr):
		return e.errHistogram(err, ts, labels)
	case errors.Is(err, globalerror.SeriesLabelsNotSorted):
		e.errLabelsNotSorted(labels)
		return true
	}
	return false
}
