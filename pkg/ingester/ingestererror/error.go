package ingestererror

import (
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

const (
	integerUnavailableMsgFormat = "ingester is unavailable (current state: %s)"
)

// safeToWrap is an interface annotating errors that are safe to wrap.
type safeToWrap interface {
	safeToWrap()
}

// safeToWrapError is an error implementing the safeToWrap interface.
type safeToWrapError string

func (s safeToWrapError) safeToWrap() {}

func (s safeToWrapError) Error() string {
	return string(s)
}

func NewSafeToWrapError(format string, args ...any) error {
	return safeToWrapError(
		fmt.Sprintf(format, args...),
	)
}

// WrapOrAnnotateWithUser prepends the given userID to the given error.
// If the error is safe, the returned error retains a reference to the former.
func WrapOrAnnotateWithUser(err error, userID string) error {
	// If this is a safe error, we wrap it with userID and return it, because
	// it might contain extra information for gRPC and our logging middleware.
	var safe safeToWrap
	if errors.As(err, &safe) {
		return fmt.Errorf("user=%s: %w", userID, err)
	}
	// Otherwise, we just annotate it with userID and return it.
	return fmt.Errorf("user=%s: %s", userID, err)
}

// Unavailable is a safeToWrap error indicating that the ingester is unavailable.
type Unavailable struct {
	safeToWrapError
}

func NewUnavailableError(state string) Unavailable {
	return Unavailable{
		safeToWrapError(
			fmt.Sprintf(integerUnavailableMsgFormat, state),
		),
	}
}

// InstanceLimitReached is a safeToWrap error indicating that an instance limit has been reached.
type InstanceLimitReached struct {
	safeToWrapError
}

func NewInstanceLimitReachedError(message string) InstanceLimitReached {
	return InstanceLimitReached{
		safeToWrapError(
			message,
		),
	}
}

// TSDBUnavailable is a safeToWrap error indicating that the TSDB is unavailable.
type TSDBUnavailable struct {
	safeToWrapError
}

func NewTSDBUnavailableError(err error) TSDBUnavailable {
	return TSDBUnavailable{
		safeToWrapError(
			err.Error(),
		),
	}
}

// BadData is a safeToWrap error indicating a problem with a sample or an exemplar.
type BadData struct {
	safeToWrapError
}

func newSampleError(errID globalerror.ID, errMsg string, timestamp model.Time, labels []mimirpb.LabelAdapter) BadData {
	return BadData{
		safeToWrapError(
			fmt.Sprintf("%v. The affected sample has timestamp %s and is from series %s",
				errID.Message(errMsg),
				timestamp.Time().UTC().Format(time.RFC3339Nano),
				mimirpb.FromLabelAdaptersToLabels(labels).String(),
			),
		),
	}
}

func newExemplarError(errID globalerror.ID, errMsg string, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) BadData {
	return BadData{
		safeToWrapError(
			fmt.Sprintf("%v. The affected exemplar is %s with timestamp %s for series %s",
				errID.Message(errMsg),
				mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
				timestamp.Time().UTC().Format(time.RFC3339Nano),
				mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
			),
		),
	}
}

func NewSampleTimestampTooOldError(timestamp model.Time, labels []mimirpb.LabelAdapter) BadData {
	return newSampleError(
		globalerror.SampleTimestampTooOld,
		"the sample has been rejected because its timestamp is too old",
		timestamp,
		labels,
	)
}

func NewSampleTimestampTooOldOOOEnabledError(timestamp model.Time, labels []mimirpb.LabelAdapter, oooTimeWindow time.Duration) BadData {
	return newSampleError(
		globalerror.SampleTimestampTooOld,
		fmt.Sprintf("the sample has been rejected because another sample with a more recent timestamp has already been ingested and this sample is beyond the out-of-order time window of %s",
			model.Duration(oooTimeWindow).String(),
		),
		timestamp,
		labels,
	)
}

func NewSampleTimestampTooFarInFutureError(timestamp model.Time, labels []mimirpb.LabelAdapter) BadData {
	return newSampleError(
		globalerror.SampleTooFarInFuture,
		"received a sample whose timestamp is too far in the future",
		timestamp,
		labels,
	)
}

func NewSampleOutOfOrderError(timestamp model.Time, labels []mimirpb.LabelAdapter) BadData {
	return newSampleError(
		globalerror.SampleOutOfOrder,
		"the sample has been rejected because another sample with a more recent timestamp has already been ingested and out-of-order samples are not allowed",
		timestamp,
		labels,
	)
}

func NewSampleDuplicateTimestampError(timestamp model.Time, labels []mimirpb.LabelAdapter) BadData {
	return newSampleError(
		globalerror.SampleDuplicateTimestamp,
		"the sample has been rejected because another sample with the same timestamp, but a different value, has already been ingested",
		timestamp,
		labels,
	)
}

func NewExemplarMissingSeriesError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) BadData {
	return newExemplarError(
		globalerror.ExemplarSeriesMissing,
		"the exemplar has been rejected because the related series has not been ingested yet",
		timestamp,
		seriesLabels,
		exemplarLabels,
	)
}

func NewExemplarTimestampTooFarInFutureError(timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) BadData {
	return newExemplarError(
		globalerror.ExemplarTooFarInFuture,
		"received an exemplar whose timestamp is too far in the future",
		timestamp,
		seriesLabels,
		exemplarLabels,
	)
}

func NewTSDBExemplarOtherErr(ingestErr error, timestamp model.Time, seriesLabels, exemplarLabels []mimirpb.LabelAdapter) error {
	if ingestErr == nil {
		return nil
	}

	return BadData{
		safeToWrapError(
			fmt.Sprintf("err: %v. timestamp=%s, series=%s, exemplar=%s",
				ingestErr,
				timestamp.Time().UTC().Format(time.RFC3339Nano),
				mimirpb.FromLabelAdaptersToLabels(seriesLabels).String(),
				mimirpb.FromLabelAdaptersToLabels(exemplarLabels).String(),
			),
		),
	}
}
