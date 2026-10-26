// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// estimateErrorClass says what an estimate error means to the caller, so
// the HTTP handler can pick a status: a request Mimir can't answer as asked
// (400), a request refused by a limit (422), or a failure (500).
type estimateErrorClass int

const (
	estimateBadRequest estimateErrorClass = iota + 1
	estimateRefused
)

type estimateError struct {
	class estimateErrorClass
	err   error
}

func (e estimateError) Error() string { return e.err.Error() }
func (e estimateError) Unwrap() error { return e.err }

// errStepNeedsOneSource is wrapped by the refusals of a per-bucket count
// over sources that may share series, which the estimator answers bucket by
// bucket instead.
var errStepNeedsOneSource = errors.New("per-bucket counts need each bucket held by sources that share no series")

// errCountsNotSupported is wrapped by the refusal of a source that doesn't
// know the count RPCs, such as an older version during a rolling update. It
// is not a limit, so callers that read a refusal as a limit check for it.
var errCountsNotSupported = errors.New("does not support cardinality estimates")

// olderVersionError is an answer that shows its source runs an older
// version, such as one that ignored a field it doesn't know. It reads as
// errCountsNotSupported, so callers that take a refusal for a limit skip it.
type olderVersionError struct{ err error }

func (e olderVersionError) Error() string        { return e.err.Error() }
func (e olderVersionError) Unwrap() error        { return e.err }
func (e olderVersionError) Is(target error) bool { return target == errCountsNotSupported }

// olderVersionEstimate refuses an answer from a source that runs an older
// version with 422, like a source that doesn't know the count RPCs. It is not
// retried: it is found while the answer is merged, so a retry would count
// the groups merged so far twice.
func olderVersionEstimate(format string, args ...any) error {
	return estimateError{class: estimateRefused, err: olderVersionError{err: fmt.Errorf(format, args...)}}
}

// badEstimateRequest marks an error as the caller's: the window or the
// parameters can't be answered as asked.
func badEstimateRequest(format string, args ...any) error {
	return estimateError{class: estimateBadRequest, err: fmt.Errorf(format, args...)}
}

// refusedEstimate marks an error as a limit refusal.
func refusedEstimate(format string, args ...any) error {
	return estimateError{class: estimateRefused, err: fmt.Errorf(format, args...)}
}

// classifyStoreGatewayError turns a store-gateway's gRPC status into an
// estimate error. Unimplemented means the store-gateway is older than the
// querier and is refused with 422. ResourceExhausted means a refusal only when the message
// names one of the limits; gRPC also uses that code for a message over the
// size limit, which is a failure, not a refusal.
func classifyStoreGatewayError(addr string, err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("store-gateway %s: %w", addr, err)
	}
	switch st.Code() {
	case codes.InvalidArgument:
		return badEstimateRequest("store-gateway %s: %s", addr, st.Message())
	case codes.Unimplemented:
		return refusedEstimate("store-gateway %s %w yet, it may run an older version", addr, errCountsNotSupported)
	case codes.ResourceExhausted:
		if strings.Contains(st.Message(), "max_index_bytes") || strings.Contains(st.Message(), "max_names") {
			return refusedEstimate("store-gateway %s: %s", addr, st.Message())
		}
	}
	return fmt.Errorf("store-gateway %s: %w", addr, err)
}

// estimateHTTPStatus maps an estimate error to the status the route returns.
func estimateHTTPStatus(err error) int {
	var e estimateError
	if errors.As(err, &e) {
		switch e.class {
		case estimateBadRequest:
			return http.StatusBadRequest
		case estimateRefused:
			return http.StatusUnprocessableEntity
		}
	}
	return http.StatusInternalServerError
}
