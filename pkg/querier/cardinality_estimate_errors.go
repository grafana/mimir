// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/prometheus/common/model"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/cardinality"
)

// estimateErrorClass says what an estimate error means to the caller, to pick an HTTP status.
type estimateErrorClass int

const (
	estimateBadRequest estimateErrorClass = iota + 1
	estimateRefused
	estimateTimedOut
	estimateCanceled
)

type estimateError struct {
	class estimateErrorClass
	// limit names the limit that refused it, one of the refusedBy values.
	limit string
	err   error
}

// The limits that refuse an estimate, as the refusals counter names them.
const (
	refusedByMaxHashes                    = cardinality.LimitMaxHashes
	refusedByMaxIndexBytes                = cardinality.LimitMaxIndexBytes
	refusedByMemory                       = "memory"
	refusedByMaxGroups                    = cardinality.LimitMaxGroups
	refusedByMaxBucketsAnsweredSeparately = "max_buckets_answered_separately"
)

func (e estimateError) Error() string { return e.err.Error() }
func (e estimateError) Unwrap() error { return e.err }

// errStepNeedsOneSource is wrapped by refusals of per-bucket counts over sources that may share series.
var errStepNeedsOneSource = errors.New("per-bucket counts need each bucket held by sources that share no series")

// errCountsNotSupported is wrapped by the refusal of a source that doesn't know the count RPCs. It is not a limit.
var errCountsNotSupported = errors.New("does not support cardinality estimates")

// olderVersionError is an answer that shows its source runs an older version. It reads as errCountsNotSupported.
type olderVersionError struct{ err error }

func (e olderVersionError) Error() string        { return e.err.Error() }
func (e olderVersionError) Unwrap() error        { return e.err }
func (e olderVersionError) Is(target error) bool { return target == errCountsNotSupported }

// olderVersionEstimate refuses an answer from an older source with 422. It is not retried, as the groups merged so far would be counted twice.
func olderVersionEstimate(format string, args ...any) error {
	return estimateError{class: estimateRefused, err: olderVersionError{err: fmt.Errorf(format, args...)}}
}

// sourceAddrError is an error from the store-gateway at addr. Its text names no address since it goes back to the caller; the handler logs addr.
type sourceAddrError struct {
	addr string
	err  error
}

func (e sourceAddrError) Error() string { return e.err.Error() }
func (e sourceAddrError) Unwrap() error { return e.err }

// withSourceAddr records that err came from the component at addr.
func withSourceAddr(addr string, err error) error {
	if err == nil {
		return nil
	}
	return sourceAddrError{addr: addr, err: err}
}

// estimateSourceAddr is the address of the component err came from, or "".
func estimateSourceAddr(err error) string {
	var e sourceAddrError
	if errors.As(err, &e) {
		return e.addr
	}
	return ""
}

// redactedError is a failure whose text may carry a component's address. The caller sees msg; the full error stays in the chain for the log.
type redactedError struct {
	msg string
	err error
}

func (e redactedError) Error() string { return e.msg }
func (e redactedError) Unwrap() error { return e.err }

// redactFailure hides the text of err behind its gRPC code. Context errors keep their text.
func redactFailure(kind string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", kind, err)
	}
	st, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		return redactedError{msg: kind + " failed", err: err}
	}
	return redactedError{msg: fmt.Sprintf("%s: the call failed with %s", kind, st.Code()), err: err}
}

// estimateFailureDetail is the full text of err for the log line.
func estimateFailureDetail(err error) string {
	var r redactedError
	if errors.As(err, &r) {
		return r.err.Error()
	}
	return err.Error()
}

// timedOutEstimate is the error of an estimate that ran past -querier.timeout.
func timedOutEstimate(timeout time.Duration, err error) error {
	return estimateError{class: estimateTimedOut, err: fmt.Errorf("cardinality estimate timed out after %s (-querier.timeout): %w", timeout, err)}
}

// canceledEstimate is the error of an estimate whose caller went away. It is not counted as a failure.
func canceledEstimate(err error) error {
	return estimateError{class: estimateCanceled, err: fmt.Errorf("cardinality estimate canceled by the caller: %w", err)}
}

// writeEstimateError writes err with its status. A timeout is answered like a timed out Prometheus query.
func writeEstimateError(w http.ResponseWriter, err error) {
	status := estimateHTTPStatus(err)
	if status != http.StatusServiceUnavailable {
		http.Error(w, err.Error(), status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "errorType": "timeout", "error": err.Error()})
}

// badEstimateRequest marks an error as the caller's.
func badEstimateRequest(format string, args ...any) error {
	return estimateError{class: estimateBadRequest, err: fmt.Errorf(format, args...)}
}

// refusedEstimate marks a refusal that no limit made.
func refusedEstimate(format string, args ...any) error {
	return estimateError{class: estimateRefused, err: fmt.Errorf(format, args...)}
}

// refusedByLimit marks an error as the refusal of limit.
func refusedByLimit(limit, format string, args ...any) error {
	return estimateError{class: estimateRefused, limit: limit, err: fmt.Errorf(format, args...)}
}

// estimateRefusalLimit is the limit that refused err, or "".
func estimateRefusalLimit(err error) string {
	var e estimateError
	if errors.As(err, &e) && e.class == estimateRefused {
		return e.limit
	}
	return ""
}

// errOverMaxGroups refuses an estimate with more groups than -querier.cardinality-estimate-max-groups.
func errOverMaxGroups(sources string, limit int64, groupBy string) error {
	if groupBy == "" {
		groupBy = model.MetricNameLabel
	}
	return refusedByLimit(refusedByMaxGroups, "%s hold more than %d distinct values of %s (the -querier.cardinality-estimate-max-groups limit)", sources, limit, groupBy)
}

// sourceRefusal is a source's refusal as the caller sees it. The source's status stays in the chain to read its detail back.
type sourceRefusal struct {
	msg string
	err error
}

func (e sourceRefusal) Error() string { return e.msg }
func (e sourceRefusal) Unwrap() error { return e.err }

// sourceRefusalLimit is the limit a source's refusal names in its status detail, or "".
func sourceRefusalLimit(err error) string {
	return cardinality.RefusedBy(err)
}

// classifyStoreGatewayError turns a store-gateway's gRPC status into an estimate error. ResourceExhausted is a refusal only when the status names a limit.
func classifyStoreGatewayError(addr string, err error) error {
	return withSourceAddr(addr, classifyStoreGatewayStatus(err))
}

func classifyStoreGatewayStatus(err error) error {
	st, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		return redactFailure("store-gateway", err)
	}
	switch st.Code() {
	case codes.InvalidArgument:
		return badEstimateRequest("store-gateway: %s", st.Message())
	case codes.Unimplemented:
		return refusedEstimate("a store-gateway %w yet, it may run an older version", errCountsNotSupported)
	case codes.ResourceExhausted:
		if limit := sourceRefusalLimit(err); limit != "" {
			return refusedByLimit(limit, "%w", sourceRefusal{msg: "store-gateway: " + st.Message(), err: err})
		}
		// gRPC's own message size limit is a failure.
		return fmt.Errorf("store-gateway: %w", err)
	}
	return redactFailure("store-gateway", err)
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
		case estimateTimedOut:
			return http.StatusServiceUnavailable
		case estimateCanceled:
			return statusClientClosedRequest
		}
	}
	return http.StatusInternalServerError
}
