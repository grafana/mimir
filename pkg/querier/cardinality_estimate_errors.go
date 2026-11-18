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

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
)

// estimateErrorClass says what an estimate error means to the caller, to pick an HTTP status.
type estimateErrorClass int

const (
	estimateBadRequest estimateErrorClass = iota + 1
	estimateRefused
	estimateTimedOut
	estimateCanceled
	estimateBusy
)

type estimateError struct {
	class estimateErrorClass
	// limit names the limit that refused it, one of the refusedBy values.
	limit string
	err   error
}

// The limits that refuse an estimate, as the refusals counter names them.
const (
	refusedByMemory    = cardinality.LimitMemory
	refusedByMaxGroups = cardinality.LimitMaxGroups
)

func (e estimateError) Error() string { return e.err.Error() }
func (e estimateError) Unwrap() error { return e.err }

// errCountsNotSupported is wrapped by the refusal of a source that doesn't know the count RPCs. It is not a limit.
var errCountsNotSupported = errors.New("does not support cardinality estimates")

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

// busyEstimate is the error of an estimate whose sources all run as many counts as they are allowed to. The caller may retry it.
func busyEstimate(kind string, err error) error {
	msg := kind
	if st, ok := grpcutil.ErrorToStatus(err); ok {
		msg += ": " + st.Message()
	}
	return estimateError{class: estimateBusy, err: sourceRefusal{msg: msg, err: err}}
}

// writeEstimateError writes err with its status, as the Prometheus API writes an error. Busy sources are answered like an unavailable server.
func writeEstimateError(w http.ResponseWriter, err error) {
	status := estimateHTTPStatus(err)
	var e estimateError
	if errors.As(err, &e) && e.class == estimateBusy {
		writeEstimateErrorBody(w, status, apierror.TypeUnavailable, err.Error())
		return
	}
	writeEstimateErrorStatus(w, status, err.Error())
}

// writeEstimateErrorStatus writes msg as a Prometheus API error body, with the error type the Prometheus API gives status.
func writeEstimateErrorStatus(w http.ResponseWriter, status int, msg string) {
	errorType := apierror.TypeInternal
	switch status {
	case http.StatusBadRequest:
		errorType = apierror.TypeBadData
	case http.StatusUnprocessableEntity:
		errorType = apierror.TypeExec
	case http.StatusServiceUnavailable:
		errorType = apierror.TypeTimeout
	case statusClientClosedRequest:
		errorType = apierror.TypeCanceled
	}
	writeEstimateErrorBody(w, status, errorType, msg)
}

func writeEstimateErrorBody(w http.ResponseWriter, status int, errorType apierror.Type, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "error", "errorType": string(errorType), "error": msg})
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
	case codes.InvalidArgument, codes.FailedPrecondition:
		// FailedPrecondition is a tenant with the estimate disabled, which every store-gateway answers the same way.
		return badEstimateRequest("store-gateway: %s", st.Message())
	case codes.Unimplemented:
		return refusedEstimate("a store-gateway %w yet, it may run an older version", errCountsNotSupported)
	case codes.ResourceExhausted:
		if limit := sourceRefusalLimit(err); limit != "" {
			return refusedByLimit(limit, "%w", sourceRefusal{msg: "store-gateway: " + st.Message(), err: err})
		}
		// gRPC's own message size limit is a failure.
		return fmt.Errorf("store-gateway: %w", err)
	case codes.Unavailable:
		if cardinality.IsBusy(err) {
			return busyEstimate("store-gateway", err)
		}
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
		case estimateTimedOut, estimateBusy:
			return http.StatusServiceUnavailable
		case estimateCanceled:
			return statusClientClosedRequest
		}
	}
	return http.StatusInternalServerError
}
