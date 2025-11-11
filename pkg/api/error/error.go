// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package error

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/prometheus/promql"
	"google.golang.org/grpc/codes"
)

type Type string

// adapted from https://github.com/prometheus/prometheus/blob/fdbc40a9efcc8197a94f23f0e479b0b56e52d424/web/api/v1/api.go#L67-L76
const (
	TypeNone            Type = ""
	TypeTimeout         Type = "timeout"
	TypeCanceled        Type = "canceled"
	TypeExec            Type = "execution"
	TypeBadData         Type = "bad_data"
	TypeInternal        Type = "internal"
	TypeUnavailable     Type = "unavailable"
	TypeNotFound        Type = "not_found"
	TypeTooManyRequests Type = "too_many_requests"
	TypeTooLargeEntry   Type = "too_large_entry"
	TypeNotAcceptable   Type = "not_acceptable"
)

// TypeForError returns the appropriate Type for err, or fallback if no appropriate Type
// can be inferred.
func TypeForError(err error, fallback Type) Type {
	// This method mirrors the behaviour of Prometheus' returnAPIError (https://github.com/prometheus/prometheus/blob/1ada3ced5a91fb4a6e5df473ac360ad99e62209e/web/api/v1/api.go#L682).
	var apiError *APIError
	if errors.As(err, &apiError) {
		return apiError.Type
	}

	if errors.Is(err, context.Canceled) {
		return TypeCanceled
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return TypeTimeout
	}

	var queryCanceledErr promql.ErrQueryCanceled
	if errors.As(err, &queryCanceledErr) {
		return TypeCanceled
	}

	var queryTimeoutErr promql.ErrQueryTimeout
	if errors.As(err, &queryTimeoutErr) {
		return TypeTimeout
	}

	var storageError promql.ErrStorage
	if errors.As(err, &storageError) {
		return TypeInternal
	}

	if s, ok := grpcutil.ErrorToStatus(err); ok {
		return typeForCode(s.Code(), fallback)
	}

	return fallback
}

func typeForCode(code codes.Code, fallback Type) Type {
	switch code {
	case codes.Canceled:
		return TypeCanceled
	case codes.Internal:
		return TypeInternal
	case codes.DeadlineExceeded:
		return TypeTimeout
	case codes.Unavailable:
		return TypeUnavailable
	case codes.NotFound:
		return TypeNotFound
	case codes.InvalidArgument:
		return TypeBadData
	default:
		return fallback
	}
}

type APIError struct {
	Type    Type
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

// adapted from https://github.com/prometheus/prometheus/blob/fdbc40a9efcc8197a94f23f0e479b0b56e52d424/web/api/v1/api.go#L1508-L1521
func (e *APIError) StatusCode() int {
	switch e.Type {
	case TypeBadData:
		return http.StatusBadRequest
	case TypeExec:
		return http.StatusUnprocessableEntity
	case TypeCanceled:
		return 499
	case TypeTimeout:
		return http.StatusServiceUnavailable
	case TypeInternal:
		return http.StatusInternalServerError
	case TypeNotFound:
		return http.StatusNotFound
	case TypeTooManyRequests:
		return http.StatusTooManyRequests
	case TypeTooLargeEntry:
		return http.StatusRequestEntityTooLarge
	case TypeNotAcceptable:
		return http.StatusNotAcceptable
	case TypeUnavailable:
		return http.StatusServiceUnavailable
	}
	return http.StatusInternalServerError
}

func (e *APIError) EncodeJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			Status    string `json:"status"`
			ErrorType Type   `json:"errorType,omitempty"`
			Error     string `json:"error,omitempty"`
		}{
			Status:    "error",
			Error:     e.Message,
			ErrorType: e.Type,
		},
	)
}

// HTTPResponseFromError converts an APIError into a JSON HTTP response
func HTTPResponseFromError(err error) (*httpgrpc.HTTPResponse, bool) {
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		return nil, false
	}

	body, err := apiErr.EncodeJSON()
	if err != nil {
		return nil, false
	}

	return &httpgrpc.HTTPResponse{
		Code: int32(apiErr.StatusCode()),
		Body: body,
		Headers: []*httpgrpc.Header{
			{Key: "Content-Type", Values: []string{"application/json"}},
		},
	}, true
}

// New creates a new apiError with a static string message
func New(typ Type, msg string) *APIError {
	return &APIError{
		Message: msg,
		Type:    typ,
	}
}

// Newf creates a new apiError with a formatted message
func Newf(typ Type, tmpl string, args ...interface{}) *APIError {
	return New(typ, fmt.Sprintf(tmpl, args...))
}

// IsAPIError returns true if the error provided is an apiError.
// This implies that HTTPResponseFromError will succeed.
func IsAPIError(err error) bool {
	apiErr := &APIError{}
	return errors.As(err, &apiErr)
}

// AddDetails adds details to an existing apiError, but keeps the type and handling.
// If the error is not an apiError, it will wrap the error with the details.
func AddDetails(err error, details string) error {
	apiErr := &APIError{}
	if !errors.As(err, &apiErr) {
		return fmt.Errorf("%s: %w", details, err)
	}
	apiErr.Message = fmt.Sprintf("%s: %s", details, apiErr.Message)
	return apiErr
}

// IsRetryableAPIError returns true if err is an apiError which should be retried, false otherwise.
func IsRetryableAPIError(err error) bool {
	apiErr := &APIError{}
	// If this isn't actually an APIError we can't determine if it should or should not be
	// retried. We count on the Prometheus API converting errors into APIErrors before we
	// apply our retry logic.
	if !errors.As(err, &apiErr) {
		return true
	}
	return IsRetryableAPIErrorType(apiErr.Type)
}

func IsRetryableAPIErrorType(typ Type) bool {
	// Reasoning:
	// TypeNone and TypeNotFound are not used anywhere in Mimir nor Prometheus;
	// TypeTimeout, TypeTooManyRequests, TypeNotAcceptable, TypeUnavailable we presume a retry of the same request will fail in the same way.
	// TypeCanceled means something wants us to stop.
	// TypeExec, TypeBadData and TypeTooLargeEntry are caused by the input data.
	// TypeInternal can be a 500 error e.g. from querier failing to contact store-gateway.
	return typ == TypeInternal
}
