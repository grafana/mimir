// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package error

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/weaveworks/common/httpgrpc"
)

type Type string

// adapted from https://github.com/prometheus/prometheus/blob/fdbc40a9efcc8197a94f23f0e479b0b56e52d424/web/api/v1/api.go#L67-L76
const (
	TypeNone        Type = ""
	TypeTimeout     Type = "timeout"
	TypeCanceled    Type = "canceled"
	TypeExec        Type = "execution"
	TypeBadData     Type = "bad_data"
	TypeInternal    Type = "internal"
	TypeUnavailable Type = "unavailable"
	TypeNotFound    Type = "not_found"
)

type apiError struct {
	Type    Type
	Message string
}

func (e *apiError) Error() string {
	return e.Message
}

// adapted from https://github.com/prometheus/prometheus/blob/fdbc40a9efcc8197a94f23f0e479b0b56e52d424/web/api/v1/api.go#L1508-L1521
func (e *apiError) statusCode() int {
	switch e.Type {
	case TypeBadData:
		return http.StatusBadRequest
	case TypeExec:
		return http.StatusUnprocessableEntity
	case TypeCanceled, TypeTimeout:
		return http.StatusServiceUnavailable
	case TypeInternal:
		return http.StatusInternalServerError
	case TypeNotFound:
		return http.StatusNotFound
	}
	return http.StatusInternalServerError
}

// HTTPResponseFromError converts an apiError into a JSON HTTP response
func HTTPResponseFromError(err error) (*httpgrpc.HTTPResponse, bool) {
	var apiErr *apiError
	if !errors.As(err, &apiErr) {
		return nil, false
	}

	body, err := json.Marshal(
		struct {
			Status    string `json:"status"`
			ErrorType Type   `json:"errorType,omitempty"`
			Error     string `json:"error,omitempty"`
		}{
			Status:    "error",
			Error:     apiErr.Message,
			ErrorType: apiErr.Type,
		},
	)
	if err != nil {
		return nil, false
	}

	return &httpgrpc.HTTPResponse{
		Code: int32(apiErr.statusCode()),
		Body: body,
		Headers: []*httpgrpc.Header{
			{Key: "Content-Type", Values: []string{"application/json"}},
		},
	}, true
}

// New creates a new apiError with a static string message
func New(typ Type, msg string) error {
	return &apiError{
		Message: msg,
		Type:    typ,
	}
}

// Newf creates a new apiError with a static string message
func Newf(typ Type, tmpl string, args ...interface{}) error {
	return New(typ, fmt.Sprintf(tmpl, args...))
}
