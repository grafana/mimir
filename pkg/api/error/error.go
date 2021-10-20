// SPDX-License-Identifier: AGPL-3.0-only

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

type APIError struct {
	Type    Type
	Message string
}

func (e *APIError) Error() string {
	return e.Message
}

// adapted from https://github.com/grafana/mimir/blob/8f49e25bbd603ce85eadb6247204764c388e7d84/vendor/github.com/prometheus/prometheus/web/api/v1/api.go#L1508-L1521
func (e *APIError) StatusCode() int {
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

// HTTPResponseFromError converts an APIError into a JSON HTTP response
func HTTPResponseFromError(err error) (*httpgrpc.HTTPResponse, bool) {
	var apiError *APIError
	if !errors.As(err, &apiError) {
		return nil, false
	}

	body, err := json.Marshal(
		struct {
			Status    string `json:"status"`
			ErrorType Type   `json:"errorType,omitempty"`
			Error     string `json:"error,omitempty"`
		}{
			Status:    "error",
			Error:     apiError.Message,
			ErrorType: apiError.Type,
		},
	)
	if err != nil {
		return nil, false
	}

	return &httpgrpc.HTTPResponse{
		Code: int32(apiError.StatusCode()),
		Body: body,
		Headers: []*httpgrpc.Header{
			{Key: "Content-Type", Values: []string{"application/json"}},
		},
	}, true
}

func New(typ Type, msg string) error {
	return &APIError{
		Message: msg,
		Type:    typ,
	}
}

func Newf(typ Type, tmpl string, args ...interface{}) error {
	return New(typ, fmt.Sprintf(tmpl, args...))
}
