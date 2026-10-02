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
// estimate error. ResourceExhausted means a refusal only when the message
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
