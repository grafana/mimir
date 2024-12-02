// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"

	"github.com/grafana/dskit/cancellation"
)

const requestValidationFailedFmt = "request validation failed for "

var errMetricsQueryRequestValidationFailed = cancellation.NewErrorf(
	requestValidationFailedFmt + "metrics query",
)
var errLabelsQueryRequestValidationFailed = cancellation.NewErrorf(
	requestValidationFailedFmt + "labels query",
)

type MetricsQueryRequestValidationRoundTripper struct {
	codec Codec
	next  http.RoundTripper
}

func NewMetricsQueryRequestValidationRoundTripper(codec Codec, next http.RoundTripper) http.RoundTripper {
	return MetricsQueryRequestValidationRoundTripper{
		codec: codec,
		next:  next,
	}
}

func (rt MetricsQueryRequestValidationRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(errMetricsQueryRequestValidationFailed)

	_, err := rt.codec.DecodeMetricsQueryRequest(ctx, r)
	if err != nil {
		return nil, err
	}
	return rt.next.RoundTrip(r)
}

type LabelsQueryRequestValidationRoundTripper struct {
	codec Codec
	next  http.RoundTripper
}

func NewLabelsQueryRequestValidationRoundTripper(codec Codec, next http.RoundTripper) http.RoundTripper {
	return LabelsQueryRequestValidationRoundTripper{
		codec: codec,
		next:  next,
	}
}

func (rt LabelsQueryRequestValidationRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, cancel := context.WithCancelCause(r.Context())
	defer cancel(errLabelsQueryRequestValidationFailed)

	_, err := rt.codec.DecodeLabelsQueryRequest(ctx, r)
	if err != nil {
		return nil, err
	}
	return rt.next.RoundTrip(r)
}
