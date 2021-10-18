// SPDX-License-Identifier: AGPL-3.0-only

package queryrange

import (
	"net/http"

	"github.com/pkg/errors"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/weaveworks/common/httpgrpc"
)

// newBadDataError creates "bad_data" error response formatted as Prometheus would.
func newBadDataError(inErr error) error {
	type body struct {
		Status    string           `json:"status"`
		ErrorType promv1.ErrorType `json:"errorType,omitempty"`
		Error     string           `json:"error,omitempty"`
	}
	b := body{
		Status:    "error",
		ErrorType: promv1.ErrBadData,
		Error:     inErr.Error(),
	}

	out, err := json.Marshal(&b)
	if err != nil {
		return httpgrpc.Errorf(http.StatusBadRequest,
			"failed to marshal api error: %s, original error: %s", err, inErr)
	}

	return httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: int32(http.StatusBadRequest),
		Headers: []*httpgrpc.Header{
			{
				Key:    "Content-Type",
				Values: []string{"application/json"},
			},
		},
		Body: []byte(out),
	})
}

// newInvalidParamError creates "bad_data" error formatted and prefixed as Prometheus would.
func newInvalidParamError(err error, param string) error {
	return newBadDataError(errors.Wrapf(err, "invalid parameter %q", param))
}
