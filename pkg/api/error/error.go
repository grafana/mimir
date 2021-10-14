// SPDX-License-Identifier: AGPL-3.0-only

package error

import (
	"encoding/json"
	"fmt"

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

func JSONErrorf(typ Type, code int, tmpl string, args ...interface{}) error {
	body, err := json.Marshal(
		struct {
			Status    string `json:"status"`
			ErrorType Type   `json:"errorType,omitempty"`
			Error     string `json:"error,omitempty"`
		}{
			Status:    "error",
			Error:     fmt.Sprintf(tmpl, args...),
			ErrorType: typ,
		},
	)
	if err != nil {
		return err
	}

	return httpgrpc.ErrorFromHTTPResponse(&httpgrpc.HTTPResponse{
		Code: int32(code),
		Body: body,
		Headers: []*httpgrpc.Header{
			{Key: "Content-Type", Values: []string{"application/json"}},
		},
	})
}
