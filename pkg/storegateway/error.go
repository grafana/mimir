// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import "github.com/grafana/mimir/pkg/mimirpb"

type storeGatewayError interface {
	error
	errorCause() mimirpb.ErrorCause
}

type staticError struct {
	cause mimirpb.ErrorCause
	msg   string
}

func (s staticError) Error() string {
	return s.msg
}

func (s staticError) errorCause() mimirpb.ErrorCause {
	return s.cause
}
