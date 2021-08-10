// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/grpc/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package grpc

import (
	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
)

// IsGRPCContextCanceled returns whether the input error is a GRPC error wrapping
// the context.Canceled error.
func IsGRPCContextCanceled(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}

	return s.Code() == codes.Canceled
}
