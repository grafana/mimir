// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"github.com/grafana/dskit/grpcutil"
)

// IsClientError returns true if err is a gRPC or HTTPgRPC error whose cause is a well-known client error.
func IsClientError(err error) bool {
	// This code is needed for backward compatibility.
	if code := grpcutil.ErrorToStatusCode(err); code/100 == 4 {
		return true
	}

	if status, ok := grpcutil.ErrorToStatus(err); ok {
		for _, details := range status.Details() {
			if errDetails, ok := details.(*ErrorDetails); ok {
				return IsClientErrorCause(errDetails.GetCause())
			}
		}
	}

	return false
}

// IsClientErrorCause returns true if the given ErrorCause corresponds to a well-known client error cause.
func IsClientErrorCause(errCause ErrorCause) bool {
	return errCause == BAD_DATA || errCause == TENANT_LIMIT
}
