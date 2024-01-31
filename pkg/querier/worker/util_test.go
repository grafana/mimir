// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"context"
	"errors"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
)

// This function emulates the error-translating behaviour of google.golang.org/grpc.toRPCErr(),
// which is used by the gRPC client to translate errors (including client-side context cancellations)
// to gRPC-style errors.
//
// This implementation does not cover all the cases supported by the real implementation - it has just
// enough to support the test cases in this file.
//
// Based on https://github.com/grpc/grpc-go/blob/c0aa20a8ac825f86edd59b2cab842de6da77a841/rpc_util.go#L874.
func toRPCErr(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, context.Canceled.Error())
	}

	return status.Error(codes.Unknown, err.Error())
}
