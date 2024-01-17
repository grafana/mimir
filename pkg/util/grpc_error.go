// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// WrapGrpcContextError wraps a gRPC error with a custom error mapping gRPC to standard golang context errors.
func WrapGrpcContextError(err error) error {
	if err == nil {
		return nil
	}

	// Get the gRPC status from the error.
	type grpcErrorStatus interface{ GRPCStatus() *grpcstatus.Status }
	var grpcError grpcErrorStatus
	if !errors.As(err, &grpcError) {
		return err
	}

	switch status := grpcError.GRPCStatus(); {
	case status.Code() == codes.Canceled:
		return &grpcContextError{grpcErr: err, grpcStatus: status, stdErr: context.Canceled}
	case status.Code() == codes.DeadlineExceeded:
		return &grpcContextError{grpcErr: err, grpcStatus: status, stdErr: context.DeadlineExceeded}
	default:
		return err
	}
}

// grpcContextError is a custom error used to wrap gRPC errors which maps to golang standard context errors.
type grpcContextError struct {
	// grpcErr is the gRPC error wrapped by grpcContextError.
	grpcErr error

	// grpcStatus is the gRPC status associated with the gRPC error. It's guaranteed to be non-nil.
	grpcStatus *grpcstatus.Status

	// stdErr is the equivalent golang standard context error.
	stdErr error
}

func (e *grpcContextError) Error() string {
	return e.grpcErr.Error()
}

func (e *grpcContextError) Unwrap() error {
	return e.grpcErr
}

func (e *grpcContextError) Is(err error) bool {
	return errors.Is(e.stdErr, err) || errors.Is(e.grpcErr, err)
}

func (e *grpcContextError) As(target any) bool {
	if errors.As(e.stdErr, target) {
		return true
	}

	return errors.As(e.grpcErr, target)
}

func (e *grpcContextError) GRPCStatus() *grpcstatus.Status {
	return e.grpcStatus
}
