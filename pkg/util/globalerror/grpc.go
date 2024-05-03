// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"context"

	"github.com/gogo/status"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
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

// ErrorWithStatus is used for wrapping errors returned by ingester.
// Errors returned by ingester should be gRPC errors, but the errors
// produced by both gogo/status and grpc/status packages do not keep
// the semantics of the underlying error, which is sometimes needed.
// For example, the logging middleware needs to know whether an error
// should be logged, sampled or ignored. Errors of type ErrorWithStatus
// are valid gRPC errors that could be parsed by both gogo/status
// and grpc/status packages, but which preserve the original error
// semantics.
type ErrorWithStatus struct {
	UnderlyingErr error
	Status        *status.Status
}

func NewErrorWithGRPCStatus(originalErr error, code codes.Code, details *mimirpb.ErrorDetails) ErrorWithStatus {
	stat := status.New(code, originalErr.Error())
	if details != nil {
		if statWithDetails, err := stat.WithDetails(details); err == nil {
			return ErrorWithStatus{
				UnderlyingErr: originalErr,
				Status:        statWithDetails,
			}
		}
	}
	return ErrorWithStatus{
		UnderlyingErr: originalErr,
		Status:        stat,
	}
}

func (e ErrorWithStatus) Error() string {
	return e.Status.Message()
}

func (e ErrorWithStatus) Unwrap() error {
	return e.UnderlyingErr
}

// GRPCStatus with a *grpcstatus.Status as output is needed
// for a correct execution of grpc/status.FromError().
func (e ErrorWithStatus) GRPCStatus() *grpcstatus.Status {
	if stat, ok := e.Status.Err().(interface{ GRPCStatus() *grpcstatus.Status }); ok {
		return stat.GRPCStatus()
	}
	return nil
}

// writeErrorDetails is needed for testing purposes only. It returns the
// mimirpb.ErrorDetails object stored in this error's Status, if any
// or nil otherwise.
func (e ErrorWithStatus) writeErrorDetails() *mimirpb.ErrorDetails {
	details := e.Status.Details()
	if len(details) != 1 {
		return nil
	}
	if errDetails, ok := details[0].(*mimirpb.ErrorDetails); ok {
		return errDetails
	}
	return nil
}

// Equals returns true if the given error and this error are equal, i.e., if they are both of
// type ErrorWithStatus, if their underlying statuses have the same code,
// messages, and if both have either no details, or exactly one detail
// of type mimirpb.ErrorDetails, which are equal too.
func (e ErrorWithStatus) Equals(err error) bool {
	if err == nil {
		return false
	}

	errWithStatus, ok := err.(ErrorWithStatus)
	if !ok {
		return false
	}
	if e.Status.Code() != errWithStatus.Status.Code() || e.Status.Message() != errWithStatus.Status.Message() {
		return false
	}
	errDetails := e.writeErrorDetails()
	otherErrDetails := errWithStatus.writeErrorDetails()
	if errDetails == nil && otherErrDetails == nil {
		return true
	}
	if errDetails != nil && otherErrDetails != nil {
		return errDetails.GetCause() == otherErrDetails.GetCause()
	}
	return false
}
