// SPDX-License-Identifier: AGPL-3.0-only

package globalerror

import (
	"context"

	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var (
	grpcClientConnectionIsClosingErr = "grpc: the client connection is closing"
)

// WrapGRPCErrorWithContextError checks if the given error is a gRPC error corresponding
// to a standard golang context error, and if it is, wraps the former with the latter.
// If the given error isn't a gRPC error, or it doesn't correspond to a standard golang
// context error, the original error is returned.
func WrapGRPCErrorWithContextError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() == nil {
		return err
	}
	if stat, ok := grpcutil.ErrorToStatus(err); ok {
		switch stat.Code() {
		case codes.Canceled:
			if stat.Message() != grpcClientConnectionIsClosingErr {
				return &ErrorWithStatus{
					UnderlyingErr: err,
					Status:        stat,
					ctxErr:        context.Canceled,
				}
			}
		case codes.DeadlineExceeded:
			return &ErrorWithStatus{
				UnderlyingErr: err,
				Status:        stat,
				ctxErr:        context.DeadlineExceeded,
			}
		default:
			return err
		}
	}
	return err
}

// WrapErrorWithGRPCStatus wraps the given error with a gRPC status, which is built out of the given parameters:
// the gRPC status' code and details are passed as parameters, while its message corresponds to the original error.
// The resulting error is of type ErrorWithStatus.
func WrapErrorWithGRPCStatus(originalErr error, errCode codes.Code, errDetails *mimirpb.ErrorDetails) ErrorWithStatus {
	stat := createGRPCStatus(originalErr, errCode, errDetails)
	return ErrorWithStatus{
		UnderlyingErr: originalErr,
		Status:        stat,
	}
}

func createGRPCStatus(originalErr error, errCode codes.Code, errDetails *mimirpb.ErrorDetails) *status.Status {
	stat := status.New(errCode, originalErr.Error())
	if errDetails != nil {
		statWithDetails, err := stat.WithDetails(errDetails)
		if err == nil {
			return statWithDetails
		}
	}
	return stat
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
	ctxErr        error
}

func (e ErrorWithStatus) Error() string {
	return e.Status.Message()
}

// Err returns an immutable error representing this ErrorWithStatus.
// Returns nil if UnderlyingError is nil or Status.Code() is OK.
// The resulting error is of type error, and it can be parsed to
// the corresponding gRPC status by both gogo/status and grpc/status
// packages.
func (e ErrorWithStatus) Err() error {
	if e.UnderlyingErr == nil {
		return nil
	}
	if e.Status.Code() == codes.OK {
		return nil
	}
	return e.Status.Err()
}

func (e ErrorWithStatus) Unwrap() []error {
	if e.ctxErr == nil {
		return []error{e.UnderlyingErr}
	}
	return []error{e.UnderlyingErr, e.ctxErr}
}

// GRPCStatus with a *grpcstatus.Status as output is needed
// for a correct execution of grpc/status.FromError().
func (e ErrorWithStatus) GRPCStatus() *grpcstatus.Status {
	if stat, ok := e.Status.Err().(interface{ GRPCStatus() *grpcstatus.Status }); ok {
		return stat.GRPCStatus()
	}
	return nil
}

// details is needed for testing purposes only. It returns the
// mimirpb.ErrorDetails object stored in this error's Status, if any
// or nil otherwise.
func (e ErrorWithStatus) details() *mimirpb.ErrorDetails {
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
	errDetails := e.details()
	otherErrDetails := errWithStatus.details()
	if errDetails == nil && otherErrDetails == nil {
		return true
	}
	if errDetails != nil && otherErrDetails != nil {
		return errDetails.GetCause() == otherErrDetails.GetCause()
	}
	return false
}
