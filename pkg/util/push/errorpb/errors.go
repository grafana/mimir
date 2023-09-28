// SPDX-License-Identifier: AGPL-3.0-only

package errorpb

import (
	"slices"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"
	grpc "google.golang.org/grpc/status"
)

type ErrorDetail interface {
	errorDetail()
}

func (t ErrorType) errorDetail() {}

func (f OptionalFlag) errorDetail() {}

func (p *PushErrorDetails) ContainsOptionalFlag(optionalFlag OptionalFlag) bool {
	return slices.Contains(p.GetOptionalFlags(), optionalFlag)
}

// newPushErrorDetails creates a new instance of PushErrorDetails by using
// the given set of ErrorDetail objects.
func newPushErrorDetails(errorDetails ...ErrorDetail) *PushErrorDetails {
	var (
		errorType     ErrorType
		optionalFlags []OptionalFlag
	)
	if len(errorDetails) == 0 {
		return nil
	}
	if len(errorDetails) > 1 {
		optionalFlags = make([]OptionalFlag, 0, len(errorDetails)-1)
	}
	for _, opt := range errorDetails {
		eT, ok := opt.(ErrorType)
		if ok {
			errorType = eT
			continue
		}
		oF, ok := opt.(OptionalFlag)
		if ok {
			optionalFlags = append(optionalFlags, oF)
		}
	}
	return &PushErrorDetails{
		ErrorType:     errorType,
		OptionalFlags: optionalFlags,
	}
}

type errorWithStatus struct {
	err    error
	status *status.Status
}

func newErrorWithStatus(code codes.Code, originalErr error) errorWithStatus {
	return errorWithStatus{
		err: originalErr,
		status: status.New(
			code,
			originalErr.Error(),
		),
	}
}

func (e errorWithStatus) Error() string {
	return e.status.Message()
}

func (e errorWithStatus) Unwrap() error {
	return e.err
}

func (e errorWithStatus) GRPCStatus() *grpc.Status {
	se, ok := e.status.Err().(interface{ GRPCStatus() *grpc.Status })
	if !ok {
		return nil
	}
	return se.GRPCStatus()
}

// NewPushError gets an error, a gRPC code and a set of ErrorDetail objects,
// and does the following things:
//   - if the given error is produced by gogo/status or grpc/status packages,
//     the error itself with no additional details is returned.
//   - Otherwise, a new gRPC errorWithStatus error backed up by the given error,
//     with the given gRPC code and with given set of details is created and
//     returned.
func NewPushError(code codes.Code, originalErr error, errorDetails ...ErrorDetail) error {
	if _, ok := status.FromError(originalErr); ok {
		return originalErr
	}

	return addPushErrorDetails(
		newErrorWithStatus(
			code,
			originalErr,
		),
		errorDetails...,
	)
}

// addPushErrorDetails gets a gRPC error and a set of ErrorDetail objects,
// and creates a new errorWithStatus with the provided details appended to
// the original error's status.
// If the operation is successful, the new error is returned.
// If the original error was not a gRPC error, no details are passed as parameter,
// or if the operation was not successful, the original error is returned.
func addPushErrorDetails(srcErr error, errDetails ...ErrorDetail) error {
	errorDetails := newPushErrorDetails(errDetails...)
	if errorDetails == nil {
		return srcErr
	}
	stat, ok := status.FromError(srcErr)
	if !ok {
		return srcErr
	}
	statWithDetails, err := stat.WithDetails(errorDetails)
	if err != nil {
		return srcErr
	}
	return errorWithStatus{
		err:    srcErr,
		status: statWithDetails,
	}
}

// GetPushErrorDetails gets a gRPC error, and returns the first occurrence of a PushErrorDetail
// object from the status of the former. If this was successful, the status true is returned.
// If the error is a gRPC error, or no PushErrorDetails are found, nil and false are returned.
func GetPushErrorDetails(err error) (*PushErrorDetails, bool) {
	stat, ok := status.FromError(err)
	if !ok {
		return nil, false
	}

	details := stat.Details()
	for _, detail := range details {
		errDetails, ok := detail.(*PushErrorDetails)
		if ok {
			return errDetails, true
		}
	}
	return nil, false
}
