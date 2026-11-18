// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The limits a source names when it refuses a count.
const (
	LimitMaxGroups = "max_groups"
	// LimitMemory is the tenant's cardinality estimate memory limit, applied to what a source holds while it counts.
	LimitMemory = "memory"
)

// refusalDomain marks a refusal's ErrorInfo, to tell it from gRPC's own ResourceExhausted.
const refusalDomain = "cardinality_estimate"

// Refusalf returns a ResourceExhausted error refusing a count on limit, one of the Limit values.
func Refusalf(limit, format string, args ...any) error {
	return refusal(limit, fmt.Sprintf(format, args...))
}

// MaxGroupsRefusal refuses a request with more than maxGroups distinct values
// of groupBy. holder says where they were counted, such as "the blocks hold".
func MaxGroupsRefusal(holder string, groups int64, groupBy string, maxGroups int64) error {
	return Refusalf(LimitMaxGroups, "%s %d distinct values of %s, more than max_groups %d", holder, groups, groupBy, maxGroups)
}

func refusal(limit string, msg string) error {
	st := status.New(codes.ResourceExhausted, msg)
	with, err := st.WithDetails(&errdetails.ErrorInfo{Reason: limit, Domain: refusalDomain})
	if err != nil {
		return st.Err()
	}
	return with.Err()
}

// busyReason is the ErrorInfo reason of a source at its limit of concurrent counts.
const busyReason = "busy"

// Busyf returns an Unavailable error for a source that runs as many counts as it is allowed to.
// The caller may ask another source, or the same one later. See IsBusy.
func Busyf(format string, args ...any) error {
	st := status.New(codes.Unavailable, fmt.Sprintf(format, args...))
	with, err := st.WithDetails(&errdetails.ErrorInfo{Reason: busyReason, Domain: refusalDomain})
	if err != nil {
		return st.Err()
	}
	return with.Err()
}

// IsBusy reports whether err's chain is a Busyf error, to tell it from a source that could not be reached.
func IsBusy(err error) bool {
	info := errorInfo(err, codes.Unavailable)
	return info != nil && info.Reason == busyReason
}

// RefusedBy returns the limit err's chain refuses on, or "" if it is not a refusal.
func RefusedBy(err error) string {
	if info := refusalInfo(err); info != nil {
		return info.Reason
	}
	return ""
}

func refusalInfo(err error) *errdetails.ErrorInfo {
	return errorInfo(err, codes.ResourceExhausted)
}

// errorInfo returns the ErrorInfo detail of the status with code in err's chain.
func errorInfo(err error, code codes.Code) *errdetails.ErrorInfo {
	if err == nil {
		return nil
	}
	//lint:ignore faillint The ErrorInfo detail is a golang/protobuf message, which a gogo status from grpcutil.ErrorToStatus can not decode.
	st, ok := status.FromError(err)
	if !ok || st.Code() != code {
		return nil
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.Domain == refusalDomain {
			return info
		}
	}
	return nil
}
