// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"fmt"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The limits a source names when it refuses a count.
const (
	LimitMaxIndexBytes = "max_index_bytes"
	LimitMaxHashes     = "max_hashes"
	LimitMaxGroups     = "max_groups"
)

// refusalDomain marks a refusal's ErrorInfo, to tell it from gRPC's own ResourceExhausted.
const refusalDomain = "cardinality_estimate"

// hashesNeededKey is the ErrorInfo metadata key of a max_hashes refusal's series count.
const hashesNeededKey = "series"

// Refusalf returns a ResourceExhausted error refusing a count on limit, one of the Limit values.
func Refusalf(limit, format string, args ...any) error {
	return refusal(limit, nil, fmt.Sprintf(format, args...))
}

// MaxHashesRefusal refuses a request for hashes of more than maxHashes series. See HashesNeeded.
func MaxHashesRefusal(series, maxHashes int64) error {
	return refusal(LimitMaxHashes, map[string]string{hashesNeededKey: strconv.FormatInt(series, 10)},
		fmt.Sprintf("the request would hash %d matching series, more than max_hashes %d", series, maxHashes))
}

// MaxGroupsRefusal refuses a request with more than maxGroups distinct values
// of groupBy. holder says where they were counted, such as "the blocks hold".
func MaxGroupsRefusal(holder string, groups int64, groupBy string, maxGroups int64) error {
	return Refusalf(LimitMaxGroups, "%s %d distinct values of %s, more than max_groups %d", holder, groups, groupBy, maxGroups)
}

func refusal(limit string, metadata map[string]string, msg string) error {
	st := status.New(codes.ResourceExhausted, msg)
	with, err := st.WithDetails(&errdetails.ErrorInfo{Reason: limit, Domain: refusalDomain, Metadata: metadata})
	if err != nil {
		return st.Err()
	}
	return with.Err()
}

// RefusedBy returns the limit err's chain refuses on, or "" if it is not a refusal.
func RefusedBy(err error) string {
	if info := refusalInfo(err); info != nil {
		return info.Reason
	}
	return ""
}

// HashesNeeded returns the series count of a max_hashes refusal in err's chain.
func HashesNeeded(err error) (int64, bool) {
	info := refusalInfo(err)
	if info == nil || info.Reason != LimitMaxHashes {
		return 0, false
	}
	n, perr := strconv.ParseInt(info.Metadata[hashesNeededKey], 10, 64)
	return n, perr == nil
}

func refusalInfo(err error) *errdetails.ErrorInfo {
	if err == nil {
		return nil
	}
	//lint:ignore faillint The ErrorInfo detail is a golang/protobuf message, which a gogo status from grpcutil.ErrorToStatus can not decode.
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		return nil
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok && info.Domain == refusalDomain {
			return info
		}
	}
	return nil
}
