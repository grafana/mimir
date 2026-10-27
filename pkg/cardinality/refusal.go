// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"fmt"
	"strconv"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The limits a store-gateway or an ingester names when it refuses a count,
// as the querier's refusals counter and log line name them.
const (
	LimitMaxIndexBytes = "max_index_bytes"
	LimitMaxHashes     = "max_hashes"
	LimitMaxGroups     = "max_groups"
)

// refusalDomain marks the ErrorInfo detail of a refusal, so a
// ResourceExhausted status from gRPC itself, such as a message over the size
// limit, is not read as one.
const refusalDomain = "cardinality_estimate"

// hashesNeededKey is the ErrorInfo metadata key of a max_hashes refusal's
// matching series.
const hashesNeededKey = "series"

// Refusalf is the ResourceExhausted status error of a source refusing a
// count on limit, one of the Limit values, which the status carries as a
// detail so the querier reads it without parsing the message.
func Refusalf(limit, format string, args ...any) error {
	return refusal(limit, nil, fmt.Sprintf(format, args...))
}

// MaxHashesRefusal is the refusal of a request that asks for hashes when
// its matching series are more than maxHashes. The querier reads the series
// back with HashesNeeded.
func MaxHashesRefusal(series, maxHashes int64) error {
	return refusal(LimitMaxHashes, map[string]string{hashesNeededKey: strconv.FormatInt(series, 10)},
		fmt.Sprintf("the request would hash %d matching series, more than max_hashes %d", series, maxHashes))
}

func refusal(limit string, metadata map[string]string, msg string) error {
	st := status.New(codes.ResourceExhausted, msg)
	with, err := st.WithDetails(&errdetails.ErrorInfo{Reason: limit, Domain: refusalDomain, Metadata: metadata})
	if err != nil {
		return st.Err()
	}
	return with.Err()
}

// RefusedBy is the limit that the status in err's chain names, or "" when
// err is not a source's refusal.
func RefusedBy(err error) string {
	if info := refusalInfo(err); info != nil {
		return info.Reason
	}
	return ""
}

// HashesNeeded is the matching series a max_hashes refusal in err's chain
// names.
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
