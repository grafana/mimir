// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"github.com/grafana/dskit/grpcutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stillWarmingDetail is the message-detail string the readcache puts
// on `status.Error(codes.Unavailable, …)` responses while a partition
// is still warming its head from Kafka. The distributor checks for
// this string to decide whether to fall back to the previous lease
// owner of the partition (rather than treating it as a generic
// 503 / unavailable result).
//
// Why a magic string rather than a richer status.Details payload:
// the ingester gRPC service descriptor is shared with vanilla
// ingesters, and adding a new proto-defined detail type would mean
// regenerating the public ingester service for a behaviour that is
// only meaningful for readcache pods. Using a sentinel message keeps
// the protobuf surface untouched.
const stillWarmingDetail = "readcache:still_warming"

// partitionEpochUnavailableDetail is returned when the distributor
// routes a partition-hinted query to this pod but the pod has neither
// a live nor a frozen epoch for that partition. Returning an empty
// series set in that case would silently under-count (the assignment
// log still names this pod as an owner for the window). Fail-fast
// matches the still_warming policy.
const partitionEpochUnavailableDetail = "readcache:partition_epoch_unavailable"

// errStillWarming returns the typed gRPC error a readcache RPC
// handler returns when a queried partition's head has not yet caught
// up to its starting Kafka offset. Distributor read-routing reacts
// to this error specifically by trying the partition's previous
// lease owner.
func errStillWarming(partitionID int32) error {
	return status.Errorf(codes.Unavailable, "%s partition=%d", stillWarmingDetail, partitionID)
}

// errPartitionEpochUnavailable is returned when a partition-hinted
// read finds no live partition and no frozen epoch on this pod.
func errPartitionEpochUnavailable(partitionID int32) error {
	return status.Errorf(codes.Unavailable, "%s partition=%d", partitionEpochUnavailableDetail, partitionID)
}

// IsStillWarming reports whether err is a "still warming" response
// returned by a readcache pod (as produced by errStillWarming). The
// match is on the gRPC status message rather than the type so the
// detection survives transport-layer wrapping.
func IsStillWarming(err error) bool {
	if err == nil {
		return false
	}
	st, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		return false
	}
	if st.Code() != codes.Unavailable {
		return false
	}
	return len(st.Message()) >= len(stillWarmingDetail) &&
		st.Message()[:len(stillWarmingDetail)] == stillWarmingDetail
}
