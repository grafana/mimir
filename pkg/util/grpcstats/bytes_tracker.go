// SPDX-License-Identifier: AGPL-3.0-only

package grpcstats

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/stats"
)

// NewDataTransferStatsHandler creates a stats.Handler that tracks the total number of bytes
// transferred (both sent and received) for all RPC calls on a gRPC client.
// The bytesTransferred counter will be incremented with the wire length of all payloads.
func NewDataTransferStatsHandler(bytesTransferred prometheus.Counter) stats.Handler {
	return &grpcDataTransferStatsHandler{
		bytesTransferred: bytesTransferred,
	}
}

type grpcDataTransferStatsHandler struct {
	bytesTransferred prometheus.Counter
}

func (h *grpcDataTransferStatsHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *grpcDataTransferStatsHandler) HandleRPC(_ context.Context, rpcStats stats.RPCStats) {
	switch s := rpcStats.(type) {
	case *stats.Begin:
		// Ignore begin events.
	case *stats.End:
		// Ignore end events.
	case *stats.InHeader:
		h.bytesTransferred.Add(float64(s.WireLength))
	case *stats.InPayload:
		h.bytesTransferred.Add(float64(s.WireLength))
	case *stats.InTrailer:
		h.bytesTransferred.Add(float64(s.WireLength))
	case *stats.OutHeader:
		// OutHeader does not have a WireLength field.
	case *stats.OutPayload:
		h.bytesTransferred.Add(float64(s.WireLength))
	case *stats.OutTrailer:
		// OutTrailer has WireLength field, but the field is deprecated and its value is never set.
	}
}

func (h *grpcDataTransferStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *grpcDataTransferStatsHandler) HandleConn(_ context.Context, _ stats.ConnStats) {
	// Not interested.
}
