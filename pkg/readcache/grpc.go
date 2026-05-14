// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// The Readcache gRPC surface is the read subset of the
// client.IngesterServer interface plus the nautilus-specific
// HashRangeStats / SetHashRanges / GetHashRanges RPCs.
//
// Push, NotifyPreCommit, etc. exist as stubs returning Unimplemented
// so the same client.IngesterClient stubs work transparently against
// either an ingester or a readcache instance during the Phase 2C
// rollout.

// ensureIngesterServer is a compile-time check that *Readcache
// satisfies the ingester gRPC service interface.
var _ client.IngesterServer = (*Readcache)(nil)

// Push is not implemented by readcache: it is read-only.
func (r *Readcache) Push(_ context.Context, _ *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	return nil, status.Error(codes.Unimplemented, "readcache is read-only; pushes are served by the ingester")
}

// The read RPCs are implemented in pkg/readcache/handlers.go; the
// stubs below are placeholders that compile while the handlers are
// fleshed out in follow-up patches.

// QueryStream implements client.IngesterServer. See handlers.go.
func (r *Readcache) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	return r.queryStream(req, stream)
}

// QueryExemplars implements client.IngesterServer. See handlers.go.
func (r *Readcache) QueryExemplars(ctx context.Context, req *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	return r.queryExemplars(ctx, req)
}

// LabelValues implements client.IngesterServer.
func (r *Readcache) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	return r.labelValues(ctx, req)
}

// LabelNames implements client.IngesterServer.
func (r *Readcache) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	return r.labelNames(ctx, req)
}

// UserStats implements client.IngesterServer.
func (r *Readcache) UserStats(ctx context.Context, req *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	return r.userStats(ctx, req)
}

// AllUserStats implements client.IngesterServer.
func (r *Readcache) AllUserStats(ctx context.Context, req *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	return r.allUserStats(ctx, req)
}

// MetricsForLabelMatchers implements client.IngesterServer.
func (r *Readcache) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	return r.metricsForLabelMatchers(ctx, req)
}

// MetricsMetadata implements client.IngesterServer.
func (r *Readcache) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	// Metadata is a write-path concept (the producer publishes it).
	// Readcache does not host metadata; surface an empty response.
	_ = ctx
	_ = req
	return &client.MetricsMetadataResponse{}, nil
}

// LabelNamesAndValues implements client.IngesterServer.
func (r *Readcache) LabelNamesAndValues(req *client.LabelNamesAndValuesRequest, srv client.Ingester_LabelNamesAndValuesServer) error {
	return r.labelNamesAndValues(req, srv)
}

// LabelValuesCardinality implements client.IngesterServer.
func (r *Readcache) LabelValuesCardinality(req *client.LabelValuesCardinalityRequest, srv client.Ingester_LabelValuesCardinalityServer) error {
	return r.labelValuesCardinality(req, srv)
}

// ActiveSeries implements client.IngesterServer.
func (r *Readcache) ActiveSeries(req *client.ActiveSeriesRequest, srv client.Ingester_ActiveSeriesServer) error {
	return r.activeSeries(req, srv)
}

// HashRangeStats implements client.IngesterServer. Readcache is the
// authoritative producer of this RPC under the Phase 2 plan.
func (r *Readcache) HashRangeStats(ctx context.Context, req *client.HashRangeStatsRequest) (*client.HashRangeStatsResponse, error) {
	return r.hashRangeStats(ctx, req)
}

// SetHashRanges implements client.IngesterServer.
func (r *Readcache) SetHashRanges(ctx context.Context, req *client.SetHashRangesRequest) (*client.SetHashRangesResponse, error) {
	return r.setHashRanges(ctx, req)
}

// GetHashRanges implements client.IngesterServer.
func (r *Readcache) GetHashRanges(ctx context.Context, req *client.GetHashRangesRequest) (*client.GetHashRangesResponse, error) {
	return r.getHashRanges(ctx, req)
}
