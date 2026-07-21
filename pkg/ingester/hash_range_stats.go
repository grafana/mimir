// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// The nautilus HashRangeStats / SetHashRanges / GetHashRanges RPCs are
// not implemented by the ingester. They exist on the Ingester gRPC
// service for wire-compatibility with older builds; the contract is
// served by pkg/readcache instead (see the readcache phase 2 plan).
// Calling these against an ingester returns Unimplemented so callers
// distinguish "endpoint exists but is unused" from a transport error.

var errNautilusUnimplemented = status.Error(codes.Unimplemented, "nautilus is served by readcache, not the ingester")

func (i *Ingester) HashRangeStats(_ context.Context, _ *client.HashRangeStatsRequest) (*client.HashRangeStatsResponse, error) {
	return nil, errNautilusUnimplemented
}

func (i *Ingester) SetHashRanges(_ context.Context, _ *client.SetHashRangesRequest) (*client.SetHashRangesResponse, error) {
	return nil, errNautilusUnimplemented
}

func (i *Ingester) GetHashRanges(_ context.Context, _ *client.GetHashRangesRequest) (*client.GetHashRangesResponse, error) {
	return nil, errNautilusUnimplemented
}
