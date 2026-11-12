// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/cost"
)

// SampleQueryCost reserves the sampling RPC until bounded sampling is implemented.
func (*Ingester) SampleQueryCost(context.Context, *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SampleQueryCost not implemented")
}
