// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// SearchLabelNames streams label names matching the search filter.
// Full implementation arrives in a later PR.
func (i *Ingester) SearchLabelNames(_ *client.SearchLabelNamesRequest, _ client.Ingester_SearchLabelNamesServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchLabelNames not implemented")
}

// SearchLabelValues streams label values for the given label name matching the search filter.
// Full implementation arrives in a later PR.
func (i *Ingester) SearchLabelValues(_ *client.SearchLabelValuesRequest, _ client.Ingester_SearchLabelValuesServer) error {
	return status.Errorf(codes.Unimplemented, "method SearchLabelValues not implemented")
}
