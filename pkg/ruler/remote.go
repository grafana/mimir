// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"net/textproto"

	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"
)

// WithOrgIDHeader attaches orgID header by inspecting the passed context.
func WithOrgIDHeader(ctx context.Context, req *httpgrpc.HTTPRequest) error {
	orgID, err := ExtractTenantIDs(ctx)
	if err != nil {
		return err
	}
	req.Headers = append(req.Headers, &httpgrpc.Header{
		Key:    textproto.CanonicalMIMEHeaderKey(user.OrgIDHeaderName),
		Values: []string{orgID},
	})
	return nil
}
