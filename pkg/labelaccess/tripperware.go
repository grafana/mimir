// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"net/http"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
)

const (
	contextKeyLabelPolicyHash key = 1
)

type tripperwareWrapper struct {
	next http.RoundTripper
}

// WrapTripperware takes the provided tripperware and injects the label policy hash in the context
// before passing the request to the wrapped tripperware.
func WrapTripperware(qrTrw querymiddleware.Tripperware) querymiddleware.Tripperware {
	return func(next http.RoundTripper) http.RoundTripper {
		// Call chain for RoundTrip() is:
		// tripperwareWrapper.RoundTrip -> querymiddleware -> next
		lbacTrw := &tripperwareWrapper{
			next: qrTrw(next),
		}
		return lbacTrw
	}
}

func (t *tripperwareWrapper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()
	labelPolicySet, err := ExtractLabelMatchersContext(ctx)
	// If there are no policies in the context, that's fine - just pass through without adding hash.
	if err != nil && err != errNoMatcherSource {
		return nil, err
	}

	// If the instance policies are set on the query, then inject the hash of the policy set,
	// so that CacheKeyGenerator can use it to cache results for the tenant differently with and without the policy.
	if len(labelPolicySet) != 0 {
		ctx = context.WithValue(ctx, contextKeyLabelPolicyHash, labelPolicySet.Hash())
		r = r.Clone(ctx)
	}

	return t.next.RoundTrip(r)
}
