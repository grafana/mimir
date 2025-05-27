// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/frontend/downstream_roundtripper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package frontend

import (
	"net/http"
	"net/url"
	"path"

	"github.com/grafana/mimir/pkg/util/instrumentation"
)

// RoundTripper that forwards requests to downstream URL.
type downstreamRoundTripper struct {
	downstreamURL *url.URL
}

func NewDownstreamRoundTripper(downstreamURL string) (http.RoundTripper, error) {
	u, err := url.Parse(downstreamURL)
	if err != nil {
		return nil, err
	}

	return &instrumentation.TracerTransport{Next: downstreamRoundTripper{downstreamURL: u}}, nil
}

func (d downstreamRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.URL.Scheme = d.downstreamURL.Scheme
	r.URL.Host = d.downstreamURL.Host
	r.URL.Path = path.Join(d.downstreamURL.Path, r.URL.Path)
	r.Host = ""
	return http.DefaultTransport.RoundTrip(r)
}
