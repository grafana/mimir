// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/grafana/dskit/clusterutil"
	"github.com/stretchr/testify/require"
)

func TestClientClusterLabel(t *testing.T) {
	for name, tc := range map[string]struct {
		label string
		want  []string
	}{
		"set":   {label: "test-cluster", want: []string{"test-cluster"}},
		"unset": {label: "", want: nil},
	} {
		t.Run(name, func(t *testing.T) {
			var got []string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Values(clusterutil.ClusterValidationLabelHeader)
				_, _ = w.Write([]byte(`{}`))
			}))
			defer srv.Close()

			c := &client{baseURL: srv.URL, tenant: "test-tenant", http: srv.Client(), clusterLabel: tc.label}
			var out struct{}
			_, err := c.get(context.Background(), "/x", url.Values{}, false, &out)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
