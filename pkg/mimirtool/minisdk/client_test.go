// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana-tools/sdk/blob/master/rest-dashboard_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2016-2019 The Grafana SDK authors

package minisdk_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
)

func TestClient_Search(t *testing.T) {
	tests := []struct {
		name string
		in   []minisdk.SearchParam
		out  url.Values
	}{
		{
			name: "no params",
			in:   nil,
			out:  url.Values{},
		},
		{
			name: "type and page",
			in: []minisdk.SearchParam{
				minisdk.SearchType(minisdk.SearchTypeDashboard),
				minisdk.SearchPage(3),
			},
			out: url.Values{
				"type": []string{string(minisdk.SearchTypeDashboard)},
				"page": []string{"3"},
			},
		},
		{
			name: "last value wins for non-repeatable params",
			in: []minisdk.SearchParam{
				minisdk.SearchPage(1),
				minisdk.SearchPage(2),
				minisdk.SearchType(minisdk.SearchTypeDashboard),
			},
			out: url.Values{
				"type": []string{string(minisdk.SearchTypeDashboard)},
				"page": []string{"2"},
			},
		},
		{
			name: "zero page is ignored",
			in: []minisdk.SearchParam{
				minisdk.SearchPage(0),
			},
			out: url.Values{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodGet, r.Method)
				require.Equal(t, "/api/search", r.URL.Path)
				gotQuery, err := url.ParseQuery(r.URL.RawQuery)
				require.NoError(t, err)
				require.Equal(t, tc.out, gotQuery)
				_, err = w.Write([]byte(`[{"uid":"u","title":"T","folderTitle":"F"}]`))
				require.NoError(t, err)
			}))
			defer ts.Close()

			c, err := minisdk.NewClient(ts.URL, "", ts.Client(), "")
			require.NoError(t, err)

			got, err := c.Search(context.Background(), tc.in...)
			require.NoError(t, err)
			require.Equal(t, []minisdk.FoundBoard{{UID: "u", Title: "T", FolderTitle: "F"}}, got)
		})
	}
}

func TestClient_Search_NonOKStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer ts.Close()

	c, err := minisdk.NewClient(ts.URL, "", ts.Client(), "")
	require.NoError(t, err)

	_, err = c.Search(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP error 500")
}

func TestClient_GetRawDashboardByUID(t *testing.T) {
	const board = `{"uid":"abc","title":"My Board"}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/api/dashboards/uid/abc", r.URL.Path)
		_, err := w.Write([]byte(`{"meta":{"isStarred":true},"dashboard":` + board + `}`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c, err := minisdk.NewClient(ts.URL, "", ts.Client(), "")
	require.NoError(t, err)

	got, err := c.GetRawDashboardByUID(context.Background(), "abc")
	require.NoError(t, err)
	assert.JSONEq(t, board, string(got))
}

func TestClient_GetRawDashboardByUID_NonOKStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer ts.Close()

	c, err := minisdk.NewClient(ts.URL, "", ts.Client(), "")
	require.NoError(t, err)

	_, err = c.GetRawDashboardByUID(context.Background(), "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HTTP error 404")
}

func TestNewClient_Auth(t *testing.T) {
	tests := []struct {
		name              string
		credentials       string
		expectAuthHeader  string
		expectBasicHeader bool
	}{
		{
			name:             "no credentials",
			credentials:      "",
			expectAuthHeader: "",
		},
		{
			name:             "API key produces bearer header",
			credentials:      "my-api-key",
			expectAuthHeader: "Bearer my-api-key",
		},
		{
			name:              "user:pass produces basic auth",
			credentials:       "alice:s3cret",
			expectBasicHeader: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.expectBasicHeader {
					user, pass, ok := r.BasicAuth()
					require.True(t, ok, "expected basic auth header")
					assert.Equal(t, "alice", user)
					assert.Equal(t, "s3cret", pass)
					// And no explicit bearer set.
					assert.Empty(t, r.Header.Get("Authorization-Bearer"))
				} else {
					assert.Equal(t, tc.expectAuthHeader, r.Header.Get("Authorization"))
				}
				_, err := w.Write([]byte(`[]`))
				require.NoError(t, err)
			}))
			defer ts.Close()

			c, err := minisdk.NewClient(ts.URL, tc.credentials, ts.Client(), "")
			require.NoError(t, err)
			_, err = c.Search(context.Background())
			require.NoError(t, err)
		})
	}
}
