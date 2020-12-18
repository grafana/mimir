package client

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrailingSlash(t *testing.T) {
	tc := []struct {
		name      string
		path      string
		method    string
		url       string
		resultURL string
	}{
		{
			name:      "builds the correct URL with a trailing slash",
			path:      "/api/v1/rules",
			method:    http.MethodPost,
			url:       "http://cortexurl.com/",
			resultURL: "http://cortexurl.com/api/v1/rules",
		},
		{
			name:      "builds the correct URL without a trailing slash",
			path:      "/api/v1/rules",
			method:    http.MethodPost,
			url:       "http://cortexurl.com",
			resultURL: "http://cortexurl.com/api/v1/rules",
		},
		{
			name:      "builds the correct URL when the base url has a path",
			path:      "/api/v1/rules",
			method:    http.MethodPost,
			url:       "http://cortexurl.com/apathto",
			resultURL: "http://cortexurl.com/apathto/api/v1/rules",
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			url, err := url.Parse(tt.url)
			require.NoError(t, err)

			req, err := buildRequest(tt.path, tt.method, *url, []byte{})
			require.NoError(t, err)
			require.Equal(t, tt.resultURL, req.URL.String())
		})
	}

}
