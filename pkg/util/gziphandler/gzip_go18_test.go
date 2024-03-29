// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/nytimes/gziphandler/blob/2f8bb1d30d9d69c8e0c3714da5a9917125a87769/gzip_go18_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Copyright 2016-2017 The New York Times Company.
//go:build go1.8

package gziphandler

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetAcceptEncodingForPushOptionsWithoutHeaders(t *testing.T) {
	var opts *http.PushOptions
	opts = setAcceptEncodingForPushOptions(opts)

	assert.NotNil(t, opts)
	assert.NotNil(t, opts.Header)

	for k, v := range opts.Header {
		assert.Equal(t, "Accept-Encoding", k)
		assert.Len(t, v, 1)
		assert.Equal(t, "gzip", v[0])
	}

	opts = &http.PushOptions{}
	opts = setAcceptEncodingForPushOptions(opts)

	assert.NotNil(t, opts)
	assert.NotNil(t, opts.Header)

	for k, v := range opts.Header {
		assert.Equal(t, "Accept-Encoding", k)
		assert.Len(t, v, 1)
		assert.Equal(t, "gzip", v[0])
	}
}

func TestSetAcceptEncodingForPushOptionsWithHeaders(t *testing.T) {
	opts := &http.PushOptions{
		Header: http.Header{
			"User-Agent": []string{"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36"},
		},
	}
	opts = setAcceptEncodingForPushOptions(opts)

	assert.NotNil(t, opts)
	assert.NotNil(t, opts.Header)

	assert.Equal(t, "gzip", opts.Header.Get("Accept-Encoding"))
	assert.Equal(t, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36", opts.Header.Get("User-Agent"))

	opts = &http.PushOptions{
		Header: http.Header{
			"User-Agent":   []string{"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36"},
			acceptEncoding: []string{"deflate"},
		},
	}
	opts = setAcceptEncodingForPushOptions(opts)

	assert.NotNil(t, opts)
	assert.NotNil(t, opts.Header)

	e, found := opts.Header["Accept-Encoding"]
	if !found {
		assert.Fail(t, "Missing Accept-Encoding header value")
	}
	assert.Len(t, e, 1)
	assert.Equal(t, "deflate", e[0])
	assert.Equal(t, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36", opts.Header.Get("User-Agent"))
}
