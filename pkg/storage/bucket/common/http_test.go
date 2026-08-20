// SPDX-License-Identifier: AGPL-3.0-only

package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/exthttp"
)

// TestHTTPConfig_ToExtHTTP_DefaultTransport makes sure that the Mimir HTTP
// options reach the http.Transport that objstore builds.
func TestHTTPConfig_ToExtHTTP_DefaultTransport(t *testing.T) {
	cfg := HTTPConfig{
		IdleConnTimeout:       time.Minute,
		ResponseHeaderTimeout: 2 * time.Minute,
		InsecureSkipVerify:    true,
		TLSHandshakeTimeout:   3 * time.Minute,
		ExpectContinueTimeout: 4 * time.Minute,
		MaxIdleConns:          10,
		MaxIdleConnsPerHost:   20,
		MaxConnsPerHost:       30,
		ForceAttemptHTTP2:     true,
		TLSConfig: TLSConfig{
			ServerName: "server",
		},
	}

	transport, err := exthttp.DefaultTransport(cfg.ToExtHTTP())
	require.NoError(t, err)

	require.Equal(t, cfg.IdleConnTimeout, transport.IdleConnTimeout)
	require.Equal(t, cfg.ResponseHeaderTimeout, transport.ResponseHeaderTimeout)
	require.Equal(t, cfg.TLSHandshakeTimeout, transport.TLSHandshakeTimeout)
	require.Equal(t, cfg.ExpectContinueTimeout, transport.ExpectContinueTimeout)
	require.Equal(t, cfg.MaxIdleConns, transport.MaxIdleConns)
	require.Equal(t, cfg.MaxIdleConnsPerHost, transport.MaxIdleConnsPerHost)
	require.Equal(t, cfg.MaxConnsPerHost, transport.MaxConnsPerHost)
	require.Equal(t, cfg.ForceAttemptHTTP2, transport.ForceAttemptHTTP2)
	require.Equal(t, cfg.InsecureSkipVerify, transport.TLSClientConfig.InsecureSkipVerify)
	require.Equal(t, cfg.TLSConfig.ServerName, transport.TLSClientConfig.ServerName)
}
