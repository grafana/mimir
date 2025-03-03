package http

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// NewTLSClient creates a new HTTP client with the provided TLS configuration or with default settings.
func NewTLSClient(tlsConfig *tls.Config) *http.Client {
	nc := func(tlsConfig *tls.Config) *http.Client {
		return &http.Client{
			Timeout: time.Second * 30,
			Transport: &http.Transport{
				TLSClientConfig: tlsConfig,
				Proxy:           http.ProxyFromEnvironment,
				Dial: (&net.Dialer{
					Timeout: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout: 5 * time.Second,
			},
		}
	}

	if tlsConfig == nil {
		return nc(&tls.Config{Renegotiation: tls.RenegotiateFreelyAsClient})
	}

	return nc(tlsConfig)
}
