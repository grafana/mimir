// SPDX-License-Identifier: AGPL-3.0-only

package common

import (
	"flag"
	"net/http"
	"time"

	"github.com/prometheus/common/model"
	"github.com/thanos-io/objstore/exthttp"
)

// HTTPConfig stores the http.Transport configuration for an object storage client
type HTTPConfig struct {
	IdleConnTimeout       time.Duration `yaml:"idle_conn_timeout" category:"advanced"`
	ResponseHeaderTimeout time.Duration `yaml:"response_header_timeout" category:"advanced"`
	InsecureSkipVerify    bool          `yaml:"insecure_skip_verify" category:"advanced"`
	TLSHandshakeTimeout   time.Duration `yaml:"tls_handshake_timeout" category:"advanced"`
	ExpectContinueTimeout time.Duration `yaml:"expect_continue_timeout" category:"advanced"`
	MaxIdleConns          int           `yaml:"max_idle_connections" category:"advanced"`
	MaxIdleConnsPerHost   int           `yaml:"max_idle_connections_per_host" category:"advanced"`
	MaxConnsPerHost       int           `yaml:"max_connections_per_host" category:"advanced"`

	// Allow upstream callers to inject a round tripper
	Transport http.RoundTripper `yaml:"-"`

	TLSConfig TLSConfig `yaml:",inline"`
}

func (cfg *HTTPConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.IdleConnTimeout, prefix+"http.idle-conn-timeout", 90*time.Second, "The time an idle connection remains idle before closing.")
	f.DurationVar(&cfg.ResponseHeaderTimeout, prefix+"http.response-header-timeout", 2*time.Minute, "The amount of time the client waits for a server's response headers.")
	f.BoolVar(&cfg.InsecureSkipVerify, prefix+"http.insecure-skip-verify", false, "If the client connects to object storage via HTTPS and this option is enabled, the client accepts any certificate and hostname.")
	f.DurationVar(&cfg.TLSHandshakeTimeout, prefix+"tls-handshake-timeout", 10*time.Second, "Maximum time to wait for a TLS handshake. Set to 0 for no limit.")
	f.DurationVar(&cfg.ExpectContinueTimeout, prefix+"expect-continue-timeout", 1*time.Second, "The time to wait for a server's first response headers after fully writing the request headers if the request has an Expect header. Set to 0 to send the request body immediately.")
	f.IntVar(&cfg.MaxIdleConns, prefix+"max-idle-connections", 100, "Maximum number of idle (keep-alive) connections across all hosts. Set to 0 for no limit.")
	f.IntVar(&cfg.MaxIdleConnsPerHost, prefix+"max-idle-connections-per-host", 100, "Maximum number of idle (keep-alive) connections to keep per-host. Set to 0 to use a built-in default value.")
	f.IntVar(&cfg.MaxConnsPerHost, prefix+"max-connections-per-host", 0, "Maximum number of connections per host. Set to 0 for no limit.")
	cfg.TLSConfig.RegisterFlagsWithPrefix(prefix, f)
}

func (cfg *HTTPConfig) ToExtHTTP() exthttp.HTTPConfig {
	return exthttp.HTTPConfig{
		IdleConnTimeout:       model.Duration(cfg.IdleConnTimeout),
		ResponseHeaderTimeout: model.Duration(cfg.ResponseHeaderTimeout),
		InsecureSkipVerify:    cfg.InsecureSkipVerify,
		TLSHandshakeTimeout:   model.Duration(cfg.TLSHandshakeTimeout),
		ExpectContinueTimeout: model.Duration(cfg.ExpectContinueTimeout),
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		Transport:             cfg.Transport,
		TLSConfig:             cfg.TLSConfig.ToExtHTTP(),
	}
}

// TLSConfig configures the options for TLS connections.
type TLSConfig struct {
	CAPath     string `yaml:"tls_ca_path" category:"advanced"`
	CertPath   string `yaml:"tls_cert_path" category:"advanced"`
	KeyPath    string `yaml:"tls_key_path" category:"advanced"`
	ServerName string `yaml:"tls_server_name" category:"advanced"`
}

func (cfg *TLSConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.CAPath, prefix+"http.tls-ca-path", "", "Path to the Certificate Authority (CA) certificates to validate the server certificate. If not set, the host's root CA certificates are used.")
	f.StringVar(&cfg.CertPath, prefix+"http.tls-cert-path", "", "Path to the client certificate, which is used for authenticating with the server. This setting also requires you to configure the key path.")
	f.StringVar(&cfg.KeyPath, prefix+"http.tls-key-path", "", "Path to the key for the client certificate. This setting also requires you to configure the client certificate.")
	f.StringVar(&cfg.ServerName, prefix+"http.tls-server-name", "", "Override the expected name on the server certificate.")
}

func (cfg *TLSConfig) ToExtHTTP() exthttp.TLSConfig {
	return exthttp.TLSConfig{
		CAFile:     cfg.CAPath,
		CertFile:   cfg.CertPath,
		KeyFile:    cfg.KeyPath,
		ServerName: cfg.ServerName,
	}
}
