package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/net/proxy"
)

var (
	// PluginSecureSocksProxyEnabled is a constant for the GF_SECURE_SOCKS_DATASOURCE_PROXY_SERVER_ENABLED
	// environment variable used to specify if a secure socks proxy is allowed to be used for datasource connections.
	PluginSecureSocksProxyEnabled = "GF_SECURE_SOCKS_DATASOURCE_PROXY_SERVER_ENABLED"
	// PluginSecureSocksProxyClientCert is a constant for the GF_SECURE_SOCKS_DATASOURCE_PROXY_CLIENT_CERT
	// environment variable used to specify the file location of the client cert for the secure socks proxy.
	PluginSecureSocksProxyClientCert = "GF_SECURE_SOCKS_DATASOURCE_PROXY_CLIENT_CERT"
	// PluginSecureSocksProxyClientKey is a constant for the GF_SECURE_SOCKS_DATASOURCE_PROXY_CLIENT_KEY
	// environment variable used to specify the file location of the client key for the secure socks proxy.
	PluginSecureSocksProxyClientKey = "GF_SECURE_SOCKS_DATASOURCE_PROXY_CLIENT_KEY"
	// PluginSecureSocksProxyRootCACert is a constant for the GF_SECURE_SOCKS_DATASOURCE_PROXY_ROOT_CA_CERT
	// environment variable used to specify the file location of the root ca for the secure socks proxy.
	PluginSecureSocksProxyRootCACert = "GF_SECURE_SOCKS_DATASOURCE_PROXY_ROOT_CA_CERT"
	// PluginSecureSocksProxyProxyAddress is a constant for the GF_SECURE_SOCKS_DATASOURCE_PROXY_PROXY_ADDRESS
	// environment variable used to specify the secure socks proxy server address to proxy the connections to.
	PluginSecureSocksProxyProxyAddress = "GF_SECURE_SOCKS_DATASOURCE_PROXY_PROXY_ADDRESS"
	// PluginSecureSocksProxyServerName is a constant for the GF_SECURE_SOCKS_DATASOURCE_PROXY_SERVER_NAME
	// environment variable used to specify the server name of the secure socks proxy.
	PluginSecureSocksProxyServerName = "GF_SECURE_SOCKS_DATASOURCE_PROXY_SERVER_NAME"
	// PluginSecureSocksProxyAllowInsecure is a constant for the GF_SECURE_SOCKS_DATASOURCE_PROXY_ALLOW_INSECURE
	// environment variable used to specify if the proxy should use a TLS dialer.
	PluginSecureSocksProxyAllowInsecure = "GF_SECURE_SOCKS_DATASOURCE_PROXY_ALLOW_INSECURE"
)

var (
	socksUnknownError           = regexp.MustCompile(`unknown code: (\d+)`)
	secureSocksRequestsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "grafana",
		Name:      "secure_socks_requests_duration",
		Help:      "Duration of requests to the secure socks proxy",
	}, []string{"code"})
)

// Client is the main Proxy Client interface.
type Client interface {
	SecureSocksProxyEnabled() bool
	ConfigureSecureSocksHTTPProxy(transport *http.Transport) error
	NewSecureSocksProxyContextDialer() (proxy.Dialer, error)
}

// ClientCfg contains the information needed to allow datasource connections to be
// proxied to a secure socks proxy.
type ClientCfg struct {
	ClientCert    string
	ClientKey     string
	RootCA        string
	ProxyAddress  string
	ServerName    string
	AllowInsecure bool
}

// New creates a new proxy client from a given config.
func New(opts *Options) Client {
	return &cfgProxyWrapper{
		opts: opts,
	}
}

type cfgProxyWrapper struct {
	opts *Options
}

// SecureSocksProxyEnabled checks if the Grafana instance allows the secure socks proxy to be used
// and the datasource options specify to use the proxy
func (p *cfgProxyWrapper) SecureSocksProxyEnabled() bool {
	// it cannot be enabled if it's not enabled on Grafana
	if p.opts == nil {
		return false
	}

	// if it's enabled on Grafana, check if the datasource is using it
	return (p.opts != nil) && p.opts.Enabled
}

// ConfigureSecureSocksHTTPProxy takes a http.DefaultTransport and wraps it in a socks5 proxy with TLS
// if it is enabled on the datasource and the grafana instance
func (p *cfgProxyWrapper) ConfigureSecureSocksHTTPProxy(transport *http.Transport) error {
	if !p.SecureSocksProxyEnabled() {
		return nil
	}

	dialSocksProxy, err := p.NewSecureSocksProxyContextDialer()
	if err != nil {
		return err
	}

	contextDialer, ok := dialSocksProxy.(proxy.ContextDialer)
	if !ok {
		return errors.New("unable to cast socks proxy dialer to context proxy dialer")
	}

	transport.DialContext = contextDialer.DialContext
	return nil
}

// NewSecureSocksProxyContextDialer returns a proxy context dialer that can be used to allow datasource connections to go through a secure socks proxy
func (p *cfgProxyWrapper) NewSecureSocksProxyContextDialer() (proxy.Dialer, error) {
	p.opts.setDefaults()

	if !p.SecureSocksProxyEnabled() {
		return nil, errors.New("proxy not enabled")
	}

	var dialer proxy.Dialer

	if p.opts.ClientCfg.AllowInsecure {
		dialer = &net.Dialer{
			Timeout:   p.opts.Timeouts.Timeout,
			KeepAlive: p.opts.Timeouts.KeepAlive,
		}
	} else {
		d, err := p.getTLSDialer()
		if err != nil {
			return nil, fmt.Errorf("instantiating tls dialer: %w", err)
		}
		dialer = d
	}

	var auth *proxy.Auth
	if p.opts.Auth != nil {
		auth = &proxy.Auth{
			User:     p.opts.Auth.Username,
			Password: p.opts.Auth.Password,
		}
	}

	dialSocksProxy, err := proxy.SOCKS5("tcp", p.opts.ClientCfg.ProxyAddress, auth, dialer)
	if err != nil {
		return nil, err
	}

	return newInstrumentedSocksDialer(dialSocksProxy), nil
}

func (p *cfgProxyWrapper) getTLSDialer() (*tls.Dialer, error) {
	certPool := x509.NewCertPool()
	for _, rootCAFile := range strings.Split(p.opts.ClientCfg.RootCA, " ") {
		// nolint:gosec
		// The gosec G304 warning can be ignored because `rootCAFile` comes from config ini
		// and we check below if it's the right file type
		pemBytes, err := os.ReadFile(rootCAFile)
		if err != nil {
			return nil, err
		}

		pemDecoded, _ := pem.Decode(pemBytes)
		if pemDecoded == nil || pemDecoded.Type != "CERTIFICATE" {
			return nil, errors.New("root ca is invalid")
		}

		if !certPool.AppendCertsFromPEM(pemBytes) {
			return nil, errors.New("failed to append CA certificate " + rootCAFile)
		}
	}

	cert, err := tls.LoadX509KeyPair(p.opts.ClientCfg.ClientCert, p.opts.ClientCfg.ClientKey)
	if err != nil {
		return nil, err
	}

	return &tls.Dialer{
		Config: &tls.Config{
			Certificates: []tls.Certificate{cert},
			ServerName:   p.opts.ClientCfg.ServerName,
			RootCAs:      certPool,
			MinVersion:   tls.VersionTLS13,
		},
		NetDialer: &net.Dialer{
			Timeout:   p.opts.Timeouts.Timeout,
			KeepAlive: p.opts.Timeouts.KeepAlive,
		},
	}, nil
}

// getConfigFromEnv gets the needed proxy information from the env variables that Grafana set with the values from the config ini
func getConfigFromEnv() *ClientCfg {
	if value, ok := os.LookupEnv(PluginSecureSocksProxyEnabled); ok {
		enabled, err := strconv.ParseBool(value)
		if err != nil || !enabled {
			return nil
		}
	}

	proxyAddress := ""
	if value, ok := os.LookupEnv(PluginSecureSocksProxyProxyAddress); ok {
		proxyAddress = value
	} else {
		return nil
	}

	allowInsecure := false
	if value, ok := os.LookupEnv(PluginSecureSocksProxyAllowInsecure); ok {
		allowInsecure, _ = strconv.ParseBool(value)
	}

	// We only need to fill these fields on insecure mode.
	if allowInsecure {
		return &ClientCfg{
			ProxyAddress:  proxyAddress,
			AllowInsecure: allowInsecure,
		}
	}

	clientCert := ""
	if value, ok := os.LookupEnv(PluginSecureSocksProxyClientCert); ok {
		clientCert = value
	} else {
		return nil
	}

	clientKey := ""
	if value, ok := os.LookupEnv(PluginSecureSocksProxyClientKey); ok {
		clientKey = value
	} else {
		return nil
	}

	rootCA := ""
	if value, ok := os.LookupEnv(PluginSecureSocksProxyRootCACert); ok {
		rootCA = value
	} else {
		return nil
	}

	serverName := ""
	if value, ok := os.LookupEnv(PluginSecureSocksProxyServerName); ok {
		serverName = value
	} else {
		return nil
	}

	return &ClientCfg{
		ClientCert:    clientCert,
		ClientKey:     clientKey,
		RootCA:        rootCA,
		ProxyAddress:  proxyAddress,
		ServerName:    serverName,
		AllowInsecure: false,
	}
}

// SecureSocksProxyEnabledOnDS checks the datasource json data for `enableSecureSocksProxy`
// to determine if the secure socks proxy should be enabled on it
func SecureSocksProxyEnabledOnDS(jsonData map[string]interface{}) bool {
	res, enabled := jsonData["enableSecureSocksProxy"]
	if !enabled {
		return false
	}

	if val, ok := res.(bool); ok {
		return val
	}

	return false
}

// instrumentedSocksDialer  is a wrapper around the proxy.Dialer and proxy.DialContext
// that records relevant socks secure socks proxy.
type instrumentedSocksDialer struct {
	dialer proxy.Dialer
}

// newInstrumentedSocksDialer creates a new instrumented dialer
func newInstrumentedSocksDialer(dialer proxy.Dialer) proxy.Dialer {
	return &instrumentedSocksDialer{
		dialer: dialer,
	}
}

// Dial -
func (d *instrumentedSocksDialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

// DialContext -
func (d *instrumentedSocksDialer) DialContext(ctx context.Context, n, addr string) (net.Conn, error) {
	start := time.Now()
	dialer, ok := d.dialer.(proxy.ContextDialer)
	if !ok {
		return nil, errors.New("unable to cast socks proxy dialer to context proxy dialer")
	}
	c, err := dialer.DialContext(ctx, n, addr)

	var code string
	var oppErr *net.OpError

	switch {
	case err == nil:
		code = "0"
	case errors.As(err, &oppErr):
		unknownCode := socksUnknownError.FindStringSubmatch(err.Error())

		// Socks errors defined here: https://cs.opensource.google/go/x/net/+/refs/tags/v0.15.0:internal/socks/socks.go;l=40-63
		switch {
		case strings.Contains(err.Error(), "general SOCKS server failure"):
			code = "1"
		case strings.Contains(err.Error(), "connection not allowed by ruleset"):
			code = "2"
		case strings.Contains(err.Error(), "network unreachable"):
			code = "3"
		case strings.Contains(err.Error(), "host unreachable"):
			code = "4"
		case strings.Contains(err.Error(), "connection refused"):
			code = "5"
		case strings.Contains(err.Error(), "TTL expired"):
			code = "6"
		case strings.Contains(err.Error(), "command not supported"):
			code = "7"
		case strings.Contains(err.Error(), "address type not supported"):
			code = "8"
		case strings.HasSuffix(err.Error(), "EOF"):
			code = "eof_error"
		case strings.HasSuffix(err.Error(), "i/o timeout"):
			code = "io_timeout_error"
		case strings.HasSuffix(err.Error(), "context canceled"):
			code = "context_canceled_error"
		case len(unknownCode) > 1:
			code = unknownCode[1]
		default:
			code = "socks_unknown_error"
		}
		log.DefaultLogger.Error("received oppErr from dialer", "network", n, "addr", addr, "oppErr", oppErr, "code", code)
	default:
		log.DefaultLogger.Error("received err from dialer", "network", n, "addr", addr, "err", err)
		code = "dial_error"
	}

	secureSocksRequestsDuration.WithLabelValues(code).Observe(time.Since(start).Seconds())
	return c, err
}
