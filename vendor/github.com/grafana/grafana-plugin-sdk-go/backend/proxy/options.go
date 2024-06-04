package proxy

import "time"

// Options defines per datasource options for creating the proxy dialer.
type Options struct {
	Enabled   bool
	Auth      *AuthOptions
	Timeouts  *TimeoutOptions
	ClientCfg *ClientCfg
}

// AuthOptions socks5 username and password options.
// Every datasource can have separate credentials to the proxy.
type AuthOptions struct {
	Username string
	Password string
}

// TimeoutOptions timeout/connection options.
type TimeoutOptions struct {
	Timeout   time.Duration
	KeepAlive time.Duration
}

// DefaultTimeoutOptions default timeout/connection options for the proxy.
var DefaultTimeoutOptions = TimeoutOptions{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}

func (o *Options) setDefaults() {
	if o == nil {
		return
	}

	if o.Timeouts == nil {
		o.Timeouts = &DefaultTimeoutOptions
	}

	if o.ClientCfg == nil {
		o.ClientCfg = getConfigFromEnv()
	}
}
