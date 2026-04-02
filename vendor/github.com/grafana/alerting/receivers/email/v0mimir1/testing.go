package v0mimir1

import (
	httpcfg "github.com/grafana/alerting/http/v0mimir"
	"github.com/grafana/alerting/receivers"
)

const FullValidConfigForTesting = `{
	"to": "team@example.com",
	"from": "alertmanager@example.com",
	"smarthost": "smtp.example.com:587",
	"auth_username": "alertmanager",
	"auth_password": "password123",
	"auth_secret": "secret-auth",
	"auth_identity": "alertmanager",
	"require_tls": true,
	"text": "test email",
	"headers": {
		"Subject": "test subject"
	},
	"tls_config": {
		"insecure_skip_verify": false,
		"server_name": "test-server-name"
	},
	"send_resolved": true
}`

// GetFullValidConfig returns a fully populated Config struct with all fields
// set to non-zero values.
func GetFullValidConfig() Config {
	requireTLS := true
	cfg := DefaultConfig
	cfg.To = "test@example.com"
	cfg.From = "sender@example.com"
	cfg.Hello = "localhost"
	cfg.Smarthost = receivers.HostPort{Host: "smtp.example.com", Port: "587"}
	cfg.AuthUsername = "user"
	cfg.AuthPassword = "secret-password"
	cfg.AuthSecret = "secret-auth"
	cfg.AuthIdentity = "identity"
	cfg.Headers = map[string]string{"X-Custom": "value"}
	cfg.HTML = "<h1>Test</h1>"
	cfg.Text = "Test"
	cfg.RequireTLS = &requireTLS
	cfg.TLSConfig = httpcfg.TLSConfig{
		InsecureSkipVerify: true,
		ServerName:         "smtp.example.com",
	}
	return cfg
}
