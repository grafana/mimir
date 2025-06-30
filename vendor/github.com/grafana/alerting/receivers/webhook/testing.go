package webhook

import (
	"fmt"

	"github.com/grafana/alerting/http"
)

// FullValidConfigForTesting is a string representation of a JSON object that contains all fields supported by the notifier Config. It can be used without secrets.
var FullValidConfigForTesting = fmt.Sprintf(`{
	"url": "http://localhost",
	"httpMethod": "PUT",
	"maxAlerts": "2",
	"authorization_scheme": "basic",
	"authorization_credentials": "",
	"username": "test-user",
	"password": "test-pass",
	"title": "test-title",
	"message": "test-message",
	"tlsConfig": {
		"insecureSkipVerify": false,
		"clientCertificate": %q,
		"clientKey": %q,
		"caCertificate": %q
	},
	"hmacConfig": {
		"secret": "test-hmac-secret",
		"header": "X-Grafana-Alerting-Signature",
		"timestampHeader": "X-Grafana-Alerting-Timestamp"
	},
	"http_config": {
		"oauth2": {
			"client_id": "test-client-id",
			"client_secret": "test-client-secret",
			"token_url": "https://localhost/auth/token",
			"scopes": ["scope1", "scope2"],
			"endpoint_params": {
				"param1": "value1",
				"param2": "value2"
			},
			"tls_config": {
				"insecureSkipVerify": false,
				"clientCertificate": %[1]q,
				"clientKey": %[2]q,
				"caCertificate": %[3]q
			},
			"proxy_config": {
				"proxy_url": "http://localproxy:8080",
				"no_proxy": "localhost",
				"proxy_from_environment": false,
				"proxy_connect_header": {
					"X-Proxy-Header": "proxy-value"
				}
			}
		}
    }
}`, http.TestCertPem, http.TestKeyPem, http.TestCACert)

// FullValidSecretsForTesting is a string representation of JSON object that contains all fields that can be overridden from secrets
var FullValidSecretsForTesting = fmt.Sprintf(`{
	"username": "test-secret-user",
	"password": "test-secret-pass",
	"tlsConfig.clientCertificate": %q,
	"tlsConfig.clientKey": %q,
	"tlsConfig.caCertificate": %q,
	"hmacConfig.secret": "test-override-hmac-secret",
	"http_config.oauth2.client_secret": "test-override-oauth2-secret",
	"http_config.oauth2.tls_config.clientCertificate": %[1]q,
	"http_config.oauth2.tls_config.clientKey": %[2]q,
	"http_config.oauth2.tls_config.caCertificate": %[3]q
}`, http.TestCertPem, http.TestKeyPem, http.TestCACert)
