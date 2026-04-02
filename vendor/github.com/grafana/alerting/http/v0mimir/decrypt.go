package v0mimir

import (
	"encoding/json"
	"fmt"

	commoncfg "github.com/prometheus/common/config"

	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/schema"
)

// DecryptHTTPConfig populates secret fields of an HTTPClientConfig from the
// decrypt function. Fields are only set when the decrypted value is non-empty.
func DecryptHTTPConfig(prefix string, cfg *HTTPClientConfig, decrypt receivers.DecryptFunc) (*HTTPClientConfig, error) {
	if cfg == nil {
		def := DefaultHTTPClientConfig
		cfg = &def
	}
	p := schema.NewIntegrationFieldPath
	if secret, ok := decrypt.GetPath(p(prefix, "basic_auth", "password")); ok {
		if cfg.BasicAuth == nil {
			cfg.BasicAuth = &BasicAuth{}
		}
		cfg.BasicAuth.Password = commoncfg.Secret(secret)
	}
	if secret, ok := decrypt.GetPath(p(prefix, "authorization", "credentials")); ok {
		if cfg.Authorization == nil {
			cfg.Authorization = &Authorization{}
		}
		cfg.Authorization.Credentials = commoncfg.Secret(secret)
	}
	if secret, ok := decrypt.GetPath(p(prefix, "oauth2", "client_secret")); ok {
		if cfg.OAuth2 == nil {
			cfg.OAuth2 = &OAuth2{}
		}
		cfg.OAuth2.ClientSecret = commoncfg.Secret(secret)
	}
	if secret, ok := decrypt.GetPath(p(prefix, "bearer_token")); ok {
		cfg.BearerToken = commoncfg.Secret(secret)
	}
	if secret, ok := decrypt.GetPath(p(prefix, "tls_config", "key")); ok {
		cfg.TLSConfig.Key = commoncfg.Secret(secret)
	}
	if secret, ok := decrypt.GetPath(p(prefix, "oauth2", "tls_config", "key")); ok {
		if cfg.OAuth2 == nil {
			cfg.OAuth2 = &OAuth2{}
		}
		cfg.OAuth2.TLSConfig.Key = commoncfg.Secret(secret)
	}
	for name, header := range cfg.HTTPHeaders {
		secrets, err := decryptSecretList(decrypt, p(prefix, "http_headers", name))
		if err != nil {
			return nil, err
		}
		if secrets != nil {
			header.Secrets = secrets
			cfg.HTTPHeaders[name] = header
		}
	}
	if err := decryptProxyHeader(decrypt, p(prefix, "proxy_connect_header"), cfg.ProxyConnectHeader); err != nil {
		return nil, err
	}
	if cfg.OAuth2 != nil {
		if err := decryptProxyHeader(decrypt, p(prefix, "oauth2", "proxy_connect_header"), cfg.OAuth2.ProxyConnectHeader); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func decryptSecretList(decryptFn receivers.DecryptFunc, key schema.IntegrationFieldPath) ([]commoncfg.Secret, error) {
	secret, ok := decryptFn.GetPath(key)
	if !ok {
		return nil, nil
	}
	var secrets []commoncfg.Secret
	if err := json.Unmarshal([]byte(secret), &secrets); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", key, err)
	}
	return secrets, nil
}

func decryptProxyHeader(decryptFn receivers.DecryptFunc, prefix schema.IntegrationFieldPath, header ProxyHeader) error {
	for name := range header {
		secrets, err := decryptSecretList(decryptFn, prefix.With(name))
		if err != nil {
			return err
		}
		if secrets != nil {
			header[name] = secrets
		}
	}
	return nil
}
