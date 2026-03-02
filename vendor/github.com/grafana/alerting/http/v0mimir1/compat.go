package v0mimir1

import (
	commoncfg "github.com/prometheus/common/config"
)

// ToCommonHTTPClientConfig converts HTTPClientConfig to commoncfg.HTTPClientConfig.
func (c *HTTPClientConfig) ToCommonHTTPClientConfig() *commoncfg.HTTPClientConfig {
	if c == nil {
		return nil
	}
	return &commoncfg.HTTPClientConfig{
		BasicAuth:       toCommonBasicAuth(c.BasicAuth),
		Authorization:   toCommonAuthorization(c.Authorization),
		OAuth2:          toCommonOAuth2(c.OAuth2),
		BearerToken:     c.BearerToken,
		BearerTokenFile: c.BearerTokenFile,
		TLSConfig:       ToCommonTLSConfig(c.TLSConfig),
		FollowRedirects: c.FollowRedirects,
		EnableHTTP2:     c.EnableHTTP2,
		ProxyConfig:     toCommonProxyConfig(c.ProxyConfig),
		HTTPHeaders:     toCommonHeaders(c.HTTPHeaders),
	}
}

// FromCommonHTTPClientConfig converts commoncfg.HTTPClientConfig to HTTPClientConfig.
func FromCommonHTTPClientConfig(c *commoncfg.HTTPClientConfig) *HTTPClientConfig {
	if c == nil {
		return nil
	}
	return &HTTPClientConfig{
		BasicAuth:       fromCommonBasicAuth(c.BasicAuth),
		Authorization:   fromCommonAuthorization(c.Authorization),
		OAuth2:          fromCommonOAuth2(c.OAuth2),
		BearerToken:     c.BearerToken,
		BearerTokenFile: c.BearerTokenFile,
		TLSConfig:       FromCommonTLSConfig(c.TLSConfig),
		FollowRedirects: c.FollowRedirects,
		EnableHTTP2:     c.EnableHTTP2,
		ProxyConfig:     fromCommonProxyConfig(c.ProxyConfig),
		HTTPHeaders:     fromCommonHeaders(c.HTTPHeaders),
	}
}

func toCommonBasicAuth(a *BasicAuth) *commoncfg.BasicAuth {
	if a == nil {
		return nil
	}
	return &commoncfg.BasicAuth{
		Username:     a.Username,
		UsernameFile: a.UsernameFile,
		UsernameRef:  a.UsernameRef,
		Password:     a.Password,
		PasswordFile: a.PasswordFile,
		PasswordRef:  a.PasswordRef,
	}
}

func fromCommonBasicAuth(a *commoncfg.BasicAuth) *BasicAuth {
	if a == nil {
		return nil
	}
	return &BasicAuth{
		Username:     a.Username,
		UsernameFile: a.UsernameFile,
		UsernameRef:  a.UsernameRef,
		Password:     a.Password,
		PasswordFile: a.PasswordFile,
		PasswordRef:  a.PasswordRef,
	}
}

func toCommonAuthorization(a *Authorization) *commoncfg.Authorization {
	if a == nil {
		return nil
	}
	return &commoncfg.Authorization{
		Type:            a.Type,
		Credentials:     a.Credentials,
		CredentialsFile: a.CredentialsFile,
		CredentialsRef:  a.CredentialsRef,
	}
}

func fromCommonAuthorization(a *commoncfg.Authorization) *Authorization {
	if a == nil {
		return nil
	}
	return &Authorization{
		Type:            a.Type,
		Credentials:     a.Credentials,
		CredentialsFile: a.CredentialsFile,
		CredentialsRef:  a.CredentialsRef,
	}
}

func toCommonOAuth2(o *OAuth2) *commoncfg.OAuth2 {
	if o == nil {
		return nil
	}
	return &commoncfg.OAuth2{
		ClientID:         o.ClientID,
		ClientSecret:     o.ClientSecret,
		ClientSecretFile: o.ClientSecretFile,
		ClientSecretRef:  o.ClientSecretRef,
		Scopes:           o.Scopes,
		TokenURL:         o.TokenURL,
		EndpointParams:   o.EndpointParams,
		TLSConfig:        ToCommonTLSConfig(o.TLSConfig),
		ProxyConfig:      toCommonProxyConfig(o.ProxyConfig),
	}
}

func fromCommonOAuth2(o *commoncfg.OAuth2) *OAuth2 {
	if o == nil {
		return nil
	}
	return &OAuth2{
		ClientID:         o.ClientID,
		ClientSecret:     o.ClientSecret,
		ClientSecretFile: o.ClientSecretFile,
		ClientSecretRef:  o.ClientSecretRef,
		Scopes:           o.Scopes,
		TokenURL:         o.TokenURL,
		EndpointParams:   o.EndpointParams,
		TLSConfig:        FromCommonTLSConfig(o.TLSConfig),
		ProxyConfig:      fromCommonProxyConfig(o.ProxyConfig),
	}
}

func ToCommonTLSConfig(c TLSConfig) commoncfg.TLSConfig {
	return commoncfg.TLSConfig{
		CA:                 c.CA,
		Cert:               c.Cert,
		Key:                c.Key,
		CAFile:             c.CAFile,
		CertFile:           c.CertFile,
		KeyFile:            c.KeyFile,
		CARef:              c.CARef,
		CertRef:            c.CertRef,
		KeyRef:             c.KeyRef,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify,
		MinVersion:         c.MinVersion,
		MaxVersion:         c.MaxVersion,
	}
}

func FromCommonTLSConfig(c commoncfg.TLSConfig) TLSConfig {
	return TLSConfig{
		CA:                 c.CA,
		Cert:               c.Cert,
		Key:                c.Key,
		CAFile:             c.CAFile,
		CertFile:           c.CertFile,
		KeyFile:            c.KeyFile,
		CARef:              c.CARef,
		CertRef:            c.CertRef,
		KeyRef:             c.KeyRef,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify,
		MinVersion:         c.MinVersion,
		MaxVersion:         c.MaxVersion,
	}
}

func toCommonProxyConfig(c ProxyConfig) commoncfg.ProxyConfig {
	return commoncfg.ProxyConfig{
		ProxyURL:             c.ProxyURL,
		NoProxy:              c.NoProxy,
		ProxyFromEnvironment: c.ProxyFromEnvironment,
		ProxyConnectHeader:   commoncfg.ProxyHeader(c.ProxyConnectHeader),
	}
}

func fromCommonProxyConfig(c commoncfg.ProxyConfig) ProxyConfig {
	return ProxyConfig{
		ProxyURL:             c.ProxyURL,
		NoProxy:              c.NoProxy,
		ProxyFromEnvironment: c.ProxyFromEnvironment,
		ProxyConnectHeader:   ProxyHeader(c.ProxyConnectHeader),
	}
}

func toCommonHeaders(h *Headers) *commoncfg.Headers {
	if h == nil {
		return nil
	}
	headers := make(map[string]commoncfg.Header, len(h.Headers))
	for k, v := range h.Headers {
		headers[k] = toCommonHeader(v)
	}
	return &commoncfg.Headers{Headers: headers}
}

func fromCommonHeaders(h *commoncfg.Headers) *Headers {
	if h == nil {
		return nil
	}
	headers := make(map[string]Header, len(h.Headers))
	for k, v := range h.Headers {
		headers[k] = fromCommonHeader(v)
	}
	return &Headers{Headers: headers}
}

func toCommonHeader(h Header) commoncfg.Header {
	return commoncfg.Header{
		Values:  h.Values,
		Secrets: h.Secrets,
		Files:   h.Files,
	}
}

func fromCommonHeader(h commoncfg.Header) Header {
	return Header{
		Values:  h.Values,
		Secrets: h.Secrets,
		Files:   h.Files,
	}
}
