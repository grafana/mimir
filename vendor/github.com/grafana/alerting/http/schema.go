package http

import "github.com/grafana/alerting/receivers/schema"

func V1TLSSubformOptions() []schema.Field {
	return []schema.Field{
		{
			Label:        "Disable certificate verification",
			Element:      schema.ElementTypeCheckbox,
			Description:  "Do not verify the server's certificate chain and host name.",
			PropertyName: "insecureSkipVerify",
			Required:     false,
		},
		{
			Label:        "CA Certificate",
			Element:      schema.ElementTypeTextArea,
			Description:  "Certificate in PEM format to use when verifying the server's certificate chain.",
			InputType:    schema.InputTypeText,
			PropertyName: "caCertificate",
			Required:     false,
			Secure:       true,
		},
		{
			Label:        "Client Certificate",
			Element:      schema.ElementTypeTextArea,
			Description:  "Client certificate in PEM format to use when connecting to the server.",
			InputType:    schema.InputTypeText,
			PropertyName: "clientCertificate",
			Required:     false,
			Secure:       true,
		},
		{
			Label:        "Client Key",
			Element:      schema.ElementTypeTextArea,
			Description:  "Client key in PEM format to use when connecting to the server.",
			InputType:    schema.InputTypeText,
			PropertyName: "clientKey",
			Required:     false,
			Secure:       true,
		},
	}
}

func V1ProxyOption() schema.Field {
	return schema.Field{ // New in 12.1.
		Label:        "Proxy Config",
		PropertyName: "proxy_config",
		Description:  "Optional proxy configuration.",
		Element:      schema.ElementTypeSubform,
		SubformOptions: []schema.Field{
			{
				Label:        "Proxy URL",
				PropertyName: "proxy_url",
				Description:  "HTTP proxy server to use to connect to the targets.",
				Element:      schema.ElementTypeInput,
				InputType:    schema.InputTypeText,
				Placeholder:  "https://proxy.example.com",
				Required:     false,
				Secure:       false,
				Protected:    true,
			},
			{
				Label:        "Proxy from environment",
				PropertyName: "proxy_from_environment",
				Description:  "Use environment HTTP_PROXY, HTTPS_PROXY and NO_PROXY to determine proxies.",
				Element:      schema.ElementTypeCheckbox,
				Required:     false,
				Secure:       false,
			},
			{
				Label:        "No Proxy",
				PropertyName: "no_proxy",
				Description:  "Comma-separated list of addresses that should not use a proxy.",
				Element:      schema.ElementTypeInput,
				InputType:    schema.InputTypeText,
				Placeholder:  "example.com,1.2.3.4",
				Required:     false,
				Secure:       false,
			},
			{
				Label:        "Proxy Connect Header",
				PropertyName: "proxy_connect_header",
				Description:  "Optional headers to send to proxies during CONNECT requests.",
				Element:      schema.ElementTypeKeyValueMap,
				InputType:    schema.InputTypeText,
				Required:     false,
				Secure:       false,
			},
		},
	}
}

func V1HttpClientOption() schema.Field {
	return schema.Field{ // New in 12.1.
		Label:        "HTTP Config",
		PropertyName: "http_config",
		Description:  "Common HTTP client options.",
		Element:      schema.ElementTypeSubform,
		SubformOptions: []schema.Field{
			{ // New in 12.1.
				Label:        "OAuth2",
				PropertyName: "oauth2",
				Description:  "OAuth2 configuration options",
				Element:      schema.ElementTypeSubform,
				SubformOptions: []schema.Field{
					{
						Label:        "Token URL",
						PropertyName: "token_url",
						Element:      schema.ElementTypeInput,
						Description:  "URL for the access token endpoint.",
						InputType:    schema.InputTypeText,
						Required:     true,
						Secure:       false,
						Protected:    true,
					},
					{
						Label:        "Client ID",
						PropertyName: "client_id",
						Element:      schema.ElementTypeInput,
						Description:  "Client ID to use when authenticating.",
						InputType:    schema.InputTypeText,
						Required:     true,
						Secure:       false,
					},
					{
						Label:        "Client Secret",
						PropertyName: "client_secret",
						Element:      schema.ElementTypeInput,
						Description:  "Client secret to use when authenticating.",
						InputType:    schema.InputTypeText,
						Required:     true,
						Secure:       true,
					},
					{
						Label:        "Scopes",
						PropertyName: "scopes",
						Element:      schema.ElementStringArray,
						Description:  "Optional scopes to request when obtaining an access token.",
						Required:     false,
						Secure:       false,
					},
					{
						Label:        "Endpoint Parameters",
						PropertyName: "endpoint_params",
						Element:      schema.ElementTypeKeyValueMap,
						Description:  "Optional parameters to append to the access token request.",
						Required:     false,
						Secure:       false,
					},
					{
						Label:          "TLS",
						PropertyName:   "tls_config",
						Description:    "Optional TLS configuration options for OAuth2 requests.",
						Element:        schema.ElementTypeSubform,
						SubformOptions: V1TLSSubformOptions(),
					},
					V1ProxyOption(),
				},
			},
		},
	}
}
