// SPDX-License-Identifier: AGPL-3.0-only

package commands

type EnvVarNames struct {
	Address               string
	APIKey                string
	APIUser               string
	TLSCAPath             string
	TLSCertPath           string
	TLSKeyPath            string
	TLSInsecureSkipVerify string
	TenantID              string
	UseLegacyRoutes       string
	MimirHTTPPrefix       string
	AuthToken             string
	ExtraHeaders          string
}

func NewEnvVarsWithPrefix(prefix string) EnvVarNames {
	const (
		address               = "ADDRESS"
		apiKey                = "API_KEY"
		apiUser               = "API_USER"
		tenantID              = "TENANT_ID"
		tlsCAPath             = "TLS_CA_PATH"
		tlsCertPath           = "TLS_CERT_PATH"
		tlsKeyPath            = "TLS_KEY_PATH"
		tlsInsecureSkipVerify = "TLS_INSECURE_SKIP_VERIFY"
		useLegacyRoutes       = "USE_LEGACY_ROUTES"
		authToken             = "AUTH_TOKEN"
		extraHeaders          = "EXTRA_HEADERS"
		mimirHTTPPrefix       = "HTTP_PREFIX"
	)

	if len(prefix) > 0 && prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}

	return EnvVarNames{
		Address:               prefix + address,
		APIKey:                prefix + apiKey,
		APIUser:               prefix + apiUser,
		TLSCAPath:             prefix + tlsCAPath,
		TLSCertPath:           prefix + tlsCertPath,
		TLSKeyPath:            prefix + tlsKeyPath,
		TLSInsecureSkipVerify: prefix + tlsInsecureSkipVerify,
		TenantID:              prefix + tenantID,
		UseLegacyRoutes:       prefix + useLegacyRoutes,
		AuthToken:             prefix + authToken,
		ExtraHeaders:          prefix + extraHeaders,
		MimirHTTPPrefix:       prefix + mimirHTTPPrefix,
	}
}
