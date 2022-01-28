// SPDX-License-Identifier: AGPL-3.0-only

package commands

type EnvVarNames struct {
	Address         string
	APIKey          string
	APIUser         string
	TLSCAPath       string
	TLSCertPath     string
	TLSKeyPath      string
	TenantID        string
	UseLegacyRoutes string
}

func NewEnvVarsWithPrefix(prefix string) EnvVarNames {
	const (
		address         = "ADDRESS"
		apiKey          = "API_KEY"
		apiUser         = "API_USER"
		tenantID        = "TENANT_ID"
		tlsCAPath       = "TLS_CA_PATH"
		tlsCertPath     = "TLS_CERT_PATH"
		tlsKeyPath      = "TLS_KEY_PATH"
		useLegacyRoutes = "USE_LEGACY_ROUTES"
	)

	if len(prefix) > 0 && prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}

	return EnvVarNames{
		Address:         prefix + address,
		APIKey:          prefix + apiKey,
		APIUser:         prefix + apiUser,
		TLSCAPath:       prefix + tlsCAPath,
		TLSCertPath:     prefix + tlsCertPath,
		TLSKeyPath:      prefix + tlsKeyPath,
		TenantID:        prefix + tenantID,
		UseLegacyRoutes: prefix + useLegacyRoutes,
	}
}
