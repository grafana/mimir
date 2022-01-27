package commands

type EnvVarNames struct {
	Address     string
	APIKey      string
	APIUser     string
	TenantID    string
	TLSCAPath   string
	TLSCertPath string
	TLSKeyPath  string
}

func NewEnvVarsWithPrefix(prefix string) EnvVarNames {
	const (
		address     = "ADDRESS"
		apiKey      = "API_KEY"
		apiUser     = "API_USER"
		tenantID    = "TENANT_ID"
		tlsCAPath   = "TLS_CA_PATH"
		tlsCertPath = "TLS_CERT_PATH"
		tlsKeyPath  = "TLS_KEY_PATH"
	)

	if len(prefix) < 1 || prefix[len(prefix)-1] != '_' {
		prefix = "_" + prefix
	}

	return EnvVarNames{
		Address:     prefix + address,
		APIKey:      prefix + apiKey,
		APIUser:     prefix + apiUser,
		TenantID:    prefix + tenantID,
		TLSCAPath:   prefix + tlsCAPath,
		TLSCertPath: prefix + tlsCertPath,
		TLSKeyPath:  prefix + tlsKeyPath,
	}
}
