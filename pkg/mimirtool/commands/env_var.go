// SPDX-License-Identifier: AGPL-3.0-only

package commands

type EnvVarNames struct {
	Address                 string
	APIKey                  string
	APIUser                 string
	TLSCAPath               string
	TLSCertPath             string
	TLSKeyPath              string
	TLSInsecureSkipVerify   string
	TenantID                string
	UseLegacyRoutes         string
	MimirHTTPPrefix         string
	AuthToken               string
	ExtraHeaders            string
	SigV4Region             string
	SigV4AccessKey          string
	SigV4SecretKey          string
	SigV4Profile            string
	SigV4AssumeRoleARN      string
	SigV4ExternalID         string
	SigV4UseFIPSSTSEndpoint string
	SigV4ServiceName        string
}

func NewEnvVarsWithPrefix(prefix string) EnvVarNames {
	const (
		address                 = "ADDRESS"
		apiKey                  = "API_KEY"
		apiUser                 = "API_USER"
		tenantID                = "TENANT_ID"
		tlsCAPath               = "TLS_CA_PATH"
		tlsCertPath             = "TLS_CERT_PATH"
		tlsKeyPath              = "TLS_KEY_PATH"
		tlsInsecureSkipVerify   = "TLS_INSECURE_SKIP_VERIFY"
		useLegacyRoutes         = "USE_LEGACY_ROUTES"
		authToken               = "AUTH_TOKEN"
		extraHeaders            = "EXTRA_HEADERS"
		mimirHTTPPrefix         = "HTTP_PREFIX"
		sigV4Region             = "SIGV4_REGION"
		sigV4AccessKey          = "SIGV4_ACCESS_KEY"
		sigV4SecretKey          = "SIGV4_SECRET_KEY"
		sigV4Profile            = "SIGV4_PROFILE"
		sigV4AssumeRoleARN      = "SIGV4_ASSUME_ROLE_ARN"
		sigV4ExternalID         = "SIGV4_EXTERNAL_ID"
		sigV4UseFIPSSTSEndpoint = "SIGV4_USE_FIPS_STS_ENDPOINT"
		sigV4ServiceName        = "SIGV4_SERVICE_NAME"
	)

	if len(prefix) > 0 && prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}

	return EnvVarNames{
		Address:                 prefix + address,
		APIKey:                  prefix + apiKey,
		APIUser:                 prefix + apiUser,
		TLSCAPath:               prefix + tlsCAPath,
		TLSCertPath:             prefix + tlsCertPath,
		TLSKeyPath:              prefix + tlsKeyPath,
		TLSInsecureSkipVerify:   prefix + tlsInsecureSkipVerify,
		TenantID:                prefix + tenantID,
		UseLegacyRoutes:         prefix + useLegacyRoutes,
		AuthToken:               prefix + authToken,
		ExtraHeaders:            prefix + extraHeaders,
		MimirHTTPPrefix:         prefix + mimirHTTPPrefix,
		SigV4Region:             prefix + sigV4Region,
		SigV4AccessKey:          prefix + sigV4AccessKey,
		SigV4SecretKey:          prefix + sigV4SecretKey,
		SigV4Profile:            prefix + sigV4Profile,
		SigV4AssumeRoleARN:      prefix + sigV4AssumeRoleARN,
		SigV4ExternalID:         prefix + sigV4ExternalID,
		SigV4UseFIPSSTSEndpoint: prefix + sigV4UseFIPSSTSEndpoint,
		SigV4ServiceName:        prefix + sigV4ServiceName,
	}
}
