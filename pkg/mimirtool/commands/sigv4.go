// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"github.com/alecthomas/kingpin/v2"

	"github.com/grafana/mimir/pkg/mimirtool/client"
)

func registerSigV4Flags(cmd *kingpin.CmdClause, envVars EnvVarNames, cfg *client.SigV4Config) {
	cmd.Flag("sigv4-region", "AWS region for SigV4 request signing; alternatively, set "+envVars.SigV4Region+".").
		Default("").
		Envar(envVars.SigV4Region).
		StringVar(&cfg.Region)

	cmd.Flag("sigv4-access-key", "AWS access key for SigV4 request signing; alternatively, set "+envVars.SigV4AccessKey+".").
		Default("").
		Envar(envVars.SigV4AccessKey).
		StringVar(&cfg.AccessKey)

	cmd.Flag("sigv4-secret-key", "AWS secret key for SigV4 request signing; alternatively, set "+envVars.SigV4SecretKey+".").
		Default("").
		Envar(envVars.SigV4SecretKey).
		StringVar(&cfg.SecretKey)

	cmd.Flag("sigv4-profile", "AWS shared config profile for SigV4 request signing; alternatively, set "+envVars.SigV4Profile+".").
		Default("").
		Envar(envVars.SigV4Profile).
		StringVar(&cfg.Profile)

	cmd.Flag("sigv4-assume-role-arn", "AWS role Amazon Resource Name (ARN) to assume for SigV4 request signing; alternatively, set "+envVars.SigV4AssumeRoleARN+".").
		Default("").
		Envar(envVars.SigV4AssumeRoleARN).
		StringVar(&cfg.RoleARN)

	cmd.Flag("sigv4-external-id", "AWS external ID to use when assuming the SigV4 role; alternatively, set "+envVars.SigV4ExternalID+".").
		Default("").
		Envar(envVars.SigV4ExternalID).
		StringVar(&cfg.ExternalID)

	cmd.Flag("sigv4-use-fips-sts-endpoint", "Use the Federal Information Processing Standards (FIPS) AWS Security Token Service (STS) endpoint when assuming a SigV4 role; alternatively, set "+envVars.SigV4UseFIPSSTSEndpoint+".").
		Default("false").
		Envar(envVars.SigV4UseFIPSSTSEndpoint).
		BoolVar(&cfg.UseFIPSSTSEndpoint)

	cmd.Flag("sigv4-service-name", "AWS service name for SigV4 request signing. If SigV4 is enabled and this is unset, the underlying signer defaults to aps; use execute-api for API Gateway. Alternatively, set "+envVars.SigV4ServiceName+".").
		Default("").
		Envar(envVars.SigV4ServiceName).
		StringVar(&cfg.ServiceName)
}
