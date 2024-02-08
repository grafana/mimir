// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"fmt"
	"testing"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	hashivault "github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestVaultTokenRenewal(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Initialize Vault
	vault := e2e.NewHTTPService(
		"vault",
		"hashicorp/vault:1.13.2",
		// Create the buckets before starting minio
		nil,
		e2e.NewHTTPReadinessProbe(8200, "/v1/sys/health", 200, 200),
		8200,
	)
	vault.SetEnvVars(map[string]string{"VAULT_DEV_ROOT_TOKEN_ID": "dev_token"})
	require.NoError(t, s.StartAndWaitReady(vault))

	cli, err := hashivault.NewClient(&hashivault.Config{Address: fmt.Sprintf("http://%s", vault.HTTPEndpoint())})
	require.NoError(t, err)

	cli.SetToken("dev_token")

	err = cli.Sys().EnableAuthWithOptions("userpass", &hashivault.EnableAuthOptions{
		Type: "userpass",
	})
	require.NoError(t, err)

	_, err = cli.Logical().Write("auth/userpass/users/foo", map[string]interface{}{
		"password": "bar",
		"ttl":      "5s",
		"max_ttl":  "10s",
	})
	require.NoError(t, err)

	consul := e2edb.NewConsul()
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(consul, minio))

	flags := mergeFlags(
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-vault.enabled":                "true",
			"-vault.url":                    fmt.Sprintf("http://%s", vault.NetworkHTTPEndpoint()),
			"-vault.mount-path":             "secret",
			"-vault.auth.type":              "userpass",
			"-vault.auth.userpass.username": "foo",
			"-vault.auth.userpass.password": "bar",
			"-log.level":                    "debug",
		},
	)

	// Start Mimir
	mimir := e2emimir.NewSingleBinary("mimir-1", e2e.MergeFlags(DefaultSingleBinaryFlags(), flags))
	require.NoError(t, s.StartAndWaitReady(mimir))

	// Check that the token lease has been updated before hitting max_ttl
	require.NoError(t, mimir.WaitSumMetrics(e2e.GreaterOrEqual(2), "cortex_vault_token_lease_renewal_total"))
	// Check that re-authentication occurred
	require.NoError(t, mimir.WaitSumMetrics(e2e.Equals(2), "cortex_vault_auth_total"))
}
