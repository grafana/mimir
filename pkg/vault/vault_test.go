// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"testing"

	kv "github.com/hashicorp/vault-plugin-secrets-kv"
	vaultapi "github.com/hashicorp/vault/api"
	vaulthttp "github.com/hashicorp/vault/http"
	"github.com/hashicorp/vault/sdk/logical"
	hashivault "github.com/hashicorp/vault/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSecret(t *testing.T) {
	testVault := createTestVaultWithSecrets(t)
	defer testVault.Cleanup()
	testVaultClient := testVault.Cores[0].Client

	mimirVaultClient := Vault{
		client: testVaultClient,
		config: Config{
			URL:       "https://127.0.0.1",
			Token:     "test-token",
			MountPath: "secret",
		},
	}

	tests := map[string]struct {
		path          string
		expectError   bool
		expectedValue string
	}{
		"read secret1 with no error": {
			path:          "test/secret1",
			expectError:   false,
			expectedValue: "foo1",
		},
		"read secret2 with no error": {
			path:          "test/secret2",
			expectError:   false,
			expectedValue: "foo2",
		},
		"read secret with non-existent path": {
			path:        "test/secret3",
			expectError: true,
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			secret, err := mimirVaultClient.ReadSecret(testCase.path)
			if testCase.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, testCase.expectedValue, string(secret))
			}
		})
	}
}

func createTestVaultWithSecrets(t *testing.T) *hashivault.TestCluster {
	testCluster := hashivault.NewTestCluster(t, &hashivault.CoreConfig{
		LogicalBackends: map[string]logical.Factory{
			"kv": kv.Factory,
		},
	}, &hashivault.TestClusterOptions{
		HandlerFunc: vaulthttp.Handler,
	})

	testCluster.Start()

	err := testCluster.Cores[0].Client.Sys().Mount("kv", &vaultapi.MountInput{
		Type: "kv",
		Options: map[string]string{
			"version": "2",
		},
	})
	require.NoError(t, err)

	core := testCluster.Cores[0].Core
	hashivault.TestWaitActive(t, core)
	client := testCluster.Cores[0].Client

	_, err = client.Logical().Write("secret/data/test/secret1", map[string]interface{}{
		"data": map[string]interface{}{
			"value": "foo1"},
	})
	require.NoError(t, err)

	_, err = client.Logical().Write("secret/data/test/secret2", map[string]interface{}{
		"data": map[string]interface{}{
			"value": "foo2"},
	})
	require.NoError(t, err)

	return testCluster
}
