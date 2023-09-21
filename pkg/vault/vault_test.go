// SPDX-License-Identifier: AGPL-3.0-only

package vault_test

import (
	"testing"

	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSecret(t *testing.T) {
	mimirVaultClient, _ := test.NewMockVault(vault.Config{})

	tests := map[string]struct {
		path          string
		expectError   bool
		expectedValue string
		errorMsg      string
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
			errorMsg:    "unable to read secret from vault",
		},
		"secret returned is nil": {
			path:        "test/secret4",
			expectError: true,
			errorMsg:    "secret data is nil",
		},
		"secret data is not a string": {
			path:        "test/secret5",
			expectError: true,
			errorMsg:    "secret data type is not string, found int value: 123",
		},
		"secret data is nil": {
			path:        "test/secret6",
			expectError: true,
			errorMsg:    "secret data type is not string, found <nil> value: <nil>",
		},
	}

	for testName, testCase := range tests {
		t.Run(testName, func(t *testing.T) {
			secret, err := mimirVaultClient.ReadSecret(testCase.path)
			if testCase.expectError {
				require.Error(t, err)
				require.ErrorContains(t, err, testCase.errorMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, testCase.expectedValue, string(secret))
			}
		})
	}
}
