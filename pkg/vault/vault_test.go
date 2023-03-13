// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"context"
	"errors"
	"testing"

	hashivault "github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSecret(t *testing.T) {
	mockKVStore := newMockKVStore()
	mimirVaultClient := Vault{
		KVStore: mockKVStore,
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
		"secret returned is nil": {
			path:        "test/secret4",
			expectError: true,
		},
		"secret data is not a string": {
			path:        "test/secret5",
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

type mockKVStore struct {
	values map[string]mockValue
}

type mockValue struct {
	secret *hashivault.KVSecret
	err    error
}

func newMockKVStore() *mockKVStore {
	return &mockKVStore{
		values: map[string]mockValue{
			"test/secret1": {
				secret: &hashivault.KVSecret{
					Data: map[string]interface{}{
						"value": "foo1",
					},
				},
				err: nil,
			},
			"test/secret2": {
				secret: &hashivault.KVSecret{
					Data: map[string]interface{}{
						"value": "foo2",
					},
				},
				err: nil,
			},
			"test/secret3": {
				secret: nil,
				err:    errors.New("non-existent path"),
			},
			"test/secret4": {
				secret: nil,
				err:    nil,
			},
			"test/secret5": {
				secret: &hashivault.KVSecret{
					Data: map[string]interface{}{
						"value": 123,
					},
				},
				err: nil,
			},
		},
	}
}

func (m *mockKVStore) Get(ctx context.Context, path string) (*hashivault.KVSecret, error) {
	return m.values[path].secret, m.values[path].err
}
