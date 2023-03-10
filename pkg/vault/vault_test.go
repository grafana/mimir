// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"context"
	"errors"
	"testing"

	hashivault "github.com/hashicorp/vault/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestReadSecret(t *testing.T) {
	mockKVStore := NewMockKVStore()
	mockKVStore.On("Get", mock.Anything, "test/secret1").Return(&hashivault.KVSecret{
		Data: map[string]interface{}{
			"value": "foo1",
		},
	}, nil)

	mockKVStore.On("Get", mock.Anything, "test/secret2").Return(&hashivault.KVSecret{
		Data: map[string]interface{}{
			"value": "foo2",
		},
	}, nil)

	mockKVStore.On("Get", mock.Anything, "test/secret3").Return(&hashivault.KVSecret{}, errors.New("vault error"))

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
	mock.Mock
}

func NewMockKVStore() *mockKVStore {
	return &mockKVStore{}
}

func (m *mockKVStore) Get(ctx context.Context, path string) (*hashivault.KVSecret, error) {
	args := m.Called(ctx, path)
	return args.Get(0).(*hashivault.KVSecret), args.Error(1)
}
