// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"context"

	hashivault "github.com/hashicorp/vault/api"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/vault"
)

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
			"test/secret6": {
				secret: &hashivault.KVSecret{
					Data: map[string]interface{}{
						"value": nil,
					},
				},
				err: nil,
			},
		},
	}
}

func (m *mockKVStore) Get(_ context.Context, path string) (*hashivault.KVSecret, error) {
	return m.values[path].secret, m.values[path].err
}

func NewMockVault(vault.Config) (*vault.Vault, error) {
	return &vault.Vault{
		KVStore: newMockKVStore(),
	}, nil
}
