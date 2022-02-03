// SPDX-License-Identifier: AGPL-3.0-only

package config

type Migrator struct{}

func (m Migrator) Migrate(contents []byte) ([]byte, error) {
	return nil, nil
}
