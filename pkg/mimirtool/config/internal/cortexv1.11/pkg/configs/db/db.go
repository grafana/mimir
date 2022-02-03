// SPDX-License-Identifier: AGPL-3.0-only

package db

import (
	"flag"
)

type Config struct {
	URI           string `yaml:"uri"`
	MigrationsDir string `yaml:"migrations_dir"`
	PasswordFile  string `yaml:"password_file"`

	// Allow injection of mock DBs for unit testing.
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.URI, "configs.database.uri", "postgres://postgres@configs-db.weave.local/configs?sslmode=disable", "URI where the database can be found (for dev you can use memory://)")
	f.StringVar(&cfg.MigrationsDir, "configs.database.migrations-dir", "", "Path where the database migration files can be found")
	f.StringVar(&cfg.PasswordFile, "configs.database.password-file", "", "File containing password (username goes in URI)")
}
