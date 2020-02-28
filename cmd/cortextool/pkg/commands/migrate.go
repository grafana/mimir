package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/cortextool/pkg/chunk/migrate"
)

type migrateChunksCommandOptions struct {
	migrate.MigratorConfig
}

func registerMigrateChunksCommandOptions(cmd *kingpin.CmdClause) {
	migrateChunksCommandOptions := &migrateChunksCommandOptions{}
	migrateChunksCommand := cmd.Command("migrate", "Deletes the specified chunk references from the index").Action(migrateChunksCommandOptions.run)

	migrateChunksCommandOptions.MigratorConfig.Register(migrateChunksCommand)
}

func (c *migrateChunksCommandOptions) run(k *kingpin.ParseContext) error {
	migrator, err := migrate.NewMigrator(c.MigratorConfig)
	if err != nil {
		return err
	}

	migrator.Run()
	return nil
}
