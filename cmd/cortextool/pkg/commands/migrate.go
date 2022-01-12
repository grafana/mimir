package commands

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/cmd/cortextool/pkg/chunk/migrate"
	"github.com/grafana/mimir/cmd/cortextool/pkg/chunk/migrate/reader"
)

type migrateChunksCommandOptions struct {
	ConfigFile string
	Config     migrate.Config
	Planner    reader.PlannerConfig
}

func registerMigrateChunksCommandOptions(cmd *kingpin.CmdClause) {
	migrateChunksCommandOptions := &migrateChunksCommandOptions{}
	migrateChunksCommand := cmd.Command("migrate", "Deletes the specified chunk references from the index").Action(migrateChunksCommandOptions.run)
	migrateChunksCommand.Flag("config-file", "path to migration job config file").Required().StringVar(&migrateChunksCommandOptions.ConfigFile)
	migrateChunksCommandOptions.Planner.Register(migrateChunksCommand)
}

func (c *migrateChunksCommandOptions) run(k *kingpin.ParseContext) error {
	f, err := os.Open(c.ConfigFile)
	if err != nil {
		return err
	}

	decoder := yaml.NewDecoder(f)
	decoder.KnownFields(true)

	if err := decoder.Decode(&c.Config); err != nil {
		return err
	}

	migrator, err := migrate.NewMigrator(c.Config, c.Planner)
	if err != nil {
		return err
	}

	migrator.Run()
	return nil
}
