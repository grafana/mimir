// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"flag"
	"fmt"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/runtimeconfig"

	"github.com/grafana/mimir/pkg/mimir"
)

// RuntimeConfigCommand works with Mimir's runtime config.
type RuntimeConfigCommand struct {
	configFile string
}

// Register runtime config related commands and flags with the kingpin application.
func (c *RuntimeConfigCommand) Register(app *kingpin.Application) {
	cmd := app.Command("runtime-config", "Work with Grafana Mimir runtime configuration.")

	validateCmd := cmd.Command("validate", "Validate a runtime configuration file").Action(c.validate)
	validateCmd.Flag("config-file", "The runtime configuration file to validate.").StringVar(&c.configFile)
}

func (c *RuntimeConfigCommand) validate(*kingpin.ParseContext) error {
	if c.configFile == "" {
		return fmt.Errorf("--config-file cannot be empty")
	}

	mimirCfg := mimir.Config{
		RuntimeConfig: runtimeconfig.Config{
			LoadPath: flagext.StringSliceCSV{c.configFile},
		},
	}
	mimirCfg.RegisterFlags(flag.NewFlagSet("", flag.PanicOnError), log.NewNopLogger())
	if err := mimirCfg.Validate(log.NewNopLogger()); err != nil {
		return err
	}

	serv, err := mimir.NewRuntimeManager(&mimirCfg, "mimirtool-runtime-config", nil, log.NewNopLogger())
	if err != nil {
		return fmt.Errorf("create runtime config service: %w", err)
	}
	ctx := context.Background()
	if err := serv.StartAsync(ctx); err != nil {
		return fmt.Errorf("start runtime config service: %w", err)
	}
	if err := serv.AwaitRunning(ctx); err != nil {
		return fmt.Errorf("wait on runtime config service: %w", err)
	}
	serv.StopAsync()
	if err := serv.AwaitTerminated(ctx); err != nil {
		return fmt.Errorf("wait on runtime config service termination: %w", err)
	}
	return nil
}
