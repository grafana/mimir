// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/mimir"
)

// RuntimeConfigCommand works with Mimir's runtime config.
type RuntimeConfigCommand struct {
	configFile string
	mimirFlags []string
}

// Register runtime config related commands and flags with the kingpin application.
func (c *RuntimeConfigCommand) Register(app *kingpin.Application) {
	cmd := app.Command("runtime-config", "Work with Grafana Mimir runtime configuration.")

	validateCmd := cmd.Command("validate", "Validate a runtime configuration file").Action(c.validate)
	validateCmd.Flag("config-file", "The runtime configuration file to validate.").StringVar(&c.configFile)
	validateCmd.Arg("mimir-flags", "Mimir flags to validate the runtime configuration against, passed after --. For example: -- -target=querier -querier.query-store-after=5h. Flags that aren't passed use their default values.").StringsVar(&c.mimirFlags)
}

func (c *RuntimeConfigCommand) validate(*kingpin.ParseContext) error {
	if c.configFile == "" {
		return fmt.Errorf("--config-file cannot be empty")
	}

	var mimirCfg mimir.Config
	if err := parseMimirFlags(&mimirCfg, c.mimirFlags); err != nil {
		return err
	}
	mimirCfg.RuntimeConfig.LoadPath = flagext.StringSliceCSV{c.configFile}
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

// parseMimirFlags sets cfg to Mimir's defaults overridden by args, the same way the Mimir binary does.
// The runtime configuration is validated against some of these flags, e.g. -querier.query-store-after.
func parseMimirFlags(cfg *mimir.Config, args []string) error {
	fs := flag.NewFlagSet("mimir", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg.RegisterFlags(fs, log.NewNopLogger())

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse Mimir flags: %w", err)
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("parse Mimir flags: unexpected argument %q", fs.Arg(0))
	}

	if err := mimir.InheritCommonFlagValues(log.NewNopLogger(), fs, cfg.Common, cfg); err != nil {
		return fmt.Errorf("inherit common flag values: %w", err)
	}
	return nil
}
