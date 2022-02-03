// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// ConfigCommand works with the mimir configuration parameters (YAML files and CLI flags)
type ConfigCommand struct {
	configFile string
}

// Register rule related commands and flags with the kingpin application
func (c *ConfigCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	configCmd := app.Command("config", "Work with Grafana Mimir configuration (YAML files and CLI flags).")

	configCmd.
		Command("convert", "Convert a configuration file from Cortex v1.11.0 to Grafana Mimir and output it to stdout").
		Action(c.convertConfig).
		Arg("config-file", "The YAML config file to convert.").Required().ExistingFileVar(&c.configFile)
}

func (c *ConfigCommand) convertConfig(ctx *kingpin.ParseContext) error {
	return nil
}
