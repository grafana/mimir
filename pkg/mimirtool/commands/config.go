// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/config"
)

// ConfigCommand works with the mimir configuration parameters (YAML files and CLI flags)
type ConfigCommand struct {
	configFile string
}

// Register rule related commands and flags with the kingpin application
func (c *ConfigCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	configCmd := app.Command("config", "Work with Grafana Mimir configuration.")

	configCmd.
		Command("convert", "Convert a configuration file from Cortex v1.11.0 to Grafana Mimir and output it to stdout").
		Action(c.convertConfig).
		Arg("config-file", "The YAML config file to convert.").Required().ExistingFileVar(&c.configFile)
}

func (c *ConfigCommand) convertConfig(_ *kingpin.ParseContext) error {
	original, err := os.ReadFile(c.configFile)
	if err != nil {
		return errors.Wrap(err, "could not read config-file")
	}
	converted, err := config.Convert(original, config.CortexToMimirMapper, config.DefaultCortexConfig, config.DefaultMimirConfig)
	if err != nil {
		return errors.Wrap(err, "could not convert configuration")
	}
	_, err = fmt.Fprint(os.Stdout, string(converted))
	return err
}
