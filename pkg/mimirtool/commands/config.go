// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/config"
)

// ConfigCommand works with the mimir configuration parameters (YAML files and CLI flags)
type ConfigCommand struct {
	yamlFile  string
	flagsFile string

	outYAMLFile  string
	outFlagsFile string
}

// Register rule related commands and flags with the kingpin application
func (c *ConfigCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	configCmd := app.Command("config", "Work with Grafana Mimir configuration.")

	convertCmd := configCmd.
		Command("convert", "Convert a configuration file from Cortex v1.11.0 to Grafana Mimir and output it to stdout").
		Action(c.convertConfig)

	convertCmd.Flag("yaml-file", "The YAML configuration file to convert.").ExistingFileVar(&c.yamlFile)
	convertCmd.Flag("flags-file", "New-line-delimited list of CLI flags to convert.").ExistingFileVar(&c.flagsFile)
	convertCmd.Flag("yaml-out", "Location to output the converted YAML configuration to. Default STDOUT").ExistingFileVar(&c.outYAMLFile)
	convertCmd.Flag("flags-out", "Location to output list of converted CLI flags to. Default STDOUT").ExistingFileVar(&c.outFlagsFile)
}

func (c *ConfigCommand) convertConfig(_ *kingpin.ParseContext) error {
	yamlContents, flagsFlags, err := c.prepareInputs()
	if err != nil {
		return err
	}

	convertedYAML, flagsFlags, err := config.Convert(yamlContents, flagsFlags, config.CortexToMimirMapper, config.DefaultCortexConfig, config.DefaultMimirConfig)
	if err != nil {
		return errors.Wrap(err, "could not convert configuration")
	}

	return c.output(convertedYAML, flagsFlags)
}

func (c *ConfigCommand) prepareInputs() ([]byte, []string, error) {
	var (
		yamlContents []byte
		flags        []string
		err          error
	)

	if c.flagsFile == "" && c.yamlFile == "" {
		return nil, nil, errors.New("provide at least one of --config-file or --flags-file")
	}
	yamlContents, err = os.ReadFile(c.yamlFile)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, errors.Wrap(err, "could not read config-file")
	}

	flagsContents, err := os.ReadFile(c.flagsFile)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, errors.Wrap(err, "could not read flags-file")
	}
	if len(flagsContents) > 1 {
		flags = strings.Split(string(flagsContents), "\n")
	}
	return yamlContents, flags, nil
}

func (c *ConfigCommand) output(yamlContents []byte, flags []string) error {
	var err error
	outYAMLWriter, outFlagsWriter := os.Stdout, os.Stdout
	if c.outYAMLFile != "" {
		outYAMLWriter, err = os.OpenFile(c.outYAMLFile, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return errors.Wrap(err, "could not open yaml-out file")
		}
		defer func() { _ = outYAMLWriter.Close() }()
	}

	if c.outFlagsFile != "" {
		outFlagsWriter, err = os.OpenFile(c.outFlagsFile, os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			return errors.Wrap(err, "could not open flags-out file")
		}
		defer func() { _ = outFlagsWriter.Close() }()
	}

	_, err = fmt.Fprintln(outYAMLWriter, string(yamlContents))
	_, err2 := fmt.Fprintln(outFlagsWriter, strings.Join(flags, "\n"))
	return multierror.New(err, err2).Err()
}
