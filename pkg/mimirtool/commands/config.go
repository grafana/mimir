// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/mimirtool/config"
)

// ConfigCommand works with the mimir configuration parameters (YAML files and CLI flags)
type ConfigCommand struct {
	yamlFile  string
	flagsFile string

	outYAMLFile    string
	outFlagsFile   string
	outNoticesFile string

	updateDefaults  bool
	includeDefaults bool

	verbose bool

	gem bool
}

// Register config related commands and flags with the kingpin application.
func (c *ConfigCommand) Register(app *kingpin.Application, _ EnvVarNames) {
	configCmd := app.Command("config", "Work with Grafana Mimir configuration.")

	convertCmd := configCmd.
		Command("convert", "Convert configuration parameters (YAML and CLI flags) from Cortex v1.10.0 and above to Grafana Mimir v2.0.0 (default), and from Grafana Metrics Enterprise v1.7.0 to v2.0.0 (via --gem flag).").
		Action(c.convertConfig)

	convertCmd.Flag("yaml-file", "The YAML configuration file to convert.").StringVar(&c.yamlFile)
	convertCmd.Flag("flags-file", "Newline-delimited list of CLI flags to convert.").StringVar(&c.flagsFile)
	convertCmd.Flag("yaml-out", "The file to output the converted YAML configuration to. If not set, output to stdout.").StringVar(&c.outYAMLFile)
	convertCmd.Flag("flags-out", "The file to output the list of converted CLI flags to. If not set, output to stdout.").StringVar(&c.outFlagsFile)
	convertCmd.Flag("update-defaults", "If you set this flag and you set a configuration parameter to a default value that has changed in Mimir 2.0, the parameter updates to the new default value.").BoolVar(&c.updateDefaults)
	convertCmd.Flag("include-defaults", "If you set this flag, all default values are included in the output YAML, regardless of whether you explicitly set the values in the input files.").BoolVar(&c.includeDefaults)
	convertCmd.Flag("verbose", "If you set this flag, the CLI flags and YAML paths from the old configuration that do not exist in the new configuration are printed to stderr. This flag also prints default values that have changed between the old and the new configuration.").Short('v').BoolVar(&c.verbose)
	convertCmd.Flag("gem", "If you set this flag, the tool will convert from Grafana Metrics Enterprise (GEM) v1.7.x to v2.0.0.").BoolVar(&c.gem)
}

func (c *ConfigCommand) convertConfig(_ *kingpin.ParseContext) error {
	yamlContents, flagsFlags, err := c.prepareInputs()
	if err != nil {
		return err
	}

	sourceFactory, targetFactory, mapper := config.DefaultCortexConfig, config.DefaultMimirConfig, config.CortexToMimirMapper()
	if c.gem {
		sourceFactory, targetFactory, mapper = config.DefaultGEM170Config, config.DefaultGEMConfig, config.GEM170ToGEMMapper()
	}

	convertedYAML, flagsFlags, notices, err := config.Convert(yamlContents, flagsFlags, mapper, sourceFactory, targetFactory, c.updateDefaults, c.includeDefaults)
	if err != nil {
		return errors.Wrap(err, "could not convert configuration")
	}

	return c.output(convertedYAML, flagsFlags, notices)
}

func (c *ConfigCommand) prepareInputs() ([]byte, []string, error) {
	var (
		yamlContents []byte
		flags        []string
		err          error
	)

	if c.flagsFile == "" && c.yamlFile == "" {
		return nil, nil, errors.New("provide at least one of --yaml-file or --flags-file")
	}
	yamlContents, err = os.ReadFile(c.yamlFile)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, errors.Wrap(err, "could not read yaml-file")
	}

	flagsContents, err := os.ReadFile(c.flagsFile)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, errors.Wrap(err, "could not read flags-file")
	}
	if len(flagsContents) > 1 {
		for _, flag := range strings.Split(string(flagsContents), "\n") {
			flag = strings.TrimSpace(flag)
			if len(flag) > 0 {
				flags = append(flags, flag)
			}
		}
	}
	return yamlContents, flags, nil
}

func (c *ConfigCommand) output(yamlContents []byte, flags []string, notices config.ConversionNotices) error {
	openFile := func(path string, defaultWriter io.Writer) (io.Writer, func(), error) {
		if path == "" {
			return defaultWriter, func() {}, nil
		}
		outWriter, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return nil, nil, errors.Wrap(err, "could not open "+path)
		}
		closeFn := func() {
			err := outWriter.Close()
			if err != nil {
				_, _ = fmt.Fprint(os.Stderr, err)
			}
		}
		return outWriter, closeFn, nil
	}

	outYAMLWriter, closeFile, err := openFile(c.outYAMLFile, os.Stdout)
	if err != nil {
		return err
	}
	defer closeFile()

	outFlagsWriter, closeFile, err := openFile(c.outFlagsFile, os.Stdout)
	if err != nil {
		return err
	}
	defer closeFile()

	outNoticesWriter, closeFile, err := openFile(c.outNoticesFile, os.Stderr)
	if err != nil {
		return err
	}
	defer closeFile()

	slices.Sort(flags)

	_, err = fmt.Fprintln(outYAMLWriter, string(yamlContents))
	_, err2 := fmt.Fprintln(outFlagsWriter, strings.Join(flags, "\n"))
	err3 := c.writeNotices(notices, outNoticesWriter)

	return multierror.New(err, err2, err3).Err()
}

func (c *ConfigCommand) writeNotices(notices config.ConversionNotices, w io.Writer) error {
	if !c.verbose {
		return nil
	}
	noticesOut := bytes.Buffer{}
	for _, p := range notices.RemovedParameters {
		_, _ = noticesOut.WriteString(fmt.Sprintf("field is no longer supported: %s\n", p))
	}
	for _, f := range notices.RemovedCLIFlags {
		_, _ = noticesOut.WriteString(fmt.Sprintf("flag is no longer supported: -%s \n", f))
	}
	for _, d := range notices.ChangedDefaults {
		oldDefault, newDefault := placeholderIfEmpty(d.OldDefault), placeholderIfEmpty(d.NewDefault)
		_, _ = noticesOut.WriteString(fmt.Sprintf("using a new default for %s: %s (used to be %s)\n", d.Path, newDefault, oldDefault))
	}
	for _, d := range notices.SkippedChangedDefaults {
		oldDefault, newDefault := placeholderIfEmpty(d.OldDefault), placeholderIfEmpty(d.NewDefault)
		_, _ = noticesOut.WriteString(fmt.Sprintf("default value for %s changed: %s (used to be %s); not updating\n", d.Path, newDefault, oldDefault))
	}
	for _, d := range notices.PrunedDefaults {
		_, _ = noticesOut.WriteString(fmt.Sprintf("removed default value %s: %s\n", d.Path, placeholderIfEmpty(d.Value)))
	}

	_, err := noticesOut.WriteTo(w)
	return err
}

func placeholderIfEmpty(s string) string {
	if s == "" {
		return `""`
	}
	return s
}
