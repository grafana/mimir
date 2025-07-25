// SPDX-License-Identifier: AGPL-3.0-only

/*
Promql generator generates random PromQL queries for given label sets.

The label sets can be statically defined in a configuration file or can be obtained by connecting to a running Prometheus server.

The PromQL queries are generated using the promqlsmith library.

The generated queries are written to stdout, one query per line.

Usage:

promql-generator -series.source=file -labels.file=<labels_file.js> [-promql.count=<int>]

or

promql-generator -series.source=prometheus -prometheus.address=<url> -prometheus.matcher=<matcher> [-promql.count=<int>]

The flag variables are;

  - -labels.file=<labels_file.js> (required for -series.source=file, default=sample_series.js) - a file which contains a json representation of 1 or more label sets. An example file is provided.
  - -prometheus.address=<url> (required for -series.source=prometheus, default=http://127.0.0.1:9009/prometheus) - a fully qualified URL to a Prometheus server.
  - -prometheus.matcher=<matcher> (required for -series.source=prometheus, default={job="prometheus"}) - a matcher to limit the label sets discovered from the Prometheus server.
  - -promql.count=<int> (optional default=100) - the number of queries generated. 0 < count < 10000
*/
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/cortexproject/promqlsmith"
	"github.com/efficientgo/core/errors"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"
)

var (
	// add any functions to this map to exclude them from the generated queries
	unsupportedFunctions = map[string]struct{}{}

	enabledBinops = []parser.ItemType{
		parser.SUB,
		parser.ADD,
		parser.MUL,
		parser.MOD,
		parser.DIV,
		parser.EQLC,
		parser.NEQ,
		parser.LTE,
		parser.GTE,
		parser.LSS,
		parser.GTR,
		parser.POW,
		parser.LAND,
		parser.LOR,
		parser.LUNLESS,
	}
)

// A configOption holds a definition of a command line flag option
type configOption[T string | int] struct {
	// The CLI flag string
	cliFlag string
	// A usage description
	usage string
	// A default value
	defaultValue T
	// The value user input once parsed
	flag *T
}

// A configOptionInt is an extension of configOption and adds in a maxValue attribute
type configOptionInt struct {
	config   configOption[int]
	maxValue int
}

// Our default and max PronQL query counts which a user can select
const maxQueryCount int = 10000
const defQueryCount int = 1000

// A config holds all our command line options and running config
type config struct {
	// Where we read the label sets from - file or prometheus
	labelsSource configOption[string]
	// The number of queries we want generated
	promqlCount configOptionInt
	// The file where we read label sets from
	labelsFile configOption[string]
	// The fully qualified Prometheus server URL
	prometheusUrl configOption[string]
	// The label set matcher we use to limit the label sets discovered on the Prometheus server
	prometheusMatcher configOption[string]
}

var cfg config = config{
	labelsSource:      configOption[string]{cliFlag: "series.source", usage: "(required) Label set source - file|prometheus", defaultValue: ""},
	labelsFile:        configOption[string]{cliFlag: "labels.file", usage: "(required for source=file) Label sets file (json)", defaultValue: "sample_series.js"},
	prometheusUrl:     configOption[string]{cliFlag: "prometheus.address", usage: "(required for source=prometheus) Fully qualified Prometheus URL", defaultValue: "http://127.0.0.1:9009/prometheus"},
	prometheusMatcher: configOption[string]{cliFlag: "prometheus.matcher", usage: "(required for source=prometheus) Prometheus label set matcher", defaultValue: "{job=\"prometheus\"}"},
	promqlCount:       configOptionInt{config: configOption[int]{cliFlag: "promql.count", usage: fmt.Sprintf("(optional) The number of PromQL queries to generate (0 < count < %d)", maxQueryCount), defaultValue: defQueryCount}, maxValue: maxQueryCount},
}

// register will register a command line flag for the configOption
func (option *configOption[T]) register() {
	switch any(option.defaultValue).(type) {
	case string:
		strFlag := flag.String(option.cliFlag, any(option.defaultValue).(string), option.usage)
		option.flag = any(strFlag).(*T)
	case int:
		intFlag := flag.Int(option.cliFlag, any(option.defaultValue).(int), option.usage)
		option.flag = any(intFlag).(*T)
	}
}

// validate the input for a given configOption - check that the value is not empty
// Note - there is no implementation for the configOption[int] as this is handled by a function on configOptionInt
func (option *configOption[T]) validate() error {
	switch any(option.defaultValue).(type) {
	case string:
		str := any(*option.flag).(string)
		if len(str) == 0 {
			return errors.New(fmt.Sprintf("%s is required", option.cliFlag))
		}
	}
	return nil
}

// validate the input for the given configOptionInt - check that the value is within acceptable range bounds
func (option *configOptionInt) validate() error {
	if *option.config.flag <= 0 || *option.config.flag > option.maxValue {
		return errors.New(fmt.Sprintf("%s is required and must be 0 < val < %d", option.config.cliFlag, option.maxValue))
	}
	return nil
}

// registerFlags will register all our command line flags
func (cfg *config) registerFlags() {
	cfg.labelsSource.register()
	cfg.promqlCount.config.register()
	cfg.labelsFile.register()
	cfg.prometheusMatcher.register()
	cfg.prometheusUrl.register()
}

// validate will validate the input flags and the overall running configuration options
func (cfg *config) validate() error {
	var err error
	err = cfg.labelsSource.validate()
	if err != nil {
		return err
	}
	err = cfg.promqlCount.validate()
	if err != nil {
		return err
	}
	if *cfg.labelsSource.flag == "file" {
		err = cfg.labelsFile.validate()
		if err != nil {
			return err
		}
	} else if *cfg.labelsSource.flag == "prometheus" {
		err = cfg.prometheusUrl.validate()
		if err != nil {
			return err
		}
		err = cfg.prometheusMatcher.validate()
		if err != nil {
			return err
		}

	} else {
		return errors.New(fmt.Sprintf("%s must be file|prometheus", *cfg.labelsSource.flag))
	}

	return nil
}

// printUsageAndExit prints an optional error message and then print usage and exit(1)
func printUsageAndExit(errorIfAny error) {
	if errorIfAny != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n\n", errorIfAny.Error())
	}
	fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	flag.CommandLine.SetOutput(os.Stderr)

	cfg.registerFlags()

	var err error
	var series []model.LabelSet

	if err = flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		printUsageAndExit(err)
	}

	err = cfg.validate()
	if err != nil {
		printUsageAndExit(err)
	}

	if *cfg.labelsSource.flag == "file" {
		series, err = labelSetsFromFile(*cfg.labelsFile.flag)
	} else {
		series, err = labelSetsFromPrometheus(*cfg.prometheusUrl.flag, *cfg.prometheusMatcher.flag)
	}

	if err != nil {
		printUsageAndExit(err)
	}

	if err := generatePromQL(*cfg.promqlCount.config.flag, series); err != nil {
		os.Exit(1)
	}
}

// labelSetsFromPrometheus attempt to get label sets from Prometheus server - gets all label sets which matches the given matcher
// address is the fully qualified URL of the Prometheus server
// matcher is a string like {job=\"prometheus\"}
// Note - the matcher will only find a label set used in the last 2 hours
func labelSetsFromPrometheus(address string, matcher string) ([]model.LabelSet, error) {
	client, err := api.NewClient(api.Config{
		Address: address,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "create Prometheus client")
	}
	promAPI := v1.NewAPI(client)
	ctx := context.Background()
	now := time.Now()

	labels, _, err := promAPI.Series(
		ctx,
		[]string{matcher},
		now.Add(-2*time.Hour),
		now,
	)

	if err != nil {
		return nil, errors.Wrapf(err, "get label sets")
	}

	if len(labels) == 0 {
		return nil, errors.New("no matching label sets found")
	}

	return labels, nil
}

// labelSetsFromFile constructs a []model.LabelSet from a json file defining the label sets
func labelSetsFromFile(seriesFile string) ([]model.LabelSet, error) {
	data, err := os.ReadFile(seriesFile)
	if err != nil {
		return nil, err
	}

	var result []map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("no label sets found")
	}

	labels := make([]model.LabelSet, len(result))

	for i, item := range result {
		labelSet := make(model.LabelSet)
		for k, v := range item {
			labelSet[model.LabelName(k)] = model.LabelValue(v)
		}
		labels[i] = labelSet
	}

	return labels, nil
}

// generatePromQL generate the random PromQL queries for the given label sets and writes to stdout
func generatePromQL(count int, labelSets []model.LabelSet) error {

	now := time.Now()
	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(availableFunctions()),
		promqlsmith.WithEnabledBinOps(enabledBinops),
		promqlsmith.WithEnableVectorMatching(true),
	}
	ps := promqlsmith.New(rnd, modelLabelSetToLabels(labelSets), opts...)

	for range count {

		expr := ps.WalkInstantQuery()
		query := expr.Pretty(0)

		fmt.Printf("%s\n", strings.ReplaceAll(query, "\n", ""))

	}
	return nil
}

// modelLabelSetToLabels converts []model.LabelSet to []labels.Labels
func modelLabelSetToLabels(labelSets []model.LabelSet) []labels.Labels {
	out := make([]labels.Labels, len(labelSets))
	bufLabels := labels.EmptyLabels()
	builder := labels.NewBuilder(bufLabels)
	for i, lbls := range labelSets {
		for k, v := range lbls {
			builder.Set(string(k), string(v))
		}
		out[i] = builder.Labels()
		builder.Reset(bufLabels)
	}
	return out
}

// getAvailableFunctions returns a []*parser.Function excluding functions listed in the unsupportedFunctions map
func availableFunctions() []*parser.Function {
	var results []*parser.Function
	for _, f := range parser.Functions {
		if f.Variadic != 0 {
			continue
		}
		if slices.Contains(f.ArgTypes, parser.ValueTypeString) {
			continue
		}
		if _, ok := unsupportedFunctions[f.Name]; ok {
			continue
		}
		results = append(results, f)
	}
	return results
}
