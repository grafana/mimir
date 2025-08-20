// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/promqlsmith
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

/*
Promql generator generates random PromQL queries for given label sets.

The label sets are statically defined in a configuration file.

The PromQL queries are generated using the promqlsmith library.

The generated queries are written to stdout, one query per line.

Usage:

promql-generator -labels.file=<labels_file.json> [-promql.count=<int>]

See promql-generator --help for further usage information
*/
package main

import (
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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/exp/slices"
)

var (
	// add any functions to this map to exclude them from the generated queries
	unsupportedFunctions = map[string]struct{}{}

	// add any operations to this map to exclude them from the generated queries
	unsupportedBinOps = map[parser.ItemType]bool{}

	allBinOps = []parser.ItemType{
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

// Our default and max PromQL query counts which a user can select
const (
	maxQueryCount int = 10000
)

// A config holds all our command line options and running config
type config struct {
	// The number of queries we want generated
	promqlCount int
	// The file where we read label sets from
	labelsFile string
}

func (cfg *config) registerFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.labelsFile, "labels.file", "sample_series.json", "Label sets file (json).")
	f.IntVar(&cfg.promqlCount, "promql.count", 1000, fmt.Sprintf("(optional) The number of PromQL queries to generate (0 < count < %d)", maxQueryCount))
}

// printUsageAndExit prints an optional error message and then print usage and exit(1)
func printUsageAndExit(f *flag.FlagSet) {
	f.Usage()
	os.Exit(1)
}

func main() {
	flagSet := flag.NewFlagSet("promql-generator", flag.ExitOnError)
	flagSet.SetOutput(os.Stderr)

	cfg := &config{}
	cfg.registerFlags(flagSet)

	var err error
	var series []labels.Labels

	if err = flagext.ParseFlagsWithoutArguments(flagSet); err != nil {
		printUsageAndExit(flagSet)
	}

	if len(cfg.labelsFile) == 0 || cfg.promqlCount <= 0 || cfg.promqlCount > maxQueryCount {
		printUsageAndExit(flagSet)
	}

	if series, err = labelSetsFromFile(cfg.labelsFile); err != nil {
		fmt.Fprintf(os.Stderr, "error parsing label sets file: %v \n", err)
		printUsageAndExit(flagSet)
	}

	if err := generatePromQL(cfg.promqlCount, series); err != nil {
		os.Exit(1)
	}
}

// labelSetsFromFile constructs a []labels.Labels from a json file defining the label sets
func labelSetsFromFile(seriesFile string) ([]labels.Labels, error) {
	var data []byte
	var err error
	var result []map[string]string

	if data, err = os.ReadFile(seriesFile); err != nil {
		return nil, err
	}

	if err = json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, errors.New("no label sets found")
	}

	bufLabels := labels.EmptyLabels()
	builder := labels.NewBuilder(bufLabels)

	labels := make([]labels.Labels, len(result))

	for i, item := range result {

		for k, v := range item {
			builder.Set(string(k), string(v))
		}
		labels[i] = builder.Labels()
		builder.Reset(bufLabels)
	}

	return labels, nil
}

// generatePromQL generate the random PromQL queries for the given label sets and writes to stdout
func generatePromQL(count int, labels []labels.Labels) error {

	enabledBinOps := make([]parser.ItemType, len(allBinOps)-len(unsupportedBinOps))
	idx := 0
	for _, op := range allBinOps {
		ignore, exists := unsupportedBinOps[op]
		if exists && ignore {
			continue
		}
		enabledBinOps[idx] = op
		idx++
	}

	now := time.Now()
	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(availableFunctions()),
		promqlsmith.WithEnabledBinOps(enabledBinOps),
		promqlsmith.WithEnableVectorMatching(true),
	}
	ps := promqlsmith.New(rnd, labels, opts...)

	for range count {

		expr := ps.WalkInstantQuery()
		query := expr.Pretty(0)

		fmt.Printf("%s\n", strings.ReplaceAll(query, "\n", ""))

	}
	return nil
}

// getAvailableFunctions returns a []*parser.Function excluding functions listed in the unsupportedFunctions map
func availableFunctions() []*parser.Function {
	var results []*parser.Function
	for _, f := range parser.Functions {
		// These cases are ignored per the sample promqlsmith code
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
