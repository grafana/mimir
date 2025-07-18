// This is a utility function to generate PromQL queries from a set of label sets
// The label sets can be provided in a file or retrieved from a Prometheus server
// The queries are generated using the promqlsmith library
// The queries are written to stdout

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

// command line flags
var (
	// Flag to indicate if we are using a file or Prometheus server to get the label sets
	srcFlag = "series.source"

	// Flag for how many queries to generate
	promqlCountFlag = "promql.count"

	// Flag for the file containing the label sets json data
	labelsFileFlag = "labels.file"

	// Flag for the Prometheus server address
	prometheusAddressFlag = "prometheus.address"

	// Flag for the Prometheus matcher
	matcherFlag = "prometheus.matcher"

	// Maximum number of queries to generate
	maxCount = 10000
)

// optionally print an error message, print usage and exit(1)
func printUsageAndExit(errorFmt string, args ...any) {
	if len(errorFmt) > 0 {
		fmt.Printf("error: "+errorFmt, args)
	}
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	count := flag.Int(promqlCountFlag, 100, fmt.Sprintf("The number of PromQL queries to generate (0 < count < %d)", maxCount))
	labelsFile := flag.String(labelsFileFlag, "sample_series.js", "Label set samples file (json)")
	prometheusAddress := flag.String(prometheusAddressFlag, "http://127.0.0.1:9009/prometheus", "Prometheus address")
	matcher := flag.String(matcherFlag, "{job=\"prometheus\"}", "Prometheus matcher")
	labelsSource := flag.String(srcFlag, "", "Label set source - file|prometheus")

	flag.Parse()

	if len(*labelsSource) == 0 || *count <= 0 || *count > maxCount {
		printUsageAndExit("")
	}

	var series []model.LabelSet
	var err error

	if *labelsSource == "file" && len(*labelsFile) > 0 {
		series, err = getLabelSetsFromFile(*labelsFile)
		if err != nil {
			printUsageAndExit("error:failed to parse label set samples file: %s\n", err)
		}
	} else if *labelsSource == "prometheus" && len(*prometheusAddress) > 0 && len(*matcher) > 0 {
		series, err = getLabelSetsFromPrometheus(*prometheusAddress, *matcher)
		if err != nil {
			printUsageAndExit("error:failed to get label set from Prometheus: %s\n", err)
		}

	} else {
		printUsageAndExit("")
	}

	if err := generatePromQL(*count, series); err != nil {
		os.Exit(1)
	}
}

// Attempt to get label sets from Prometheus server - gets all label sets which matches the given matcher
// address is the fully qualified path and endpoint of the Prometheus server
// matcher is a string like {job=\"prometheus\"}
// Note - the matcher will only find a label set used in the last 2 hours
func getLabelSetsFromPrometheus(address string, matcher string) ([]model.LabelSet, error) {
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

// Construct a []model.LabelSet from a json file defining the label sets
func getLabelSetsFromFile(seriesFile string) ([]model.LabelSet, error) {
	data, err := os.ReadFile(seriesFile)
	if err != nil {
		return nil, err
	}

	var result []map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	labels := make([]model.LabelSet, len(result))

	for _, item := range result {
		labelSet := make(model.LabelSet)
		for k, v := range item {
			labelSet[model.LabelName(k)] = model.LabelValue(v)
		}
		labels = append(labels, labelSet)
	}

	if len(labels) == 0 {
		return nil, errors.New("no label sets found")
	}

	return labels, nil
}

// Generate the PromQL queries and write to stdout
func generatePromQL(count int, labelSets []model.LabelSet) error {

	now := time.Now()
	rnd := rand.New(rand.NewSource(now.Unix()))
	opts := []promqlsmith.Option{
		promqlsmith.WithEnableOffset(true),
		promqlsmith.WithEnableAtModifier(true),
		promqlsmith.WithEnabledFunctions(getAvailableFunctions()),
		promqlsmith.WithEnabledBinOps(enabledBinops),
		promqlsmith.WithEnableVectorMatching(true),
	}
	ps := promqlsmith.New(rnd, modelLabelSetToLabels(labelSets), opts...)

	for i := 0; i < count; i++ {

		expr := ps.WalkInstantQuery()
		query := expr.Pretty(0)

		fmt.Printf("%s\n", strings.ReplaceAll(query, "\n", ""))

	}
	return nil
}

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

// Get available functions - add entries to unsupportedFunctions map to exclude them
func getAvailableFunctions() []*parser.Function {
	res := make([]*parser.Function, 0)
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
		res = append(res, f)
	}
	return res
}
