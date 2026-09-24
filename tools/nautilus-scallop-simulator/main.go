// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/grafana/mimir/pkg/nautilus/scallop"
)

func main() {
	outputDirectory := flag.String("output-dir", "scallop-results", "Directory for JSON and CSV search reports.")
	flag.Parse()

	fixtures, err := loadEmbeddedFixtures()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	policy := scallop.DefaultPolicy()
	policy.MaxActions = 4
	result, err := runWeightSearch(fixtures, policy, defaultSearchConfig())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := writeReports(*outputDirectory, result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(recommendationSummary(result))
}
