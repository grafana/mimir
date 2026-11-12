// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/upstreamtestdata"
	"github.com/grafana/mimir/pkg/util/fs"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	testsDir, err := filepath.Abs(filepath.Join(".", "..", "..", "pkg", "streamingpromql", "testdata", "upstream"))
	if err != nil {
		return fmt.Errorf("could not determine tests directory: %w", err)
	}

	if ok, err := fs.DirExists(testsDir); !ok {
		return fmt.Errorf("tests directory '%v' does not exist: %w", testsDir, err)
	}

	testFiles, err := filepath.Glob(filepath.Join(testsDir, "*.test*"))
	if err != nil {
		return fmt.Errorf("could not list test files in '%v': %w", testsDir, err)
	}

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	if err != nil {
		return fmt.Errorf("could not create planner: %w", err)
	}

	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	if err != nil {
		return fmt.Errorf("could not create engine: %w", err)
	}

	for _, testFile := range testFiles {
		fmt.Printf("Checking %v\n", testFile)

		entireFileIsDisabled := strings.HasSuffix(testFile, ".disabled")
		var disabledTests []disabledTest
		var err error

		if entireFileIsDisabled {
			disabledTests, err = getAllTests(testFile)
		} else {
			disabledTests, err = getDisabledTests(testFile)
		}

		if err != nil {
			return fmt.Errorf("error reading '%v': %w", testFile, err)
		}

		if err := checkForSupportedTests(disabledTests, engine); err != nil {
			return fmt.Errorf("error checking '%v': %w", testFile, err)
		}
	}

	fmt.Println("Done!")
	return nil
}

type disabledTest struct {
	evalLine   string
	lineNumber int
}

func getDisabledTests(testFile string) ([]disabledTest, error) {
	fileBytes, err := os.ReadFile(testFile)
	if err != nil {
		return nil, err
	}

	fileContents := string(fileBytes)
	lines := strings.Split(fileContents, "\n")
	var disabledTests []disabledTest

	for lineIdx, line := range lines {
		if line == upstreamtestdata.UnsupportedMarker {
			testLineNumber := lineIdx + 2
			testLine := strings.TrimSpace(strings.TrimPrefix(lines[lineIdx+1], "#"))
			if !upstreamtestdata.IsEval(testLine) {
				return nil, fmt.Errorf("could not parse test on line %v (%v)", testLineNumber, testLine)
			}

			disabledTests = append(disabledTests, disabledTest{evalLine: testLine, lineNumber: testLineNumber})
		}
	}

	return disabledTests, nil
}

func getAllTests(testFile string) ([]disabledTest, error) {
	fileBytes, err := os.ReadFile(testFile)
	if err != nil {
		return nil, err
	}

	fileContents := string(fileBytes)
	lines := strings.Split(fileContents, "\n")
	var disabledTests []disabledTest

	for lineIdx, line := range lines {
		if !strings.HasPrefix(line, "eval") {
			continue
		}

		testLineNumber := lineIdx + 1
		testLine := strings.TrimSpace(strings.TrimPrefix(line, "#"))
		if !upstreamtestdata.IsEval(testLine) {
			return nil, fmt.Errorf("could not parse test on line %v (%v)", testLineNumber, testLine)
		}

		disabledTests = append(disabledTests, disabledTest{evalLine: testLine, lineNumber: testLineNumber})
	}

	return disabledTests, nil
}

func checkForSupportedTests(tests []disabledTest, engine promql.QueryEngine) error {
	for _, test := range tests {
		switch result, err := upstreamtestdata.ClassifyEval(context.Background(), engine, test.evalLine); result {
		case upstreamtestdata.BuildOK:
			fmt.Printf("> Disabled test case on line %v (%v) is supported!\n", test.lineNumber, test.evalLine)
		case upstreamtestdata.BuildError:
			fmt.Printf("> Warning: could not check disabled test case on line %v (%v): %v\n", test.lineNumber, test.evalLine, err)
		case upstreamtestdata.BuildNotEval:
			fmt.Printf("> Warning: could not parse disabled test case on line %v (%v)\n", test.lineNumber, test.evalLine)
		}
		// BuildUnsupported is the expected case for a disabled test; nothing to report.
	}

	return nil
}
