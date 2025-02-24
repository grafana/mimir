// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/promqltest/test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/util/fs"
)

var (
	// These expressions are taken directly from the promqltest package, and are what it uses to parse eval commands.
	patEvalInstant = regexp.MustCompile(`^eval(?:_(fail|warn|info|ordered))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$`)
	patEvalRange   = regexp.MustCompile(`^eval(?:_(fail|warn|info))?\s+range\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+?)\s+(.+)$`)
)

func main() {
	parser.EnableExperimentalFunctions = true // Silence parsing errors due to the use of experimental functions.

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

	engine, err := streamingpromql.NewEngine(streamingpromql.NewTestEngineOpts(), streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
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
	isInstant  bool
	expr       string
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
		if line == "# Unsupported by streaming engine." {
			testLineNumber := lineIdx + 2
			testLine := strings.TrimSpace(strings.TrimPrefix(lines[lineIdx+1], "#"))
			test, err := parseDisabledTest(testLine, testLineNumber)
			if err != nil {
				return nil, err
			}

			disabledTests = append(disabledTests, test)
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
		test, err := parseDisabledTest(testLine, testLineNumber)
		if err != nil {
			return nil, err
		}

		disabledTests = append(disabledTests, test)
	}

	return disabledTests, nil
}

func parseDisabledTest(line string, lineNumber int) (disabledTest, error) {
	instantParts := patEvalInstant.FindStringSubmatch(line)
	rangeParts := patEvalRange.FindStringSubmatch(line)

	if instantParts == nil && rangeParts == nil {
		return disabledTest{}, fmt.Errorf("could not parse test on line %v (%v)", lineNumber, line)
	}

	if instantParts != nil {
		return disabledTest{
			isInstant:  true,
			expr:       instantParts[3],
			lineNumber: lineNumber,
		}, nil
	}

	return disabledTest{
		isInstant:  false,
		expr:       rangeParts[5],
		lineNumber: lineNumber,
	}, nil
}

func checkForSupportedTests(tests []disabledTest, engine promql.QueryEngine) error {
	for _, test := range tests {
		var q promql.Query
		var err error

		if test.isInstant {
			q, err = engine.NewInstantQuery(context.Background(), nil, nil, test.expr, timestamp.Time(0))
		} else {
			q, err = engine.NewRangeQuery(context.Background(), nil, nil, test.expr, timestamp.Time(0), timestamp.Time(1000), time.Millisecond)
		}

		if err == nil {
			q.Close()
			fmt.Printf("> Disabled test case on line %v (%v) is supported!\n", test.lineNumber, test.expr)
		} else if err != nil && !errors.Is(err, compat.NotSupportedError{}) {
			fmt.Printf("> Warning: could not check disabled test case on line %v (%v): %v\n", test.lineNumber, test.expr, err)
		}
	}

	return nil
}
