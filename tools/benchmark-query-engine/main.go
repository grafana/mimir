// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/grafana/regexp"

	"github.com/grafana/mimir/pkg/streamingpromql/benchmarks"
)

const benchmarkName = "BenchmarkQuery"

func main() {
	app := &app{}
	if err := app.run(); err != nil {
		slog.Error("unexpected error", "err", err)
		os.Exit(1)
	}
}

type app struct {
	benchmarkPackageDir string
	tempDir             string
	dataDir             string
	binaryPath          string
	ingesterAddress     string
	cleanup             func()

	count      uint
	testFilter string
	listTests  bool
}

func (a *app) run() error {
	a.parseArgs()

	if a.listTests {
		a.printTests()
		return nil
	}

	// Do this early, to avoid doing a bunch of pointless work if the regex is invalid or doesn't match any tests.
	filteredNames, err := a.filteredTestCaseNames()
	if err != nil {
		return err
	}

	if err := a.findBenchmarkPackageDir(); err != nil {
		return fmt.Errorf("could not find engine package directory: %w", err)
	}

	if err := a.createTempDir(); err != nil {
		return fmt.Errorf("could not create temporary directory: %w", err)
	}

	defer os.RemoveAll(a.tempDir)

	if err := a.buildBinary(); err != nil {
		return fmt.Errorf("building binary failed: %w", err)
	}

	if err := a.validateBinary(); err != nil {
		return fmt.Errorf("benchmark binary failed validation: %w", err)
	}

	if err := a.startIngesterAndLoadData(); err != nil {
		return fmt.Errorf("starting ingester and loading data failed: %w", err)
	}
	defer a.cleanup()

	haveRunAnyTests := false

	for _, name := range filteredNames {
		for i := uint(0); i < a.count; i++ {
			if err := a.runTestCase(name, !haveRunAnyTests); err != nil {
				return fmt.Errorf("running test case '%v' failed: %w", name, err)
			}

			haveRunAnyTests = true
		}
	}

	slog.Info("benchmarks completed successfully, cleaning up...")

	return nil
}

func (a *app) parseArgs() {
	flag.UintVar(&a.count, "count", 1, "run each benchmark n times")
	flag.StringVar(&a.testFilter, "bench", ".", "only run benchmarks matching regexp")
	flag.BoolVar(&a.listTests, "list", false, "list known benchmarks and exit")
	flag.Parse()

	if len(flag.Args()) > 0 {
		fmt.Printf("Received unexpected arguments: %v\n", flag.Args())
		flag.Usage()
		os.Exit(1)
	}
}

func (a *app) findBenchmarkPackageDir() error {
	path, err := filepath.Abs(filepath.Join("..", "..", "pkg", "streamingpromql", "benchmarks"))
	if err != nil {
		return fmt.Errorf("resolving path to engine benchmark package directory failed: %w", err)
	}

	if _, err := os.Stat(filepath.Join(path, "comparison_test.go")); err != nil {
		return fmt.Errorf("'%v' does not appear to contain the streaming query engine benchmarks package: %w", path, err)
	}

	a.benchmarkPackageDir = path
	return nil
}

func (a *app) createTempDir() error {
	var err error
	a.tempDir, err = os.MkdirTemp("", "mimir-query-engine-benchmarks")
	if err != nil {
		return err
	}

	slog.Info("created temporary directory", "dir", a.tempDir)

	a.dataDir = filepath.Join(a.tempDir, "data")
	if err := os.Mkdir(a.dataDir, 0777); err != nil {
		return fmt.Errorf("could not create data directory '%v': %w", a.dataDir, err)
	}

	return nil
}

func (a *app) buildBinary() error {
	a.binaryPath = filepath.Join(a.tempDir, "benchmark-binary")

	cmd := exec.Command("go", "test", "-c", "-o", a.binaryPath, "-tags", "stringlabels", ".")
	cmd.Dir = a.benchmarkPackageDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("starting %v failed: %w", cmd.Args, err)
	}

	slog.Info("built binary", "path", a.binaryPath)

	return nil
}

// Ensure the benchmark hasn't been moved or renamed.
func (a *app) validateBinary() error {
	slog.Info("validating binary...")

	buf := &bytes.Buffer{}
	cmd := exec.Command(a.binaryPath, "-test.list", ".")
	cmd.Dir = a.benchmarkPackageDir
	cmd.Stdout = buf
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("starting %v failed: %w", cmd.Args, err)
	}

	output := buf.String()
	lines := strings.Split(output, "\n")
	if !slices.Contains(lines, benchmarkName) {
		return fmt.Errorf("expected benchmark binary to have a test named '%v', but it does not", benchmarkName)
	}

	return nil
}

func (a *app) startIngesterAndLoadData() error {
	slog.Info("starting ingester and loading data...")

	address, cleanup, err := benchmarks.StartIngesterAndLoadData(a.dataDir, benchmarks.MetricSizes)
	if err != nil {
		return err
	}

	a.ingesterAddress = address
	a.cleanup = cleanup

	slog.Info("loading data complete")

	return nil
}

func (a *app) printTests() {
	for _, name := range a.allTestCaseNames() {
		println(name)
	}
}

// Why do this, rather than call 'go test -list'?
// 'go test -list' only lists top-level benchmarks (eg. "BenchmarkQuery"),
// but doesn't list sub-tests.
func (a *app) allTestCaseNames() []string {
	cases := benchmarks.TestCases(benchmarks.MetricSizes)
	names := make([]string, 0, 2*len(cases))

	for _, c := range cases {
		names = append(names, benchmarkName+"/"+c.Name()+"/streaming")
		names = append(names, benchmarkName+"/"+c.Name()+"/standard")
	}

	return names
}

func (a *app) filteredTestCaseNames() ([]string, error) {
	regex, err := regexp.Compile(a.testFilter)
	if err != nil {
		return nil, fmt.Errorf("invalid regexp '%v': %w", a.testFilter, err)
	}

	all := a.allTestCaseNames()
	names := make([]string, 0, len(all))

	for _, name := range all {
		if regex.MatchString(name) {
			names = append(names, name)
		}
	}

	if len(names) == 0 {
		return nil, fmt.Errorf("regexp '%v' matched no benchmark cases, run with -list to see all available benchmark cases", a.testFilter)
	}

	return names, nil
}

func (a *app) runTestCase(name string, printBenchmarkHeader bool) error {
	cmd := exec.Command(a.binaryPath, "-test.bench="+regexp.QuoteMeta(name), "-test.benchmem")
	cmd.Stdout = &goBenchOutputFilteringWriter{printBenchmarkHeader: printBenchmarkHeader, destination: os.Stdout}
	cmd.Stderr = os.Stderr
	cmd.Env = append(cmd.Env, "STREAMING_PROMQL_ENGINE_BENCHMARK_INGESTER_ADDR="+a.ingesterAddress)
	cmd.Env = append(cmd.Env, "STREAMING_PROMQL_ENGINE_BENCHMARK_SKIP_COMPARE_RESULTS=true")

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("executing command failed: %w", err)
	}

	usage := cmd.ProcessState.SysUsage().(*syscall.Rusage)

	fmt.Printf("     %v B\n", usage.Maxrss)

	return nil
}

type goBenchOutputFilteringWriter struct {
	printBenchmarkHeader bool
	destination          io.Writer
	buf                  bytes.Buffer
}

func (g *goBenchOutputFilteringWriter) Write(p []byte) (int, error) {
	if _, err := g.buf.Write(p); err != nil {
		return 0, err
	}

	if g.buf.Len() == 0 {
		return 0, nil
	}

	// This isn't particularly efficient, but it's good enough for this scenario.
	s := g.buf.String()
	lines := strings.Split(s, "\n")
	lines = lines[:len(lines)-1] // Drop the incomplete line.

	for _, l := range lines {
		b := g.buf.Next(len(l) + 1) // +1 for newline character

		isBenchmarkHeaderLine := strings.HasPrefix(l, "goos") || strings.HasPrefix(l, "goarch") || strings.HasPrefix(l, "pkg")
		isBenchmarkLine := strings.HasPrefix(l, benchmarkName)
		isPassLine := l == "PASS"
		shouldWrite := !isPassLine

		if isBenchmarkHeaderLine {
			shouldWrite = g.printBenchmarkHeader
		}

		if isBenchmarkLine {
			// Drop the trailing newline character so we can append the RSS on the same line in runTestCase above.
			b = b[:len(b)-1]
		}

		if shouldWrite {
			if _, err := g.destination.Write(b); err != nil {
				return 0, err
			}
		}
	}

	return len(p), nil
}
