// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"

	"github.com/grafana/dskit/flagext"
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

	count           uint
	testFilter      string
	listTests       bool
	justRunIngester bool
	cpuProfilePath  string
	memProfilePath  string
}

func (a *app) run() error {
	if err := a.parseArgs(); err != nil {
		return err
	}

	// Do this early, to avoid doing a bunch of pointless work if the regex is invalid or doesn't match any tests.
	filteredNames, err := a.filteredTestCaseNames()
	if err != nil {
		return err
	}

	if a.listTests {
		a.printTests(filteredNames)
		return nil
	}

	if a.cpuProfilePath != "" || a.memProfilePath != "" {
		if a.count != 1 {
			return fmt.Errorf("must run exactly one iteration when emitting profile, but have -count=%d", a.count)
		}

		if len(filteredNames) != 1 {
			return fmt.Errorf("must select exactly one benchmark with -bench when emitting profile, but have %v benchmarks selected", len(filteredNames))
		}
	}

	if err := a.findBenchmarkPackageDir(); err != nil {
		return fmt.Errorf("could not find engine package directory: %w", err)
	}

	if err := a.createTempDir(); err != nil {
		return fmt.Errorf("could not create temporary directory: %w", err)
	}

	defer os.RemoveAll(a.tempDir)

	if err := a.startIngesterAndLoadData(); err != nil {
		return fmt.Errorf("starting ingester and loading data failed: %w", err)
	}
	defer a.cleanup()

	if a.justRunIngester {
		return a.waitForExit()
	}

	if err := a.runBenchmarks(filteredNames); err != nil {
		return err
	}

	return nil
}

func (a *app) runBenchmarks(filteredNames []string) error {
	if err := a.buildBinary(); err != nil {
		return fmt.Errorf("building binary failed: %w", err)
	}

	if err := a.validateBinary(); err != nil {
		return fmt.Errorf("benchmark binary failed validation: %w", err)
	}

	haveRunAnyTests := false

	for _, name := range filteredNames {
		for i := uint(0); i < a.count; i++ {
			if err := a.runTestCase(name, !haveRunAnyTests); err != nil {
				return fmt.Errorf("running test case '%v' failed: %w", name, err)
			}

			haveRunAnyTests = true
		}
	}

	slog.Info("benchmarks completed successfully")
	return nil
}

func (a *app) waitForExit() error {
	// I know it's a bit weird to use string formatting like this when using structured logging, but this produces the clearest message.
	slog.Info(fmt.Sprintf("ingester running, run benchmark-query-engine with -use-existing-ingester=%v", a.ingesterAddress))
	slog.Info("press Ctrl+C to exit")

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done

	println()
	slog.Info("interrupt received, shutting down...")

	return nil
}

func (a *app) parseArgs() error {
	flag.UintVar(&a.count, "count", 1, "run each benchmark n times")
	flag.StringVar(&a.testFilter, "bench", ".", "only run benchmarks matching regexp")
	flag.BoolVar(&a.listTests, "list", false, "list known benchmarks and exit")
	flag.BoolVar(&a.justRunIngester, "start-ingester", false, "start ingester and wait, run no benchmarks")
	flag.StringVar(&a.ingesterAddress, "use-existing-ingester", "", "use existing ingester rather than creating a new one")
	flag.StringVar(&a.cpuProfilePath, "cpuprofile", "", "write CPU profile to file, only supported when running a single iteration of one benchmark")
	flag.StringVar(&a.memProfilePath, "memprofile", "", "write memory profile to file, only supported when running a single iteration of one benchmark")

	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		fmt.Printf("%v\n", err)
		flag.Usage()
		os.Exit(1)
	}

	if a.justRunIngester && a.ingesterAddress != "" {
		return errors.New("cannot specify both '-start-ingester' and an existing ingester address with '-use-existing-ingester'")
	}

	return nil
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
	if a.ingesterAddress != "" {
		slog.Warn("using existing ingester; not checking data required for benchmark is present", "address", a.ingesterAddress)
		a.cleanup = func() {
			// Nothing to do.
		}
		return nil
	}

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

func (a *app) printTests(names []string) {
	for _, name := range names {
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
		names = append(names, benchmarkName+"/"+c.Name()+"/engine=Mimir")
		names = append(names, benchmarkName+"/"+c.Name()+"/engine=Prometheus")
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
	args := []string{
		"-test.bench=" + regexp.QuoteMeta(name), "-test.run=NoTestsWillMatchThisPattern", "-test.benchmem",
	}

	if a.cpuProfilePath != "" {
		args = append(args, "-test.cpuprofile="+a.cpuProfilePath)
	}

	if a.memProfilePath != "" {
		args = append(args, "-test.memprofile="+a.memProfilePath)
	}

	cmd := exec.Command(a.binaryPath, args...)
	buf := &bytes.Buffer{}
	cmd.Stdout = buf
	cmd.Stderr = os.Stderr
	cmd.Env = append(cmd.Env, "MIMIR_PROMQL_ENGINE_BENCHMARK_INGESTER_ADDR="+a.ingesterAddress)
	cmd.Env = append(cmd.Env, "MIMIR_PROMQL_ENGINE_BENCHMARK_SKIP_COMPARE_RESULTS=true")

	if err := cmd.Run(); err != nil {
		slog.Warn("output from failed command", "output", buf.String())
		return fmt.Errorf("executing command failed: %w", err)
	}

	usage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	outputLines := strings.Split(strings.TrimSpace(buf.String()), "\n")

	for _, l := range outputLines {
		isBenchmarkHeaderLine := strings.HasPrefix(l, "goos") || strings.HasPrefix(l, "goarch") || strings.HasPrefix(l, "pkg") || strings.HasPrefix(l, "cpu")
		isBenchmarkLine := strings.HasPrefix(l, benchmarkName)
		isPassLine := l == "PASS"

		if isBenchmarkHeaderLine {
			if printBenchmarkHeader {
				fmt.Println(l)
			}
		} else if isBenchmarkLine {
			fmt.Print(l)
			fmt.Printf("     %v B\n", maxRSSInBytes(usage))
		} else if !isPassLine {
			fmt.Println(l)
		}
	}

	return nil
}

func maxRSSInBytes(usage *syscall.Rusage) int64 {
	switch runtime.GOOS {
	case "linux":
		return usage.Maxrss * 1024 // Maxrss is returned in kilobytes on Linux.
	case "darwin":
		return usage.Maxrss // Maxrss is already in bytes on macOS.
	default:
		panic(fmt.Sprintf("unknown GOOS '%v'", runtime.GOOS))
	}
}
