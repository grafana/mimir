// SPDX-License-Identifier: AGPL-3.0-only

// Command disable-failing-upstream-promql-tests comments out every enabled upstream PromQL test case
// that Mimir's engine does not run successfully - unsupported feature or divergent result - so that
// TestUpstreamTestCases stays green after a mimir-prometheus bump. It is the second half of the
// automated vendoring sync (run after sync-upstream-promql-tests).
//
// It runs the existing TestUpstreamTestCases once with `go test -json`, reads the failing per-case
// subtests (named ".../<file>/line_<N>/<expr>") to learn exactly which eval commands failed, and
// comments those blocks out. A single run reports every failing case, so there is no per-case
// isolation or iteration.
//
// Because it disables on any failure - including a wrong result that could be a real regression -
// disabled cases are split by cause (unsupported vs divergent) and, when MIMIR_SYNC_BASELINE_DIR
// points at the pre-sync copies, by origin (new upstream case vs previously-passing case). The list
// is written to MIMIR_SYNC_REPORT (for the PR description) and MIMIR_SYNC_DISABLED (for the owner
// notification), and the vendoring PR stays behind human approval.
//
// Run it with `go run .` in this directory, or via `make disable-failing-upstream-promql-tests`.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/grafana/regexp"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/upstreamtestdata"
)

const comparisonsPkg = "github.com/grafana/mimir/pkg/streamingpromql/comparisons"

// failLineRE extracts the test file and 1-based line of a failing eval command from a go-test subtest
// name such as "TestUpstreamTestCases/upstream/info.test/line_433/info(...)".
var failLineRE = regexp.MustCompile(`/upstream/(.+?)/line_(\d+)(?:/|$)`)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		return err
	}
	testdataDir := filepath.Join(repoRoot, "pkg", "streamingpromql", "testdata", "upstream")

	// Run the upstream test cases once and collect the failing eval commands per file.
	out, testFailed, err := runUpstreamTests(repoRoot)
	if err != nil {
		return err
	}
	failures := parseFailures(out)

	if len(failures) == 0 {
		if testFailed {
			return fmt.Errorf("go test reported failures but no eval-case failures were found "+
				"(build error, or promqltest subtest-name format changed); output:\n%s", out)
		}
		fmt.Println("All upstream test cases pass; nothing to disable.")
		return nil
	}

	engine, err := newEngine()
	if err != nil {
		return err
	}
	ctx := context.Background()
	baselineDir := os.Getenv("MIMIR_SYNC_BASELINE_DIR")

	var unsupported, divergentNew, divergentExisting, divergentUnknown []string

	for _, file := range slices.Sorted(maps.Keys(failures)) {
		path := filepath.Join(testdataDir, file)
		contentBytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		content := string(contentBytes)
		lines := strings.Split(content, "\n")
		lineSet := failures[file]

		newContent, unmatched := upstreamtestdata.CommentOutEvals(content, lineSet)
		if len(unmatched) > 0 {
			return fmt.Errorf("in %s, failing lines %v did not correspond to eval commands "+
				"(promqltest subtest-name format may have changed)", file, unmatched)
		}

		baselineEvals := loadBaselineEnabledEvals(baselineDir, file)
		for _, line := range slices.Sorted(maps.Keys(lineSet)) {
			evalLine := strings.TrimSpace(lines[line-1])
			entry := fmt.Sprintf("- `%s` line %d: `%s`", file, line, evalLine)
			result, _ := upstreamtestdata.ClassifyEval(ctx, engine, evalLine)
			switch {
			case result == upstreamtestdata.BuildUnsupported:
				unsupported = append(unsupported, entry)
			case baselineEvals == nil:
				divergentUnknown = append(divergentUnknown, entry)
			case baselineEvals[evalLine]:
				divergentExisting = append(divergentExisting, entry)
			default:
				divergentNew = append(divergentNew, entry)
			}
		}

		if err := os.WriteFile(path, []byte(newContent), 0o644); err != nil {
			return err
		}
		fmt.Printf("Disabled %d case(s) in %s\n", len(lineSet), file)
	}

	list := renderDisabledCases(unsupported, divergentExisting, divergentNew, divergentUnknown)
	if err := appendToFileEnv("MIMIR_SYNC_REPORT", "### Auto-disabled upstream test cases\n\n"+list); err != nil {
		return err
	}
	return appendToFileEnv("MIMIR_SYNC_DISABLED", list)
}

// runUpstreamTests runs TestUpstreamTestCases with JSON output. It returns the captured stdout,
// whether the test reported failures (expected - those are the cases to disable), and an error only
// if go test could not be run at all.
func runUpstreamTests(repoRoot string) (stdout []byte, testFailed bool, err error) {
	cmd := exec.Command("go", "test", "-json", "-count=1", "-run", "^TestUpstreamTestCases$", comparisonsPkg)
	cmd.Dir = repoRoot
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf

	err = cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return nil, false, fmt.Errorf("running go test: %w\n%s", err, errBuf.String())
		}
		// A non-zero exit just means some cases failed, which is the input we want.
		return out.Bytes(), true, nil
	}
	return out.Bytes(), false, nil
}

func parseFailures(out []byte) map[string]map[int]bool {
	failures := make(map[string]map[int]bool)
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var ev struct {
			Action string
			Test   string
		}
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue // non-JSON line
		}
		if ev.Action != "fail" || ev.Test == "" {
			continue
		}
		m := failLineRE.FindStringSubmatch(ev.Test)
		if m == nil {
			continue
		}
		line, err := strconv.Atoi(m[2])
		if err != nil {
			continue
		}
		if failures[m[1]] == nil {
			failures[m[1]] = make(map[int]bool)
		}
		failures[m[1]][line] = true
	}
	return failures
}

func newEngine() (*streamingpromql.Engine, error) {
	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	if err != nil {
		return nil, fmt.Errorf("could not create planner: %w", err)
	}
	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	if err != nil {
		return nil, fmt.Errorf("could not create engine: %w", err)
	}
	return engine, nil
}

// loadBaselineEnabledEvals returns the set of enabled eval command lines in the pre-sync copy of the
// named file, used to tell whether a failing case existed before this bump. It returns nil when no
// baseline is available (origin can't be determined), and an empty set when the file is new upstream
// (so every case in it counts as newly synced).
func loadBaselineEnabledEvals(baselineDir, name string) map[string]bool {
	if baselineDir == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(baselineDir, name))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]bool{}
	}
	if err != nil {
		return map[string]bool{}
	}

	set := make(map[string]bool)
	for line := range strings.SplitSeq(string(b), "\n") {
		trimmed := strings.TrimSpace(line)
		if upstreamtestdata.IsEval(trimmed) {
			set[trimmed] = true
		}
	}
	return set
}

func renderDisabledCases(unsupported, divergentExisting, divergentNew, divergentUnknown []string) string {
	var b strings.Builder
	section := func(title string, items []string) {
		if len(items) == 0 {
			return
		}
		fmt.Fprintf(&b, "**%s:**\n\n%s\n\n", title, strings.Join(items, "\n"))
	}
	// Existing cases first: a previously-passing case that now fails is the most likely real regression.
	section("Divergent result or runtime error in EXISTING cases (possible regression, please review carefully)", divergentExisting)
	section("Unsupported by Mimir's engine (feature not implemented)", unsupported)
	section("Divergent result or runtime error in NEWLY-SYNCED upstream cases", divergentNew)
	section("Divergent result or runtime error (origin unknown)", divergentUnknown)
	return b.String()
}

func appendToFileEnv(envVar, content string) error {
	path := os.Getenv(envVar)
	if path == "" || content == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}
