// SPDX-License-Identifier: AGPL-3.0-only

// Command disable-failing-upstream-promql-tests comments out every enabled upstream PromQL test case
// Mimir's engine fails (unsupported feature or divergent result), keeping TestUpstreamTestCases green
// after a mimir-prometheus bump. Run it after sync-upstream-promql-tests, via `go run .` or
// `make disable-failing-upstream-promql-tests`.
//
// It runs TestUpstreamTestCases once with `go test -json` and comments out the eval commands whose
// per-case subtests ("line_<N>") failed - one run finds them all. Disabled cases are split by cause
// (unsupported vs divergent) and, if MIMIR_SYNC_BASELINE_REF names a commit from before the sync, by
// origin (existing, new, or previously disabled), then printed and also written to MIMIR_SYNC_REPORT
// when set. The cases that weren't disabled before are written to MIMIR_SYNC_DISABLED when set.
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

	// Resolve the baseline first, so a mistyped ref fails before the slow test run.
	baselineRef, err := resolveBaselineRef(repoRoot, os.Getenv("MIMIR_SYNC_BASELINE_REF"))
	if err != nil {
		return err
	}

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
	disabledCases := make(map[caseGroup][]string)

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

		baseline, err := loadBaseline(repoRoot, baselineRef, file)
		if err != nil {
			return err
		}
		for _, line := range slices.Sorted(maps.Keys(lineSet)) {
			evalLine := strings.TrimSpace(lines[line-1])
			entry := fmt.Sprintf("- `%s` line %d: `%s`", file, line, evalLine)
			result, _ := upstreamtestdata.ClassifyEval(ctx, engine, evalLine)
			group := caseGroup{origin: baseline.origin(evalLine), unsupported: result == upstreamtestdata.BuildUnsupported}
			disabledCases[group] = append(disabledCases[group], entry)
		}

		if err := os.WriteFile(path, []byte(newContent), 0o644); err != nil {
			return err
		}
		fmt.Printf("Disabled %d case(s) in %s\n", len(lineSet), file)
	}

	list := renderDisabledCases(disabledCases, true)
	// Printed so that someone running this locally can review what was disabled, and why, before pushing.
	fmt.Print("\n" + list)
	if baselineRef == "" {
		fmt.Println("To tell new cases from existing ones, set MIMIR_SYNC_BASELINE_REF to a commit from before the sync.")
	}
	if err := appendToFileEnv("MIMIR_SYNC_REPORT", "### Auto-disabled upstream test cases\n\n"+list); err != nil {
		return err
	}
	// Only the cases that weren't already disabled, so that re-disabling cases doesn't notify anyone.
	return appendToFileEnv("MIMIR_SYNC_DISABLED", renderDisabledCases(disabledCases, false))
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

// origin is where a disabled case came from, judged by its eval command line in the pre-sync copy of
// its file.
type origin int

const (
	originUnknown            origin = iota // No baseline given.
	originExisting                         // Enabled before the sync.
	originNew                              // Not in the file before the sync.
	originPreviouslyDisabled               // Disabled before the sync; re-enabled because upstream changed it or the lines next to it.
)

type caseGroup struct {
	origin      origin
	unsupported bool
}

// resolveBaselineRef resolves ref to a commit ID, so that e.g. HEAD~1 means the same commit for every
// file even if HEAD moves. An empty ref means there is no baseline.
func resolveBaselineRef(repoRoot, ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	// Refuse anything git could take as an option.
	if strings.HasPrefix(ref, "-") {
		return "", fmt.Errorf("invalid MIMIR_SYNC_BASELINE_REF %q", ref)
	}
	out, err := gitCommand(repoRoot, "rev-parse", "--verify", "--quiet", ref+"^{commit}").Output()
	if err != nil {
		return "", fmt.Errorf("MIMIR_SYNC_BASELINE_REF %q is not a commit: %w", ref, err)
	}
	return strings.TrimSpace(string(out)), nil
}

// baseline holds the eval command lines of the pre-sync copy of one test file.
type baseline struct {
	enabled, disabled map[string]bool
}

// loadBaseline reads the pre-sync copy of the test file name at ref. It returns nil when there is no
// baseline (origin unknown), and an empty baseline when the file is new upstream (all its cases are new).
func loadBaseline(repoRoot, ref, name string) (*baseline, error) {
	if ref == "" {
		return nil, nil
	}
	spec := ref + ":pkg/streamingpromql/testdata/upstream/" + name
	if err := gitCommand(repoRoot, "cat-file", "-e", spec).Run(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// ref was verified to be a commit, so the file just doesn't exist there.
			return parseBaseline(""), nil
		}
		return nil, err
	}
	content, err := gitCommand(repoRoot, "show", spec).Output()
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", spec, err)
	}
	return parseBaseline(string(content)), nil
}

func parseBaseline(content string) *baseline {
	b := &baseline{enabled: map[string]bool{}, disabled: map[string]bool{}}
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		if trimmed := strings.TrimSpace(line); upstreamtestdata.IsEval(trimmed) {
			b.enabled[trimmed] = true
		}
		// A disabled case's eval command is on the line after the marker, commented out.
		if line == upstreamtestdata.UnsupportedMarker && i+1 < len(lines) {
			if evalLine := strings.TrimSpace(strings.TrimPrefix(lines[i+1], "#")); upstreamtestdata.IsEval(evalLine) {
				b.disabled[evalLine] = true
			}
		}
	}
	return b
}

func (b *baseline) origin(evalLine string) origin {
	switch {
	case b == nil:
		return originUnknown
	// A file can have the same eval command in more than one case. If any of them was enabled, report the
	// case as existing, so that a possible regression isn't hidden.
	case b.enabled[evalLine]:
		return originExisting
	case b.disabled[evalLine]:
		return originPreviouslyDisabled
	default:
		return originNew
	}
}

func gitCommand(repoRoot string, args ...string) *exec.Cmd {
	// The build container runs as root on a checkout owned by someone else, which git otherwise refuses
	// to read.
	cmd := exec.Command("git", append([]string{"-c", "safe.directory=*"}, args...)...)
	cmd.Dir = repoRoot
	return cmd
}

// renderDisabledCases lists the disabled cases by group. The previously-disabled groups are only
// included if includePreviouslyDisabled is set.
func renderDisabledCases(cases map[caseGroup][]string, includePreviouslyDisabled bool) string {
	// Existing cases first: a previously-passing case that now fails is the most likely real regression.
	sections := []struct {
		group caseGroup
		title string
	}{
		{caseGroup{originExisting, false}, "Divergent result or runtime error in EXISTING cases (possible regression, please review carefully)"},
		{caseGroup{originExisting, true}, "Unsupported by Mimir's engine in EXISTING cases (possible regression, please review carefully)"},
		{caseGroup{originNew, false}, "Divergent result or runtime error in NEWLY-SYNCED upstream cases"},
		{caseGroup{originNew, true}, "Unsupported by Mimir's engine in NEWLY-SYNCED upstream cases (feature not implemented)"},
		{caseGroup{originUnknown, false}, "Divergent result or runtime error (origin unknown)"},
		{caseGroup{originUnknown, true}, "Unsupported by Mimir's engine (origin unknown)"},
		{caseGroup{originPreviouslyDisabled, false}, "Divergent result or runtime error in PREVIOUSLY-DISABLED cases that upstream changes re-enabled (disabled again)"},
		{caseGroup{originPreviouslyDisabled, true}, "Unsupported by Mimir's engine in PREVIOUSLY-DISABLED cases that upstream changes re-enabled (disabled again)"},
	}

	var b strings.Builder
	for _, s := range sections {
		items := cases[s.group]
		if len(items) == 0 || (s.group.origin == originPreviouslyDisabled && !includePreviouslyDisabled) {
			continue
		}
		fmt.Fprintf(&b, "**%s:**\n\n%s\n\n", s.title, strings.Join(items, "\n"))
	}
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
