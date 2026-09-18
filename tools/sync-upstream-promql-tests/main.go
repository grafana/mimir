// SPDX-License-Identifier: AGPL-3.0-only

// Command sync-upstream-promql-tests re-syncs pkg/streamingpromql/testdata/upstream with the
// upstream PromQL test cases vendored under
// vendor/github.com/prometheus/prometheus/promql/promqltest/testdata.
//
// It is the "straightforward" half of keeping TestOurUpstreamTestCasesAreInSyncWithUpstream green
// after a mimir-prometheus bump: it applies upstream's changes to our copies while preserving the
// eval commands we have disabled ("# Unsupported by streaming engine.") via a 3-way merge. It does
// NOT decide which new cases to disable - that is done afterwards by
// TestDisableFailingUpstreamCases, which actually runs the cases against Mimir's engine.
//
// Run it with `go run .` in this directory, or via `make sync-upstream-promql-tests`.
//
// See ../../pkg/streamingpromql/testdata/upstream/README.md for background.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
)

const unsupportedMarker = "# Unsupported by streaming engine."

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	upstreamDir, err := filepath.Abs(filepath.Join("..", "..", "vendor", "github.com", "prometheus", "prometheus", "promql", "promqltest", "testdata"))
	if err != nil {
		return err
	}
	ourDir, err := filepath.Abs(filepath.Join("..", "..", "pkg", "streamingpromql", "testdata", "upstream"))
	if err != nil {
		return err
	}

	upstreamFiles, err := filepath.Glob(filepath.Join(upstreamDir, "*.test"))
	if err != nil {
		return err
	}
	ourFiles, err := filepath.Glob(filepath.Join(ourDir, "*.test*"))
	if err != nil {
		return err
	}

	upstreamNames := make(map[string]string, len(upstreamFiles))
	for _, p := range upstreamFiles {
		upstreamNames[filepath.Base(p)] = p
	}

	var rep report

	for name, upstreamPath := range upstreamNames {
		upstreamBytes, err := os.ReadFile(upstreamPath)
		if err != nil {
			return err
		}
		// Normalize trailing whitespace, matching the in-sync test (which strips it before comparing)
		// and the style of our committed copies, so re-syncing an unchanged file is a no-op.
		upstream := stripLineTrailingWhitespace(string(upstreamBytes))
		header := licenseHeader(name)

		ourEnabled := filepath.Join(ourDir, name)
		ourDisabled := ourEnabled + ".disabled"

		switch {
		case fileExists(ourDisabled):
			// The whole file is disabled. Keep it disabled; its content must equal upstream
			// verbatim (apart from the license header), which is what the in-sync test asserts.
			if err := os.WriteFile(ourDisabled, []byte(header+upstream), 0o644); err != nil {
				return err
			}

		case !fileExists(ourEnabled):
			// Upstream added a new test file. Create it fully enabled and in sync; any cases Mimir
			// cannot run are disabled afterwards by TestDisableFailingUpstreamCases.
			if err := os.WriteFile(ourEnabled, []byte(header+upstream), 0o644); err != nil {
				return err
			}
			rep.newFiles = append(rep.newFiles, name)

		default:
			ourBytes, err := os.ReadFile(ourEnabled)
			if err != nil {
				return err
			}
			ours := stripLineTrailingWhitespace(strings.TrimPrefix(string(ourBytes), header))
			// The in-sync test guarantees restore(ours) == the previously-vendored upstream, so we
			// can reconstruct the merge base from our own copy without needing the old vendor.
			base := restoreUnsupportedTestCases(ours)

			merged, conflicts, err := threeWayMerge(base, ours, upstream)
			if err != nil {
				return err
			}
			if conflicts {
				// Upstream changed a region we had disabled. Take upstream as-is so the files stay
				// in sync; TestDisableFailingUpstreamCases re-derives the disabling from scratch.
				merged = upstream
				rep.conflicts = append(rep.conflicts, name)
			}
			if err := os.WriteFile(ourEnabled, []byte(header+merged), 0o644); err != nil {
				return err
			}
		}
	}

	// Remove copies whose upstream counterpart no longer exists.
	for _, p := range ourFiles {
		base := filepath.Base(p)
		name := strings.TrimSuffix(base, ".disabled")
		if _, ok := upstreamNames[name]; ok {
			continue
		}
		if err := os.Remove(p); err != nil {
			return err
		}
		rep.removed = append(rep.removed, base)
	}

	return rep.write(os.Getenv("MIMIR_SYNC_REPORT"))
}

// threeWayMerge merges upstream's changes (base -> theirs) into our disabled-annotated copy (ours)
// using git merge-file. It returns the merged content, or conflicts=true when the merge could not be
// completed cleanly (in which case the returned content is empty).
func threeWayMerge(base, ours, theirs string) (string, bool, error) {
	dir, err := os.MkdirTemp("", "promql-sync")
	if err != nil {
		return "", false, err
	}
	defer os.RemoveAll(dir)

	oursPath := filepath.Join(dir, "ours")
	basePath := filepath.Join(dir, "base")
	theirsPath := filepath.Join(dir, "theirs")
	for path, content := range map[string]string{oursPath: ours, basePath: base, theirsPath: theirs} {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			return "", false, err
		}
	}

	// `git merge-file -p <current> <base> <other>` writes the merge of `other` into `current` to
	// stdout. Its exit code is the number of conflicts, or negative on error.
	cmd := exec.Command("git", "merge-file", "-p", oursPath, basePath, theirsPath)
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err == nil {
		return out.String(), false, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() > 0 {
		return "", true, nil
	}
	return "", false, fmt.Errorf("git merge-file: %w", err)
}

// restoreUnsupportedTestCases re-enables cases we disabled, so the result should equal the upstream
// file. It is kept identical to the copy in pkg/streamingpromql/testdata_in_sync_test.go.
func restoreUnsupportedTestCases(s string) string {
	lines := strings.Split(s, "\n")
	inUnsupportedTestCase := false

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		if line == unsupportedMarker {
			lines = slices.Delete(lines, i, i+1)
			inUnsupportedTestCase = true
			i--
		} else if inUnsupportedTestCase && strings.HasPrefix(line, "# ") {
			lines[i] = strings.TrimPrefix(line, "# ")
		} else if inUnsupportedTestCase && strings.HasPrefix(line, "#\t") {
			lines[i] = strings.TrimPrefix(line, "#")
		} else {
			inUnsupportedTestCase = false
		}
	}

	return strings.Join(lines, "\n")
}

// stripLineTrailingWhitespace removes trailing spaces and tabs from each line. It is kept identical
// to the copy in pkg/streamingpromql/testdata_in_sync_test.go.
func stripLineTrailingWhitespace(s string) string {
	lines := strings.Split(s, "\n")
	for i := range lines {
		lines[i] = strings.TrimRight(lines[i], " \t")
	}
	return strings.Join(lines, "\n")
}

func licenseHeader(name string) string {
	return strings.Join([]string{
		"# SPDX-License-Identifier: AGPL-3.0-only",
		"# Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/testdata/" + name,
		"# Provenance-includes-license: Apache-2.0",
		"# Provenance-includes-copyright: The Prometheus Authors",
		"",
		"",
	}, "\n")
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

type report struct {
	newFiles  []string
	removed   []string
	conflicts []string
}

func (r report) write(path string) error {
	var b strings.Builder
	section := func(title string, items []string) {
		if len(items) == 0 {
			return
		}
		slices.Sort(items)
		fmt.Fprintf(&b, "### %s\n\n", title)
		for _, i := range items {
			fmt.Fprintf(&b, "- `%s`\n", i)
		}
		b.WriteString("\n")
	}
	section("New upstream test files added", r.newFiles)
	section("Test files removed (no longer present upstream)", r.removed)
	section("Files re-synced from upstream, dropping local disabling (merge conflict - please review)", r.conflicts)

	// Always echo to stdout for local runs.
	fmt.Print(b.String())

	if path == "" || b.Len() == 0 {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(b.String())
	return err
}
