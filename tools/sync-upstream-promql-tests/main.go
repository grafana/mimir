// SPDX-License-Identifier: AGPL-3.0-only

// Command sync-upstream-promql-tests re-syncs pkg/streamingpromql/testdata/upstream with the
// upstream PromQL test cases vendored under
// vendor/github.com/prometheus/prometheus/promql/promqltest/testdata.
//
// It is the "straightforward" half of keeping TestOurUpstreamTestCasesAreInSyncWithUpstream green
// after a mimir-prometheus bump: it applies upstream's changes to our copies while preserving the
// eval commands we have disabled ("# Unsupported by streaming engine.") via a 3-way merge. It does
// NOT decide which new cases to disable - that is done afterwards by
// the disable-failing-upstream-promql-tests tool, which actually runs the cases against Mimir's engine.
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

	"github.com/grafana/mimir/pkg/streamingpromql/upstreamtestdata"
)

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
		upstream := upstreamtestdata.StripLineTrailingWhitespace(string(upstreamBytes))
		header := upstreamtestdata.LicenseHeader(name)

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
			// cannot run are disabled afterwards by the disable-failing-upstream-promql-tests tool.
			if err := os.WriteFile(ourEnabled, []byte(header+upstream), 0o644); err != nil {
				return err
			}
			rep.newFiles = append(rep.newFiles, name)

		default:
			ourBytes, err := os.ReadFile(ourEnabled)
			if err != nil {
				return err
			}
			ours := upstreamtestdata.StripLineTrailingWhitespace(strings.TrimPrefix(string(ourBytes), header))
			// The in-sync test guarantees restore(ours) == the previously-vendored upstream, so we
			// can reconstruct the merge base from our own copy without needing the old vendor.
			base := upstreamtestdata.RestoreUnsupportedTestCases(ours)

			merged, conflicts, err := threeWayMerge(base, ours, upstream)
			if err != nil {
				return err
			}
			if conflicts {
				// Upstream changed one or more cases we had disabled; those blocks were taken from
				// upstream (see threeWayMerge). The disable-failing-upstream-promql-tests tool
				// re-derives whether they should be disabled.
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
// using git merge-file, and reports whether any conflicts occurred. Non-conflicting upstream changes
// are applied while our disabling is preserved. A conflict happens only where upstream changed a case
// we had disabled (both sides touched the same lines); each such block is resolved in favour of
// upstream (the `--theirs` re-run), so it comes back uncommented while unrelated disabled blocks in
// the file are left alone. The disable step then re-derives whether those blocks should be disabled.
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

	merged, conflicts, err := runMergeFile(oursPath, basePath, theirsPath, false)
	if err != nil {
		return "", false, err
	}
	if !conflicts {
		return merged, false, nil
	}

	// Re-run, resolving each conflicting hunk in favour of upstream, so only the blocks upstream
	// changed under our disabling are taken from upstream - the rest of our disabling is preserved.
	resolved, _, err := runMergeFile(oursPath, basePath, theirsPath, true)
	if err != nil {
		return "", false, err
	}
	return resolved, true, nil
}

// runMergeFile runs `git merge-file -p [--theirs] <ours> <base> <theirs>`, writing the merge of the
// upstream changes into our copy to stdout. Its exit code is the number of conflicts (0 if clean, and
// always 0 with --theirs since conflicts are auto-resolved), or negative on error.
func runMergeFile(oursPath, basePath, theirsPath string, favourTheirs bool) (string, bool, error) {
	args := []string{"merge-file", "-p"}
	if favourTheirs {
		args = append(args, "--theirs")
	}
	args = append(args, oursPath, basePath, theirsPath)

	cmd := exec.Command("git", args...)
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err == nil {
		return out.String(), false, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() > 0 {
		return out.String(), true, nil
	}
	return "", false, fmt.Errorf("git merge-file: %w", err)
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
	section("Files where upstream changed a locally-disabled case (that block was taken from upstream; the disable step re-derives its status)", r.conflicts)

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
