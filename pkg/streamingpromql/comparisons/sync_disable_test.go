// SPDX-License-Identifier: AGPL-3.0-only

package comparisons

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/upstreamtestdata"
)

// TestDisableFailingUpstreamCases is a code generator, not an assertion; it is a test only because it
// needs promqltest's per-case runner. When MIMIR_SYNC_UPSTREAM=1 it comments out every enabled
// upstream eval command that Mimir's engine cannot run - unsupported feature or divergent result - so
// TestUpstreamTestCases stays green after a mimir-prometheus bump.
//
// It runs in the automated vendoring workflow (after resync-upstream-promql-tests), not the normal
// test suite, and mutates testdata/upstream. Because it disables on any failure, including a wrong
// result that could be a real regression, disabled cases are written to MIMIR_SYNC_REPORT and
// MIMIR_SYNC_DISABLED for the PR description and owner notification, and the vendoring PR stays behind
// human approval. Failing cases make the run exit non-zero, which the workflow ignores.
func TestDisableFailingUpstreamCases(t *testing.T) {
	if os.Getenv("MIMIR_SYNC_UPSTREAM") != "1" {
		t.Skip("set MIMIR_SYNC_UPSTREAM=1 to run the upstream test-case disabling sync")
	}

	engine := newUpstreamTestEngine(t)

	testFiles, err := filepath.Glob(filepath.Join("..", "testdata", "upstream", "*.test"))
	require.NoError(t, err)
	require.NotEmpty(t, testFiles)

	ctx := t.Context()
	// A snapshot of testdata/upstream taken before the re-sync, used to tell whether a failing case
	// is newly synced from upstream or was already present (and passing) before this bump. Unset for
	// local runs, in which case divergent cases are not split by origin.
	baselineDir := os.Getenv("MIMIR_SYNC_BASELINE_DIR")

	// Disabled cases split by cause and origin, so a reviewer can tell an expected capability gap or a
	// new upstream case from a regression in a previously-passing case.
	var unsupported, divergentNew, divergentExisting, divergentUnknown []string

	for _, testFile := range testFiles {
		contentBytes, err := os.ReadFile(testFile)
		require.NoError(t, err)
		content := string(contentBytes)
		base := filepath.Base(testFile)

		// Phase 1: run the whole file in a single pass. promqltest runs each eval command as its own
		// subtest, so this loads the file's data just once - as cheap as TestUpstreamTestCases. If the
		// whole file passes, there is nothing to disable and we skip the expensive per-eval phase.
		if t.Run(base, func(t *testing.T) { promqltest.RunTest(t, content, engine) }) {
			continue
		}

		// Phase 2: the file has at least one failing case. Re-run each eval in isolation to find
		// exactly which ones to comment out. Only failing files pay this cost. preamble holds the
		// source of every preceding load/clear command, so each eval sees the storage state it would
		// have in the full file.
		lines := strings.Split(content, "\n")
		blocks := parseCommandBlocks(lines)
		baselineEvals := loadBaselineEnabledEvals(t, baselineDir, base)

		var preamble []string
		disabled := make(map[int]bool)

		for idx, b := range blocks {
			if b.kind != cmdEval {
				if b.kind == cmdLoad || b.kind == cmdClear {
					preamble = append(preamble, strings.Join(lines[b.start:b.end], "\n"))
				}
				continue
			}

			script := strings.Join(preamble, "\n\n") + "\n\n" + strings.Join(lines[b.start:b.end], "\n")
			evalLine := strings.TrimSpace(lines[b.start])
			ok := t.Run(fmt.Sprintf("%s:%d", base, b.start+1), func(t *testing.T) {
				promqltest.RunTest(t, script, engine)
			})
			if ok {
				continue
			}

			disabled[idx] = true
			entry := fmt.Sprintf("- `%s` line %d: `%s`", base, b.start+1, evalLine)
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

		if len(disabled) == 0 {
			continue
		}

		require.NoError(t, os.WriteFile(testFile, []byte(commentOutBlocks(lines, blocks, disabled)), 0o644))
	}

	list := renderDisabledCases(unsupported, divergentExisting, divergentNew, divergentUnknown)
	if list == "" {
		return
	}
	appendToFileEnv(t, "MIMIR_SYNC_REPORT", "### Auto-disabled upstream test cases\n\n"+list)
	appendToFileEnv(t, "MIMIR_SYNC_DISABLED", list)
}

// loadBaselineEnabledEvals returns the set of enabled eval command lines in the pre-sync copy of the
// named test file, used to tell whether a failing case existed before this bump. It returns nil when
// no baseline is available (origin can't be determined), and an empty set when the file is new
// upstream (so every case in it counts as newly synced).
func loadBaselineEnabledEvals(t *testing.T, baselineDir, name string) map[string]bool {
	if baselineDir == "" {
		return nil
	}
	b, err := os.ReadFile(filepath.Join(baselineDir, name))
	if errors.Is(err, os.ErrNotExist) {
		return map[string]bool{}
	}
	require.NoError(t, err)

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

func appendToFileEnv(t *testing.T, envVar, content string) {
	path := os.Getenv(envVar)
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.WriteString(content)
	require.NoError(t, err)
}

type commandKind int

const (
	cmdOther commandKind = iota
	cmdLoad
	cmdClear
	cmdEval
)

type commandBlock struct {
	kind       commandKind
	start, end int // line indices, [start, end)
}

// parseCommandBlocks splits the file into command blocks the way promqltest does: a block starts at a
// non-blank, non-comment line and runs until the next blank or comment line. Comment lines (including
// already-disabled cases) and blank lines are treated by promqltest as separators and are not part of
// any block.
func parseCommandBlocks(lines []string) []commandBlock {
	var blocks []commandBlock
	for i := 0; i < len(lines); {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			i++
			continue
		}
		start := i
		i++
		for i < len(lines) {
			t := strings.TrimSpace(lines[i])
			if t == "" || strings.HasPrefix(t, "#") {
				break
			}
			i++
		}
		blocks = append(blocks, commandBlock{kind: classifyCommand(trimmed), start: start, end: i})
	}
	return blocks
}

func classifyCommand(line string) commandKind {
	word := line
	if idx := strings.IndexAny(line, " \t"); idx >= 0 {
		word = line[:idx]
	}
	word = strings.ToLower(word)
	switch {
	case word == "clear":
		return cmdClear
	case strings.HasPrefix(word, "load"):
		return cmdLoad
	case strings.HasPrefix(word, "eval"):
		return cmdEval
	default:
		return cmdOther
	}
}

// commentOutBlocks rebuilds the file, replacing each disabled block with its commented-out form. The
// format is exactly what upstreamtestdata.RestoreUnsupportedTestCases reverses, so the file stays in
// sync with upstream.
func commentOutBlocks(lines []string, blocks []commandBlock, disabled map[int]bool) string {
	var out []string
	nextBlock := 0
	for i := 0; i < len(lines); {
		if nextBlock < len(blocks) && blocks[nextBlock].start == i {
			b := blocks[nextBlock]
			if disabled[nextBlock] {
				out = append(out, upstreamtestdata.UnsupportedMarker)
				for _, l := range lines[b.start:b.end] {
					out = append(out, "# "+l)
				}
			} else {
				out = append(out, lines[b.start:b.end]...)
			}
			i = b.end
			nextBlock++
			continue
		}
		out = append(out, lines[i])
		i++
	}
	return strings.Join(out, "\n")
}
