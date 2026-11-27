// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/tree/main/promql/promqltest/test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

// Package upstreamtestdata holds the eval command helpers shared by the tooling that keeps
// pkg/streamingpromql/testdata/upstream in sync with the upstream PromQL test cases (the disabling tool
// and the disabled-but-supported checker). The file format itself, which the in-sync test and the
// re-sync tool also use, is in the fileformat package.
package upstreamtestdata

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/upstreamtestdata/fileformat"
)

// These are the same expressions promqltest uses to parse eval commands.
var (
	patEvalInstant = regexp.MustCompile(`^eval(?:_(fail|warn|ordered|info))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$`)
	patEvalRange   = regexp.MustCompile(`^eval(?:_(fail|warn|info))?\s+range\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+?)\s+(.+)$`)
)

// ParseEval extracts the query from an eval command line, reporting whether it is an instant query and
// whether the line was a recognizable eval command at all.
func ParseEval(evalLine string) (expr string, isInstant, ok bool) {
	if m := patEvalInstant.FindStringSubmatch(evalLine); m != nil {
		return m[3], true, true
	}
	if m := patEvalRange.FindStringSubmatch(evalLine); m != nil {
		return m[5], false, true
	}
	return "", false, false
}

// IsEval reports whether evalLine is a recognizable eval command.
func IsEval(evalLine string) bool {
	_, _, ok := ParseEval(evalLine)
	return ok
}

// BuildResult is the outcome of trying to build an eval command's query against Mimir's engine.
type BuildResult int

const (
	// BuildNotEval means the line was not a recognizable eval command.
	BuildNotEval BuildResult = iota
	// BuildOK means the query built successfully.
	BuildOK
	// BuildUnsupported means the query failed to build because the feature is not implemented.
	BuildUnsupported
	// BuildError means the query failed to build for some other reason.
	BuildError
)

// ClassifyEval parses evalLine and tries to build its query against engine, reporting whether Mimir's
// engine supports it. The returned error is the build error, for BuildUnsupported and BuildError.
func ClassifyEval(ctx context.Context, engine promql.QueryEngine, evalLine string) (BuildResult, error) {
	expr, isInstant, ok := ParseEval(evalLine)
	if !ok {
		return BuildNotEval, nil
	}

	var (
		q   promql.Query
		err error
	)
	if isInstant {
		q, err = engine.NewInstantQuery(ctx, nil, nil, expr, timestamp.Time(0))
	} else {
		q, err = engine.NewRangeQuery(ctx, nil, nil, expr, timestamp.Time(0), timestamp.Time(1000), time.Millisecond)
	}

	switch {
	case err == nil:
		q.Close()
		return BuildOK, nil
	case errors.Is(err, compat.NotSupportedError{}):
		return BuildUnsupported, err
	default:
		return BuildError, err
	}
}

// CommentOutEvals comments out the eval blocks beginning at the given 1-based line numbers (as
// reported by promqltest's "line N" subtests), in the form fileformat.RestoreUnsupportedTestCases reverses. An
// eval block runs to the next blank or comment line, and comment lines directly after a disabled block
// are commented again. It returns the rewritten content and any
// requested lines that did not start an eval block.
func CommentOutEvals(content string, evalLines map[int]bool) (string, []int) {
	lines := strings.Split(content, "\n")
	blocks := parseCommandBlocks(lines)

	disabled := make(map[int]bool) // block index -> disable
	matched := make(map[int]bool)  // 1-based line -> matched an eval block
	for i, b := range blocks {
		if b.kind == cmdEval && evalLines[b.start+1] {
			disabled[i] = true
			matched[b.start+1] = true
		}
	}

	var unmatched []int
	for line := range evalLines {
		if !matched[line] {
			unmatched = append(unmatched, line)
		}
	}
	slices.Sort(unmatched)

	var out []string
	nextBlock := 0
	for i := 0; i < len(lines); {
		if nextBlock < len(blocks) && blocks[nextBlock].start == i {
			b, disable := blocks[nextBlock], disabled[nextBlock]
			nextBlock++
			if !disable {
				out = append(out, lines[b.start:b.end]...)
				i = b.end
				continue
			}
			out = append(out, fileformat.UnsupportedMarker)
			for _, l := range lines[b.start:b.end] {
				out = append(out, fileformat.CommentOutLine(l))
			}
			// RestoreUnsupportedTestCases also un-comments any "# " or "#\t" lines directly after the block,
			// so comment them too for them to be restored as they were. Another disabled case's marker
			// ends the block for RestoreUnsupportedTestCases, so it is left alone.
			for i = b.end; i < len(lines) && lines[i] != fileformat.UnsupportedMarker && (strings.HasPrefix(lines[i], "# ") || strings.HasPrefix(lines[i], "#\t")); i++ {
				out = append(out, fileformat.CommentOutLine(lines[i]))
			}
			continue
		}
		out = append(out, lines[i])
		i++
	}
	return strings.Join(out, "\n"), unmatched
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

// parseCommandBlocks splits the file into command blocks like promqltest does: each starts at a
// non-blank, non-comment line and runs to the next blank or comment line (which act as separators).
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
