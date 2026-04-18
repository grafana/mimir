// regenerate-expdiffs regenerates all .pb.go.expdiff files from clean wiresmith output.
//
// Run after wiresmith output changes (new features, struct tag format changes, etc.)
// to update the expdiff files that apply-expected-diffs.sh uses.
//
// Usage:
//
//	go run ./tools/regenerate-expdiffs
//	go run ./tools/regenerate-expdiffs pkg/mimirpb/mimir.pb.go  # single file
//
// Prerequisites: wiresmith-generated .pb.go files must be freshly generated
// (clean, without expdiff applied). Run this BEFORE `make protos`.
package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// modification describes how to transform a clean .pb.go into the desired state.
type modification struct {
	file string
	ops  []op
}

type op func(lines []string) []string

// addImportIfMissing adds an import line if not already present.
func addImportIfMissing(importPath, afterSubstr string) op {
	return func(lines []string) []string {
		importLine := fmt.Sprintf("\t%q", importPath)
		quotedImport := fmt.Sprintf("%q", importPath)
		for _, l := range lines {
			if strings.Contains(l, quotedImport) {
				return lines
			}
		}
		for i, l := range lines {
			if strings.Contains(l, afterSubstr) {
				return insert(lines, i+1, importLine)
			}
		}
		return lines
	}
}

// addStructFields inserts fields after "type Name struct {".
func addStructFields(structName string, fields ...string) op {
	return func(lines []string) []string {
		target := "type " + structName + " struct {"
		for i, l := range lines {
			if strings.Contains(l, target) {
				tabbed := make([]string, len(fields))
				for j, f := range fields {
					if f == "" {
						tabbed[j] = "" // empty line without trailing whitespace
					} else {
						tabbed[j] = "\t" + f
					}
				}
				return insertMany(lines, i+1, tabbed)
			}
		}
		return lines
	}
}

// addFieldsAfter inserts fields after the first line matching substr.
func addFieldsAfter(substr string, fields ...string) op {
	return func(lines []string) []string {
		for i, l := range lines {
			if strings.Contains(l, substr) {
				tabbed := make([]string, len(fields))
				for j, f := range fields {
					tabbed[j] = "\t" + f
				}
				return insertMany(lines, i+1, tabbed)
			}
		}
		return lines
	}
}

// inFunc finds a function and applies an inner operation within it.
func inFunc(funcSig string, inner op) op {
	return func(lines []string) []string {
		start, end := findFunc(lines, funcSig)
		if start < 0 {
			return lines
		}
		// Extract the function body, apply the inner op, splice back.
		body := make([]string, end-start)
		copy(body, lines[start:end])
		body = inner(body)
		result := make([]string, 0, len(lines)+len(body)-(end-start))
		result = append(result, lines[:start]...)
		result = append(result, body...)
		result = append(result, lines[end:]...)
		return result
	}
}

// insertAfter inserts lines after the first match within the slice.
func insertAfter(pattern string, newLines ...string) op {
	return func(lines []string) []string {
		for i, l := range lines {
			if strings.Contains(l, pattern) {
				return insertMany(lines, i+1, newLines)
			}
		}
		return lines
	}
}

// insertBefore inserts lines before the LAST match within the slice.
func insertBeforeLast(pattern string, newLines ...string) op {
	return func(lines []string) []string {
		last := -1
		for i, l := range lines {
			if strings.Contains(l, pattern) {
				last = i
			}
		}
		if last >= 0 {
			return insertMany(lines, last, newLines)
		}
		return lines
	}
}

// replaceFirst replaces the first line matching old with new.
func replaceFirst(old, new string) op {
	return func(lines []string) []string {
		for i, l := range lines {
			if strings.Contains(l, old) {
				lines[i] = new
				return lines
			}
		}
		return lines
	}
}

// replaceBlock replaces N lines starting from the first match.
func replaceBlock(pattern string, numLines int, replacement ...string) op {
	return func(lines []string) []string {
		for i, l := range lines {
			if strings.Contains(l, pattern) {
				result := make([]string, 0, len(lines)+len(replacement)-numLines)
				result = append(result, lines[:i]...)
				result = append(result, replacement...)
				result = append(result, lines[i+numLines:]...)
				return result
			}
		}
		return lines
	}
}

// wrapBlock wraps N lines starting from first match with before/after, adding indent.
func wrapBlock(pattern string, numLines int, before, after string) op {
	return func(lines []string) []string {
		for i, l := range lines {
			if strings.Contains(l, pattern) {
				var result []string
				result = append(result, lines[:i]...)
				result = append(result, before)
				for j := 0; j < numLines; j++ {
					result = append(result, "\t"+lines[i+j])
				}
				result = append(result, after)
				result = append(result, lines[i+numLines:]...)
				return result
			}
		}
		return lines
	}
}

// appendFileContent appends the contents of a file.
func appendFileContent(path string) op {
	return func(lines []string) []string {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: cannot read %s: %v\n", path, err)
			return lines
		}
		content := strings.TrimRight(string(data), "\n")
		return append(lines, strings.Split(content, "\n")...)
	}
}

// helpers

func insert(lines []string, at int, line string) []string {
	return insertMany(lines, at, []string{line})
}

func insertMany(lines []string, at int, newLines []string) []string {
	result := make([]string, 0, len(lines)+len(newLines))
	result = append(result, lines[:at]...)
	result = append(result, newLines...)
	result = append(result, lines[at:]...)
	return result
}

func findFunc(lines []string, sig string) (int, int) {
	start := -1
	for i, l := range lines {
		if strings.Contains(l, sig) {
			start = i
			break
		}
	}
	if start < 0 {
		return -1, -1
	}
	for i := start + 1; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "func ") {
			return start, i
		}
	}
	return start, len(lines)
}

// ============================================================================
// Modification registry
// ============================================================================

func bufferHolder(file string, structs ...string) modification {
	var ops []op
	ops = append(ops, addImportIfMissing("github.com/grafana/mimir/pkg/mimirpb", "github.com/grafana/wiresmith/gen/protohelpers"))
	for _, s := range structs {
		ops = append(ops, addStructFields(s,
			"// Keep reference to buffer for unsafe references.",
			"mimirpb.BufferHolder",
			"",
		))
	}
	return modification{file: file, ops: ops}
}

const wrUnmarshal = "func (m *WriteRequest) Unmarshal(b []byte) error"
const tsUnmarshal = "func (m *TimeSeries) Unmarshal(b []byte) error"

var rw2FunctionsPath = "tools/regenerate-expdiffs/rw2_functions.go.inc"

func mimirPb() modification {
	return modification{
		file: "pkg/mimirpb/mimir.pb.go",
		ops: []op{
			addImportIfMissing("io", "github.com/grafana/wiresmith/gen/protohelpers"),
			addStructFields("WriteRequest",
				"BufferHolder",
				"sourceBufferHolders map[uintptr]BufferHolder",
			),
			addFieldsAfter("SkipLabelCountValidation bool",
				"skipUnmarshalingExemplars bool",
				"skipNormalizeMetadataMetricName bool",
				"skipDeduplicateMetadata bool",
				"unmarshalFromRW2 bool",
				"rw2symbols       rw2PagedSymbols",
			),
			addFieldsAfter("CreatedTimestamp int64",
				"SkipUnmarshalingExemplars bool",
			),

			// WriteRequest.Unmarshal RW2 modifications
			inFunc(wrUnmarshal, insertAfter("for len(b) > 0 {",
				// NOTE: inserted INSIDE the for loop, after its opening brace is fine
				// because we actually need them before the for loop. Let me fix:
			)),
			// Actually: insert before the for loop
			inFunc(wrUnmarshal, func(lines []string) []string {
				for i, l := range lines {
					if strings.TrimSpace(l) == "for len(b) > 0 {" {
						return insertMany(lines, i, []string{
							"\tvar metadata metadataSet",
							"\tseenFirstSymbol := false",
						})
					}
				}
				return lines
			}),
			inFunc(wrUnmarshal, insertAfter("case 1: // timeseries",
				"\t\t\tif m.unmarshalFromRW2 { return errorUnexpectedRW1Timeseries }")),
			inFunc(wrUnmarshal, insertAfter("m.Timeseries = append(m.Timeseries, PreallocTimeseries{})",
				"\t\t\tm.Timeseries[len(m.Timeseries)-1].skipUnmarshalingExemplars = m.skipUnmarshalingExemplars")),
			inFunc(wrUnmarshal, replaceFirst(
				"Timeseries[len(m.Timeseries)-1].Unmarshal(v)",
				"\t\t\tif err := m.Timeseries[len(m.Timeseries)-1].Unmarshal(v, nil, nil, m.skipNormalizeMetadataMetricName); err != nil {")),
			inFunc(wrUnmarshal, insertAfter("case 3: // metadata",
				"\t\t\tif m.unmarshalFromRW2 { return errorUnexpectedRW1Metadata }")),
			inFunc(wrUnmarshal, insertAfter("case 4: // symbolsRW2",
				"\t\t\tif !m.unmarshalFromRW2 { return errorUnexpectedRW2Symbols }")),
			inFunc(wrUnmarshal, replaceBlock("protowire.ConsumeString(b)", 6,
				"\t\t\tv, n := protowire.ConsumeBytes(b)",
				"\t\t\tif n < 0 { return fmt.Errorf(\"invalid bytes\") }",
				"\t\t\tif !seenFirstSymbol && len(v) > 0 { return errorInvalidFirstSymbol }",
				"\t\t\tseenFirstSymbol = true",
				"\t\t\tm.rw2symbols.append(yoloString(v))",
				"\t\t\tb = b[n:]",
			)),
			inFunc(wrUnmarshal, insertAfter("case 5: // timeseriesRW2",
				"\t\t\tif !m.unmarshalFromRW2 { return errorUnexpectedRW2Timeseries }")),
			inFunc(wrUnmarshal, replaceBlock("m.TimeseriesRW2 = append(m.TimeseriesRW2, TimeSeriesRW2{})", 4,
				"\t\t\tm.Timeseries = append(m.Timeseries, PreallocTimeseries{})",
				"\t\t\tm.Timeseries[len(m.Timeseries)-1].skipUnmarshalingExemplars = m.skipUnmarshalingExemplars",
				"\t\t\tif metadata == nil { metadata = metadataSetFromSettings(m.skipDeduplicateMetadata) }",
				"\t\t\tif err := m.Timeseries[len(m.Timeseries)-1].Unmarshal(v, &m.rw2symbols, metadata, m.skipNormalizeMetadataMetricName); err != nil {",
				"\t\t\t\treturn err",
				"\t\t\t}",
			)),
			inFunc(wrUnmarshal, insertBeforeLast("return nil",
				"\tif m.unmarshalFromRW2 {",
				"\t\tif metadata != nil { m.Metadata = metadata.slice() }",
				"\t\tm.rw2symbols.releasePages()",
				"\t}",
			)),

			// TimeSeries.Unmarshal: exemplar skip guard
			inFunc(tsUnmarshal, wrapBlock(
				"m.Exemplars = append(m.Exemplars, Exemplar{})", 4,
				"\t\t\tif !m.SkipUnmarshalingExemplars {",
				"\t\t\t}",
			)),

			// Append RW2 helper functions
			appendFileContent(rw2FunctionsPath),
		},
	}
}

var allMods = []modification{
	bufferHolder("pkg/storegateway/storepb/rpc.pb.go", "SeriesResponse"),
	bufferHolder("pkg/storegateway/storepb/cache.pb.go", "CachedSeries"),
	bufferHolder("pkg/ruler/ruler.pb.go", "RulesResponse"),
	bufferHolder("pkg/frontend/querymiddleware/model.pb.go", "PrometheusResponse"),
	bufferHolder("pkg/frontend/v2/frontendv2pb/frontend.pb.go", "QueryResultStreamRequest"),
	bufferHolder("pkg/ingester/client/ingester.pb.go",
		"QueryStreamResponse", "ExemplarQueryResponse",
		"MetricsForLabelMatchersResponse", "ActiveSeriesResponse"),
	mimirPb(),
}

func main() {
	requested := map[string]bool{}
	for _, arg := range os.Args[1:] {
		requested[arg] = true
	}

	for _, mod := range allMods {
		if len(requested) > 0 && !requested[mod.file] {
			continue
		}
		if err := regenerateExpdiff(mod); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", mod.file, err)
			os.Exit(1)
		}
	}
}

func regenerateExpdiff(mod modification) error {
	// Read the clean wiresmith output
	data, err := os.ReadFile(mod.file)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	cleanLines := strings.Split(string(data), "\n")

	// Apply modifications to get the desired state
	desired := make([]string, len(cleanLines))
	copy(desired, cleanLines)
	for _, op := range mod.ops {
		desired = op(desired)
	}

	// Trim trailing empty lines
	for len(desired) > 0 && desired[len(desired)-1] == "" {
		desired = desired[:len(desired)-1]
	}

	// Write desired state to a temp file
	desiredPath := mod.file + ".desired"
	if err := os.WriteFile(desiredPath, []byte(strings.Join(desired, "\n")+"\n"), 0644); err != nil {
		return fmt.Errorf("write desired: %w", err)
	}
	defer os.Remove(desiredPath)

	// Generate expdiff: diff desired vs clean (desired has additions as - lines when reversed)
	expdiffPath := mod.file + ".expdiff"
	cmd := exec.Command("diff", "-u", desiredPath, mod.file)
	out, _ := cmd.Output() // diff exits 1 when files differ

	if len(out) == 0 {
		// No differences — remove expdiff if it exists
		os.Remove(expdiffPath)
		fmt.Printf("  %s: no modifications needed (no expdiff)\n", mod.file)
		return nil
	}

	// Fix the diff header to use relative paths
	diffStr := string(out)
	diffStr = strings.Replace(diffStr, "--- "+desiredPath, "--- a/"+mod.file, 1)
	diffStr = strings.Replace(diffStr, "+++ "+mod.file, "+++ b/"+mod.file, 1)

	if err := os.WriteFile(expdiffPath, []byte(diffStr), 0644); err != nil {
		return fmt.Errorf("write expdiff: %w", err)
	}

	// Verify the expdiff applies cleanly
	verifyCmd := exec.Command("git", "apply", "-R", "--check", expdiffPath)
	verifyCmd.Dir = filepath.Dir(mod.file)
	if parent := filepath.Dir(filepath.Dir(mod.file)); parent != "." {
		verifyCmd.Dir = "."
	}
	if verifyOut, err := verifyCmd.CombinedOutput(); err != nil {
		fmt.Printf("  %s: WARNING: expdiff may not apply cleanly: %s\n", mod.file, string(verifyOut))
	}

	fmt.Printf("  %s: regenerated expdiff (%d bytes)\n", mod.file, len(diffStr))
	return nil
}
