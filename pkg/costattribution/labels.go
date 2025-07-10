// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import "strings"

// Label represents a label for cost attribution.
type Label struct {
	// input is the source label name that exists in the input metrics.
	input string
	// output is the label name that will be used at the output of the cost attribution.
	// If empty, the input label should be used as the output label.
	output string
}

func NewLabel(inputLabel, outputLabel string) Label {
	return Label{input: inputLabel, output: outputLabel}
}

func (l Label) outputLabel() string {
	if l.output == "" {
		return l.input
	}
	return l.output
}

// ParseCostAttributionLabels parses a slice of strings into Label structs.
// Each string can be in the format "output=input" or just "input".
func ParseCostAttributionLabels(labelStrings []string) []Label {
	output := make([]Label, 0, len(labelStrings))

	for _, label := range labelStrings {
		// A label string is of the form "output=input" or "input".
		if l, r, ok := strings.Cut(label, "="); ok {
			output = append(output, Label{input: r, output: l})
		} else {
			output = append(output, Label{input: label})
		}
	}

	return output
}
