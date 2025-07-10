// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import "strings"

// Label represents a label for cost attribution.
type Label struct {
	// Input is the source label name that exists in the Input metrics.
	Input string
	// Output is the label name that will be used at the Output of the cost attribution.
	// If empty, the input label should be used as the Output label.
	Output string
}

// outputLabel returns the output label for the label.
// If the output label is empty, the input label is returned.
func (l Label) outputLabel() string {
	if l.Output == "" {
		return l.Input
	}
	return l.Output
}

// ParseCostAttributionLabels parses a slice of strings into Label structs.
// Each string can be in the format "output=input" or just "input".
func ParseCostAttributionLabels(labelStrings []string) []Label {
	output := make([]Label, 0, len(labelStrings))

	for _, label := range labelStrings {
		if out, in, ok := strings.Cut(label, "="); ok {
			output = append(output, Label{Input: in, Output: out})
		} else {
			output = append(output, Label{Input: label})
		}
	}

	return output
}
