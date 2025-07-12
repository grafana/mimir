// SPDX-License-Identifier: AGPL-3.0-only

package model

// Label represents a label for cost attribution.
type Label struct {
	// Input is the source label name that exists in the input metrics.
	Input string `yaml:"input" json:"input"`
	// Output is the label name that will be used at the output of the cost attribution.
	// If empty, the input label should be used as the output label.
	Output string `yaml:"output,omitempty" json:"output,omitempty"`
}

// OutputLabel returns the output label for the label.
// If the output label is empty, the input label is returned.
func (l Label) OutputLabel() string {
	if l.Output == "" {
		return l.Input
	}
	return l.Output
}

// ParseCostAttributionLabels parses a slice of strings into a slice of Label structs.
// Each string is used as both the input and output label. (No override is possible.)
func ParseCostAttributionLabels(labelStrings []string) []Label {
	output := make([]Label, 0, len(labelStrings))

	for _, label := range labelStrings {
		output = append(output, Label{Input: label})
	}

	return output
}
