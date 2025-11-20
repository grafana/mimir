// SPDX-License-Identifier: AGPL-3.0-only

package costattributionmodel

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
)

const reservedLabelPrefix = "__"

// Label represents a label for cost attribution.
type Label struct {
	// Input is the source label name that exists in the input metrics.
	Input string `yaml:"input" json:"input"`
	// Output is the label name that will be used at the output of the cost attribution.
	// If empty, the input label should be used as the output label.
	Output string `yaml:"output,omitempty" json:"output,omitempty"`
}

func (l Label) Validate() error {
	isValid := func(l string, checkPrefix bool) bool {
		if !model.UTF8Validation.IsValidLabelName(l) {
			return false
		}
		if checkPrefix && strings.HasPrefix(l, reservedLabelPrefix) {
			return false
		}
		return true
	}

	if l.Input == "" {
		return fmt.Errorf("cost attribution input label must not be empty: %q", l.String())
	}

	if !isValid(l.Input, false) {
		return fmt.Errorf("invalid cost attribution input label: %q", l.String())
	}

	if l.Output != "" && !isValid(l.Output, true) {
		return fmt.Errorf("invalid cost attribution output label: %q", l.String())
	}
	return nil
}

func (l Label) String() string {
	return fmt.Sprintf("%s:%s", l.Input, l.Output)
}

// OutputLabel returns the output label for the label.
// If the output label is empty, the input label is returned.
func (l Label) OutputLabel() string {
	if l.Output == "" {
		return l.Input
	}
	return l.Output
}

type Labels []Label

func (l Labels) Validate() error {
	if len(l) == 0 {
		return nil
	}

	outputLabels := make(map[string]struct{}, len(l))
	for _, labelWithOverride := range l {
		if err := labelWithOverride.Validate(); err != nil {
			return err
		}
		if _, ok := outputLabels[labelWithOverride.OutputLabel()]; ok {
			return fmt.Errorf("duplicate output label: %q", labelWithOverride.String())
		}
		outputLabels[labelWithOverride.OutputLabel()] = struct{}{}
	}
	return nil
}

func (l Labels) OutputLabels() []string {
	res := make([]string, 0, len(l))
	for _, label := range l {
		res = append(res, label.OutputLabel())
	}
	return res
}
