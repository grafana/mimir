// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/printer/printer_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package printer

import (
	"bytes"
	"testing"

	"github.com/alecthomas/chroma/v2/quick"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/rules/rwrulefmt"
)

func TestPrintRuleSet(t *testing.T) {
	giveRules := map[string][]rwrulefmt.RuleGroup{
		"test-namespace-1": {
			{RuleGroup: rulefmt.RuleGroup{Name: "test-rulegroup-a"}},
			{RuleGroup: rulefmt.RuleGroup{Name: "test-rulegroup-b"}},
		},
		"test-namespace-2": {
			{RuleGroup: rulefmt.RuleGroup{Name: "test-rulegroup-c"}},
			{RuleGroup: rulefmt.RuleGroup{Name: "test-rulegroup-d"}},
		},
	}

	wantJSONOutput := `[{"namespace":"test-namespace-1","rulegroup":"test-rulegroup-a"},{"namespace":"test-namespace-1","rulegroup":"test-rulegroup-b"},{"namespace":"test-namespace-2","rulegroup":"test-rulegroup-c"},{"namespace":"test-namespace-2","rulegroup":"test-rulegroup-d"}]`
	var wantColoredJSONBuffer bytes.Buffer
	err := quick.Highlight(&wantColoredJSONBuffer, wantJSONOutput, "json", "terminal", "swapoff")
	require.NoError(t, err)

	wantTabOutput := `Namespace        | Rule Group
test-namespace-1 | test-rulegroup-a
test-namespace-1 | test-rulegroup-b
test-namespace-2 | test-rulegroup-c
test-namespace-2 | test-rulegroup-d
`

	wantYAMLOutput := `- namespace: test-namespace-1
  rulegroup: test-rulegroup-a
- namespace: test-namespace-1
  rulegroup: test-rulegroup-b
- namespace: test-namespace-2
  rulegroup: test-rulegroup-c
- namespace: test-namespace-2
  rulegroup: test-rulegroup-d
`
	var wantColoredYAMLBuffer bytes.Buffer
	err = quick.Highlight(&wantColoredYAMLBuffer, wantYAMLOutput, "yaml", "terminal", "swapoff")
	require.NoError(t, err)

	tests := []struct {
		name             string
		giveDisableColor bool
		giveFormat       string
		giveIsTTY        bool
		giveForceColor   bool
		wantOutput       string
	}{
		{
			name:             "prints colorless json",
			giveDisableColor: true,
			giveFormat:       "json",
			wantOutput:       wantJSONOutput,
		},
		{
			name:       "prints colorful json",
			giveFormat: "json",
			giveIsTTY:  true,
			wantOutput: wantColoredJSONBuffer.String(),
		},
		{
			name:       "prints colorless json on non-tty",
			giveFormat: "json",
			giveIsTTY:  false,
			wantOutput: wantJSONOutput,
		},
		{
			name:           "prints colorful json on non-tty with forced colors",
			giveFormat:     "json",
			giveIsTTY:      false,
			giveForceColor: true,
			wantOutput:     wantColoredJSONBuffer.String(),
		},
		{
			name:             "prints colorless yaml",
			giveDisableColor: true,
			giveFormat:       "yaml",
			wantOutput:       wantYAMLOutput,
		},
		{
			name:       "prints colorful yaml",
			giveFormat: "yaml",
			giveIsTTY:  true,
			wantOutput: wantColoredYAMLBuffer.String(),
		},
		{
			name:           "prints colorful yaml on non-tty with forced colors",
			giveFormat:     "yaml",
			giveIsTTY:      false,
			giveForceColor: true,
			wantOutput:     wantColoredYAMLBuffer.String(),
		},
		{
			name:             "prints colorful yaml on tty with forced colors even if colors are disabled",
			giveFormat:       "yaml",
			giveIsTTY:        true,
			giveDisableColor: true,
			giveForceColor:   true,
			wantOutput:       wantColoredYAMLBuffer.String(),
		},
		{
			name:       "prints colorless yaml on non-tty",
			giveFormat: "yaml",
			giveIsTTY:  false,
			wantOutput: wantYAMLOutput,
		},
		{
			name:             "defaults to tabwriter",
			giveDisableColor: true,
			wantOutput:       wantTabOutput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(tst *testing.T) {
			var b bytes.Buffer

			p := New(tt.giveDisableColor, tt.giveForceColor, tt.giveIsTTY)
			err := p.PrintRuleSet(giveRules, tt.giveFormat, &b)

			require.NoError(tst, err)
			assert.Equal(tst, tt.wantOutput, b.String())
		})
	}
}
