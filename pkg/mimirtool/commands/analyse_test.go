// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/util"
)

func TestAnalyzeCommand_EnableExperimentalFunctions(t *testing.T) {
	tempDir := t.TempDir()
	ruleFile := tempDir + "/test_rules.yaml"

	ruleContent := `groups:
  - name: test_group
    interval: 1m
    rules:
      - record: test_metric
        expr: mad_over_time(some_metric[5m])
`
	require.NoError(t, os.WriteFile(ruleFile, []byte(ruleContent), 0644))

	t.Run("experimental functions disabled", func(t *testing.T) {
		_, err := AnalyzeRuleFiles([]string{ruleFile}, model.LegacyValidation, util.CreatePromQLParser(false), log.NewNopLogger())

		assert.Error(t, err, "analyzing rules with experimental functions should fail when flag is disabled")
	})

	t.Run("experimental functions enabled", func(t *testing.T) {
		output, err := AnalyzeRuleFiles([]string{ruleFile}, model.LegacyValidation, util.CreatePromQLParser(true), log.NewNopLogger())

		assert.NoError(t, err, "analyzing rules with experimental functions should succeed when flag is enabled")
		require.NotNil(t, output, "output should not be nil")
		assert.Contains(t, output.OverallMetrics, "some_metric", "analyzed metrics should include 'some_metric'")
	})
}
