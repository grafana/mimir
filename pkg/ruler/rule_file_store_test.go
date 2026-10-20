// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

func TestNewRuleFileStore(t *testing.T) {
	cfg := Config{RulePath: t.TempDir()}
	require.IsType(t, &fsRuleFileStore{}, NewRuleFileStore(cfg, log.NewNopLogger()))

	cfg.RuleRegistryEnabled = true
	require.IsType(t, &registryRuleFileStore{}, NewRuleFileStore(cfg, log.NewNopLogger()))
}

func TestRuleFileStore_SyncLoadsRuleGroups(t *testing.T) {
	for _, registryEnabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("registry_enabled=%v", registryEnabled), func(t *testing.T) {
			cfg := defaultRulerConfig(t)
			cfg.RuleRegistryEnabled = registryEnabled
			m := prepareRulerManager(t, cfg)
			t.Cleanup(m.Stop)

			const user = "user-1"
			m.SyncFullRuleGroups(t.Context(), map[string]rulespb.RuleGroupList{
				user: {
					createRuleGroup("group-1", user, createRecordingRule("sum:metric_1", "sum(metric_1)")),
					createRuleGroup("group-2", user, createRecordingRule("sum:metric_2", "sum(metric_2)")),
				},
			})
			m.Start()

			require.Eventually(t, func() bool {
				return len(m.GetRules(user)) == 2
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}
