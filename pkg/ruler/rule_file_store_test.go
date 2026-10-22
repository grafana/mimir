// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"cmp"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
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

func TestRuleFileStore_LoadValidatesRuleGroups(t *testing.T) {
	const user = "user-1"
	recordingRule := createRecordingRule("sum:metric", "sum(metric)")
	withGroupLabel := func(name, value string) *rulespb.RuleGroupDesc {
		g := createRuleGroup("group-1", user, recordingRule)
		g.Labels = []mimirpb.LabelAdapter{{Name: name, Value: value}}
		return g
	}

	// The registry errors have no position.
	// The rule file errors have the same text after the position.
	tests := map[string]struct {
		groups  rulespb.RuleGroupList
		scheme  model.ValidationScheme // UTF8Validation if not set
		wantErr string
	}{
		"empty group name": {
			groups:  rulespb.RuleGroupList{createRuleGroup("", user, recordingRule)},
			wantErr: "Groupname must not be empty",
		},
		"repeated group name": {
			groups:  rulespb.RuleGroupList{createRuleGroup(`group "1"`, user, recordingRule), createRuleGroup(`group "1"`, user, recordingRule)},
			wantErr: `groupname: "group "1"" is repeated in the same file`,
		},
		"metric name as a group label": {
			groups:  rulespb.RuleGroupList{withGroupLabel(model.MetricNameLabel, "metric")},
			wantErr: "invalid label name: __name__",
		},
		"invalid group label value": {
			groups:  rulespb.RuleGroupList{withGroupLabel("team", "\xff")},
			wantErr: "invalid label value: \xff",
		},
		"no record and no alert": {
			groups:  rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{Expr: "sum(metric)"})},
			wantErr: `group "group-1", rule 1, "": one of 'record' or 'alert' must be set`,
		},
		"empty expr": {
			groups:  rulespb.RuleGroupList{createRuleGroup("group-1", user, createRecordingRule("sum:metric", ""))},
			wantErr: `group "group-1", rule 1, "sum:metric": field 'expr' must be set in rule`,
		},
		"bad expr": {
			groups:  rulespb.RuleGroupList{createRuleGroup("group-1", user, createRecordingRule("sum:metric", "sum("))},
			wantErr: `group "group-1", rule 1, "sum:metric": could not parse expression: 1:5: parse error: unclosed left parenthesis`,
		},
		"annotations in a recording rule": {
			groups: rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{
				Record:      "sum:metric",
				Expr:        "sum(metric)",
				Annotations: []mimirpb.LabelAdapter{{Name: "summary", Value: "sum"}},
			})},
			wantErr: `group "group-1", rule 1, "sum:metric": invalid field 'annotations' in recording rule`,
		},
		"for in a recording rule": {
			groups:  rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{Record: "sum:metric", Expr: "sum(metric)", For: time.Minute})},
			wantErr: `group "group-1", rule 1, "sum:metric": invalid field 'for' in recording rule`,
		},
		"keep_firing_for in a recording rule": {
			groups:  rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{Record: "sum:metric", Expr: "sum(metric)", KeepFiringFor: time.Minute})},
			wantErr: `group "group-1", rule 1, "sum:metric": invalid field 'keep_firing_for' in recording rule`,
		},
		"invalid recording rule name": {
			groups:  rulespb.RuleGroupList{createRuleGroup("group-1", user, createRecordingRule("sum-metric", "sum(metric)"))},
			scheme:  model.LegacyValidation,
			wantErr: `group "group-1", rule 1, "sum-metric": invalid recording rule name: sum-metric`,
		},
		"braces in a recording rule name": {
			groups:  rulespb.RuleGroupList{createRuleGroup("group-1", user, createRecordingRule("sum:metric{job}", "sum(metric)"))},
			wantErr: `group "group-1", rule 1, "sum:metric{job}": braces present in the recording rule name; should it be in expr?: sum:metric{job}`,
		},
		"metric name as a rule label": {
			groups: rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{
				Record: "sum:metric",
				Expr:   "sum(metric)",
				Labels: []mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "metric"}},
			})},
			wantErr: `group "group-1", rule 1, "sum:metric": invalid label name: __name__`,
		},
		"invalid rule label value": {
			groups: rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{
				Record: "sum:metric",
				Expr:   "sum(metric)",
				Labels: []mimirpb.LabelAdapter{{Name: "team", Value: "\xff"}},
			})},
			wantErr: `group "group-1", rule 1, "sum:metric": invalid label value: ` + "\xff",
		},
		"invalid annotation name": {
			groups: rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{
				Alert:       "MetricHigh",
				Expr:        "metric > 1",
				Annotations: []mimirpb.LabelAdapter{{Name: "summary-text", Value: "high"}},
			})},
			scheme:  model.LegacyValidation,
			wantErr: `group "group-1", rule 1, "MetricHigh": invalid annotation name: summary-text`,
		},
		"bad template in an annotation": {
			groups: rulespb.RuleGroupList{createRuleGroup("group-1", user, &rulespb.RuleDesc{
				Alert:       "MetricHigh",
				Expr:        "metric > 1",
				Annotations: []mimirpb.LabelAdapter{{Name: "summary", Value: "{{ $value"}},
			})},
			wantErr: `group "group-1", rule 1, "MetricHigh": annotation "summary": template: __alert_MetricHigh:1: unclosed action`,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			scheme := cmp.Or(tc.scheme, model.UTF8Validation)
			for _, registryEnabled := range []bool{false, true} {
				t.Run(fmt.Sprintf("registry_enabled=%v", registryEnabled), func(t *testing.T) {
					store := NewRuleFileStore(Config{RulePath: t.TempDir(), RuleRegistryEnabled: registryEnabled}, log.NewNopLogger())
					_, files, err := store.mapRules(user, tc.groups)
					require.NoError(t, err)
					require.Len(t, files, 1)

					_, errs := store.groupLoader(user).Load(files[0], false, scheme)
					require.Len(t, errs, 1)
					if registryEnabled {
						require.EqualError(t, errs[0], files[0]+": "+tc.wantErr)
					} else {
						require.ErrorContains(t, errs[0], tc.wantErr)
					}
				})
			}
		})
	}
}

func TestRegistryRuleFileStore_LoadValidatesChangedNamespaces(t *testing.T) {
	const user = "user-1"
	invalid := createRuleGroup("group-1", user, &rulespb.RuleDesc{Record: "sum:metric", Expr: "sum(metric)", For: time.Minute})
	valid := createRuleGroup("group-1", user, createRecordingRule("sum:metric", "sum(metric)"))

	store := NewRuleFileStore(Config{RulePath: t.TempDir(), RuleRegistryEnabled: true}, log.NewNopLogger())
	steps := []struct {
		group      *rulespb.RuleGroupDesc
		wantUpdate bool
		wantErrs   int
	}{
		{group: invalid, wantUpdate: true, wantErrs: 1},
		{group: invalid, wantUpdate: false, wantErrs: 1},
		{group: valid, wantUpdate: true, wantErrs: 0},
		{group: invalid, wantUpdate: true, wantErrs: 1},
	}
	for i, step := range steps {
		update, files, err := store.mapRules(user, rulespb.RuleGroupList{step.group})
		require.NoError(t, err)
		require.Equal(t, step.wantUpdate, update, "step %d", i)
		require.Len(t, files, 1)

		_, errs := store.groupLoader(user).Load(files[0], false, model.UTF8Validation)
		require.Len(t, errs, step.wantErrs, "step %d", i)
	}
}
