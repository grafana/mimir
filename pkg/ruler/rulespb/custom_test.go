// SPDX-License-Identifier: AGPL-3.0-only

package rulespb

import (
	"testing"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/stretchr/testify/require"
)

func TestRuleGroupList_Equal(t *testing.T) {
	group1 := makeTestRuleGroupDesc("group1", "ns1", 60*time.Second)
	group2 := makeTestRuleGroupDesc("group2", "ns2", 30*time.Second)
	group1Copy := makeTestRuleGroupDesc("group1", "ns1", 60*time.Second)

	sharedSlice := RuleGroupList{group1, group2}

	tests := map[string]struct {
		a   RuleGroupList
		b   RuleGroupList
		exp bool
	}{
		"both nil": {
			a:   nil,
			b:   nil,
			exp: true,
		},
		"a nil, b not nil": {
			a:   nil,
			b:   RuleGroupList{group1},
			exp: false,
		},
		"a not nil, b nil": {
			a:   RuleGroupList{group1},
			b:   nil,
			exp: false,
		},
		"both empty": {
			a:   RuleGroupList{},
			b:   RuleGroupList{},
			exp: true,
		},
		"same slice pointer": {
			a:   sharedSlice,
			b:   sharedSlice,
			exp: true,
		},
		"different lengths": {
			a:   RuleGroupList{group1},
			b:   RuleGroupList{group1, group2},
			exp: false,
		},
		"same length, different contents": {
			a:   RuleGroupList{group1},
			b:   RuleGroupList{group2},
			exp: false,
		},
		"multiple groups, same contents": {
			a:   RuleGroupList{group1, group2},
			b:   RuleGroupList{group1Copy, group2},
			exp: true,
		},
		"multiple groups, different contents": {
			a:   RuleGroupList{group1, group1},
			b:   RuleGroupList{group1, group2},
			exp: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.exp, tc.a.Equal(tc.b))
		})
	}
}

func TestRuleGroupList_Formatted(t *testing.T) {
	group1 := makeTestRuleGroupDesc("group1", "ns1", 60*time.Second)
	group2 := makeTestRuleGroupDesc("group2", "ns1", 30*time.Second)
	group3 := makeTestRuleGroupDesc("group3", "ns2", 15*time.Second)

	list := RuleGroupList{group1, group2, group3}

	t.Run("Formatted", func(t *testing.T) {
		result := list.Formatted()

		require.Len(t, result, 2)
		require.Len(t, result["ns1"], 2)
		require.Len(t, result["ns2"], 1)
		require.Equal(t, "group1", result["ns1"][0].Name)
		require.Equal(t, "group2", result["ns1"][1].Name)
		require.Equal(t, "group3", result["ns2"][0].Name)
	})

	t.Run("FormattedProto", func(t *testing.T) {
		result := list.FormattedProto()

		require.Len(t, result, 2)
		require.Len(t, result["ns1"], 2)
		require.Len(t, result["ns2"], 1)
		require.Equal(t, "group1", result["ns1"][0].Name)
		require.Equal(t, "group2", result["ns1"][1].Name)
		require.Equal(t, "group3", result["ns2"][0].Name)
	})
}

func makeTestRuleGroupDesc(name, namespace string, interval time.Duration) *RuleGroupDesc {
	return &RuleGroupDesc{
		Name:      name,
		Namespace: namespace,
		Interval:  interval,
		User:      "user1",
		Rules: []*RuleDesc{
			{
				Expr: "up == 0",
				For:  5 * time.Minute,
				Labels: []mimirpb.LabelAdapter{
					{Name: "severity", Value: "critical"},
					{Name: "team", Value: "test"},
				},
			},
		},
	}
}
