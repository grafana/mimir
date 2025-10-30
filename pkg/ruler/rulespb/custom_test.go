// SPDX-License-Identifier: AGPL-3.0-only

package rulespb

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRuleGroupList_Equal(t *testing.T) {
	group1 := &RuleGroupDesc{
		Name:      "group1",
		Namespace: "ns1",
		Interval:  60 * time.Second,
		User:      "user1",
		Rules:     []*RuleDesc{},
	}
	group2 := &RuleGroupDesc{
		Name:      "group2",
		Namespace: "ns2",
		Interval:  30 * time.Second,
		User:      "user2",
		Rules:     []*RuleDesc{},
	}
	group1Copy := &RuleGroupDesc{
		Name:      "group1",
		Namespace: "ns1",
		Interval:  60 * time.Second,
		User:      "user1",
		Rules:     []*RuleDesc{},
	}

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
		"same length, equal contents": {
			a:   RuleGroupList{group1},
			b:   RuleGroupList{group1Copy},
			exp: true,
		},
		"same length, different contents": {
			a:   RuleGroupList{group1},
			b:   RuleGroupList{group2},
			exp: false,
		},
		"multiple groups, all equal": {
			a:   RuleGroupList{group1, group2},
			b:   RuleGroupList{group1Copy, group2},
			exp: true,
		},
		"multiple groups, different at end": {
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
