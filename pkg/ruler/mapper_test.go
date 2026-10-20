// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/mapper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"net/url"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	testUser1 = "user1"
	testUser2 = "user2"

	fileOneEncoded = url.PathEscape("file /one")
	fileTwoEncoded = url.PathEscape("file /two")

	fileOneUserOnePath = "/rules/user1/" + fileOneEncoded
	fileTwoUserOnePath = "/rules/user1/" + fileTwoEncoded
	fileOneUserTwoPath = "/rules/user2/" + fileOneEncoded
	fileTwoUserTwoPath = "/rules/user2/" + fileTwoEncoded

	specialCharFile        = "+A_/ReallyStrange<>NAME:SPACE/?"
	specialCharFileEncoded = url.PathEscape(specialCharFile)
	specialCharFilePath    = "/rules/user1/" + specialCharFileEncoded

	initialRuleSet           map[string][]rulefmt.RuleGroup
	outOfOrderRuleSet        map[string][]rulefmt.RuleGroup
	updatedRuleSet           map[string][]rulefmt.RuleGroup
	twoFilesRuleSet          map[string][]rulefmt.RuleGroup
	twoFilesUpdatedRuleSet   map[string][]rulefmt.RuleGroup
	twoFilesDeletedRuleSet   map[string][]rulefmt.RuleGroup
	specialCharactersRuleSet map[string][]rulefmt.RuleGroup
)

func setupRuleSets() {
	const record = "example_rule"
	const expr = "example_expr"
	const recordUpdated = "example_ruleupdated"
	const exprUpdated = "example_exprupdated"
	initialRuleSet = map[string][]rulefmt.RuleGroup{
		"file /one": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
	}
	outOfOrderRuleSet = map[string][]rulefmt.RuleGroup{
		"file /one": {
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
	}
	updatedRuleSet = map[string][]rulefmt.RuleGroup{
		"file /one": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
			{
				Name: "rulegroup_three",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
	}
	twoFilesRuleSet = map[string][]rulefmt.RuleGroup{
		"file /one": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
		"file /two": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
	}
	twoFilesUpdatedRuleSet = map[string][]rulefmt.RuleGroup{
		"file /one": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
		"file /two": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: recordUpdated,
						Expr:   exprUpdated,
					},
				},
			},
		},
	}
	twoFilesDeletedRuleSet = map[string][]rulefmt.RuleGroup{
		"file /one": {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
			{
				Name: "rulegroup_two",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
	}
	specialCharactersRuleSet = map[string][]rulefmt.RuleGroup{
		specialCharFile: {
			{
				Name: "rulegroup_one",
				Rules: []rulefmt.Rule{
					{
						Record: record,
						Expr:   expr,
					},
				},
			},
		},
	}
}

func testRuleSetAsProto(userID string, in map[string][]rulefmt.RuleGroup) map[string]rulespb.RuleGroupList {
	result := make(map[string]rulespb.RuleGroupList, len(in))
	for ns, groups := range in {
		protoGroups := make(rulespb.RuleGroupList, 0, len(groups))
		for _, group := range groups {
			protoGroups = append(protoGroups, rulespb.ToProto(userID, ns, group))
		}
		result[ns] = protoGroups
	}
	return result
}

func Test_registry_MapRules(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("basic rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, initialRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		_, ok = userRules[fileOneUserOnePath]
		require.True(t, ok)
	})

	t.Run("identical rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, initialRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		_, ok = userRules[fileOneUserOnePath]
		require.True(t, ok)
	})

	t.Run("out of order identical rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, outOfOrderRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		_, ok = userRules[fileOneUserOnePath]
		require.True(t, ok)
	})

	t.Run("updated rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, updatedRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		_, ok = userRules[fileOneUserOnePath]
		require.True(t, ok)
	})
}

func Test_mapper_MapRules(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: l,
	}

	t.Run("basic rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
	})

	t.Run("identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, initialRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
	})

	t.Run("out of order identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, outOfOrderRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
	})

	t.Run("updated rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, updatedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
	})
}

func Test_registry_MapRulesMultipleFiles(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("basic rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, initialRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, userRules, fileOneUserOnePath)
	})

	t.Run("add a file", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, twoFilesRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.Contains(t, files, fileOneUserOnePath)
		require.Contains(t, files, fileTwoUserOnePath)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, userRules, fileOneUserOnePath)
		require.Contains(t, userRules, fileTwoUserOnePath)
	})

	t.Run("update one file", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, twoFilesUpdatedRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.Contains(t, files, fileOneUserOnePath)
		require.Contains(t, files, fileTwoUserOnePath)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, userRules, fileOneUserOnePath)
		require.Contains(t, userRules, fileTwoUserOnePath)
	})

	t.Run("delete one file", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, twoFilesDeletedRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, userRules, fileOneUserOnePath)
		require.NotContains(t, userRules, fileTwoUserOnePath)
	})
}

func Test_mapper_MapRulesMultipleFiles(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: l,
	}

	t.Run("basic rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
	})

	t.Run("add a file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.Contains(t, files, fileOneUserOnePath)
		require.Contains(t, files, fileTwoUserOnePath)
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
		requireFileExists(t, m.FS, fileTwoUserOnePath)
	})

	t.Run("update one file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, twoFilesUpdatedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.Contains(t, files, fileOneUserOnePath)
		require.Contains(t, files, fileTwoUserOnePath)
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
		requireFileExists(t, m.FS, fileTwoUserOnePath)
	})

	t.Run("delete one file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, twoFilesDeletedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
		requireFileNotExists(t, m.FS, fileTwoUserOnePath)
	})
}

func Test_registry_MapRulesMultipleTenants(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("basic rulegroup tenant 1", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, initialRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, userRules, fileOneUserOnePath)
	})

	t.Run("basic rulegroup tenant 2 still considered new", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser2, initialRuleSet)
		updated, files, err := r.MapRules(testUser2, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserTwoPath, files[0])
		require.NoError(t, err)

		user2Rules, ok := r.rules[testUser2]
		require.True(t, ok)
		require.Contains(t, user2Rules, fileOneUserTwoPath)
	})

	t.Run("simultaneous update and add tenant 2", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser2, twoFilesRuleSet)
		updated, files, err := r.MapRules(testUser2, protoRules)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.Contains(t, files, fileOneUserTwoPath)
		require.Contains(t, files, fileTwoUserTwoPath)
		require.NoError(t, err)

		user1Rules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, user1Rules, fileOneUserOnePath)
		user2Rules, ok := r.rules[testUser2]
		require.True(t, ok)
		require.Contains(t, user2Rules, fileOneUserTwoPath)
		require.Contains(t, user2Rules, fileTwoUserTwoPath)
	})

	t.Run("identical rulegroup tenant 1 not considered updated", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, initialRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, userRules, fileOneUserOnePath)
	})

	t.Run("removal of tenant 1 groups keeps tenant 2 groups", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, map[string][]rulefmt.RuleGroup{})
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Empty(t, userRules)
		user2Rules, ok := r.rules[testUser2]
		require.True(t, ok)
		require.Contains(t, user2Rules, fileOneUserTwoPath)
		require.Contains(t, user2Rules, fileTwoUserTwoPath)
	})
}

func Test_mapper_MapRulesMultipleTenants(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: l,
	}

	t.Run("basic rulegroup tenant 1", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
	})

	t.Run("basic rulegroup tenant 2 still considered new", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser2, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserTwoPath, files[0])
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserTwoPath)
	})

	t.Run("simultaneous update and add tenant 2", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser2, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.Contains(t, files, fileOneUserTwoPath)
		require.Contains(t, files, fileTwoUserTwoPath)
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
		requireFileExists(t, m.FS, fileOneUserTwoPath)
		requireFileExists(t, m.FS, fileTwoUserTwoPath)
	})

	t.Run("identical rulegroup tenant 1 not considered updated", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, initialRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		requireFileExists(t, m.FS, fileOneUserOnePath)
	})

	t.Run("removal of tenant 1 groups keeps tenant 2 groups", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, map[string][]rulefmt.RuleGroup{})
		require.True(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)

		requireFileNotExists(t, m.FS, fileOneUserOnePath)
		requireFileExists(t, m.FS, fileOneUserTwoPath)
		requireFileExists(t, m.FS, fileOneUserTwoPath)
	})
}

func Test_registry_MapRulesSpecialCharNamespace(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("create special characters rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, specialCharactersRuleSet)
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, specialCharFilePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.Contains(t, userRules, specialCharFilePath)
	})

	t.Run("delete special characters rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, map[string][]rulefmt.RuleGroup{})
		updated, files, err := r.MapRules(testUser1, protoRules)
		require.True(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser1]
		require.True(t, ok)
		require.NotContains(t, userRules, specialCharFilePath)
	})
}

func Test_registry_MapRulesCopiesLabelStrings(t *testing.T) {
	group := &rulespb.RuleGroupDesc{
		Name:      "group",
		Namespace: "namespace",
		Labels:    []mimirpb.LabelAdapter{{Name: "group_label", Value: "group_value"}},
		Rules: []*rulespb.RuleDesc{{
			Alert:       "Down",
			Expr:        "up == 0",
			Labels:      []mimirpb.LabelAdapter{{Name: "severity", Value: "critical"}},
			Annotations: []mimirpb.LabelAdapter{{Name: "summary", Value: "down"}},
		}},
	}
	buf, err := group.Marshal()
	require.NoError(t, err)
	// Unmarshal makes label strings that point into buf.
	unmarshaled := &rulespb.RuleGroupDesc{}
	require.NoError(t, unmarshaled.Unmarshal(buf))

	r := newRuleRegistry("/rules", log.NewNopLogger())
	_, files, err := r.MapRules(testUser1, rulespb.RuleGroupList{unmarshaled}.FormattedProto())
	require.NoError(t, err)
	require.Len(t, files, 1)

	// The ruler does not use the read buffer after the sync.
	for i := range buf {
		buf[i] = 'x'
	}
	require.Equal(t, []rulefmt.RuleGroup{rulespb.FromProto(group)}, r.get(testUser1, files[0]).Groups)
}

func Test_mapper_MapRulesSpecialCharNamespace(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: l,
	}

	t.Run("create special characters rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, specialCharactersRuleSet)
		require.NoError(t, err)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, specialCharFilePath, files[0])

		requireFileExists(t, m.FS, specialCharFilePath)
	})

	t.Run("delete special characters rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, map[string][]rulefmt.RuleGroup{})
		require.NoError(t, err)
		require.True(t, updated)
		require.Len(t, files, 0)

		requireFileNotExists(t, m.FS, specialCharFilePath)
	})
}

func Test_registry_users(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("should not fail if path does not exist", func(t *testing.T) {
		r := newRuleRegistry("/rules", l)

		actual, err := r.users()
		require.NoError(t, err)
		require.Empty(t, actual)
	})

	t.Run("adding a rulegroup returns the user", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, initialRuleSet)
		_, _, err := r.MapRules(testUser1, protoRules)
		require.NoError(t, err)

		result, err := r.users()

		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Contains(t, result, testUser1)
	})

	t.Run("adding a rulegroup for a second user returns both users", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser2, initialRuleSet)
		_, _, err := r.MapRules(testUser2, protoRules)
		require.NoError(t, err)

		result, err := r.users()

		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, testUser1)
		require.Contains(t, result, testUser2)
	})

	t.Run("deleting a user's rule groups deschedules that user", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser1, map[string][]rulefmt.RuleGroup{})
		_, _, err := r.MapRules(testUser1, protoRules)
		require.NoError(t, err)

		result, err := r.users()

		require.NoError(t, err)
		require.Len(t, result, 1)
		require.NotContains(t, result, testUser1)
		require.Contains(t, result, testUser2)
	})

	t.Run("cleanup removes all users", func(t *testing.T) {
		r.cleanup()

		result, err := r.users()

		require.NoError(t, err)
		require.Empty(t, result)
	})
}

func Test_mapper_users(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: l,
	}

	t.Run("should not fail if path does not exist", func(t *testing.T) {
		m := &mapper{
			Path:   "/path-does-not-exist",
			FS:     afero.NewMemMapFs(),
			logger: log.NewNopLogger(),
		}

		actual, err := m.users()
		require.NoError(t, err)
		require.Empty(t, actual)
	})

	t.Run("adding a rulegroup returns the user", func(t *testing.T) {
		_, _, err := m.MapRules(testUser1, initialRuleSet)
		require.NoError(t, err)

		result, err := m.users()

		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Contains(t, result, testUser1)
	})

	t.Run("adding a rulegroup for a second user returns both users", func(t *testing.T) {
		_, _, err := m.MapRules(testUser2, initialRuleSet)
		require.NoError(t, err)

		result, err := m.users()

		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, testUser1)
		require.Contains(t, result, testUser2)
	})

	t.Run("deleting a user's rule groups keeps that user", func(t *testing.T) {
		_, _, err := m.MapRules(testUser1, map[string][]rulefmt.RuleGroup{})
		require.NoError(t, err)

		// This happens because MapRules does not delete the user directory if it cleared all the files inside.
		// However, users() only looks at the set of user directories.
		// This is something that can be improved on in the future.
		result, err := m.users()

		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, testUser1)
		require.Contains(t, result, testUser2)
	})

	t.Run("cleanup removes all users", func(t *testing.T) {
		m.cleanup()

		result, err := m.users()

		require.NoError(t, err)
		require.Empty(t, result)
	})
}

func Test_mapper_ExprWithLeadingNewlines(t *testing.T) {
	m := &mapper{
		Path:   "/rules",
		FS:     afero.NewMemMapFs(),
		logger: log.NewNopLogger(),
	}

	ruleConfigs := map[string][]rulefmt.RuleGroup{
		"rules.yaml": {
			{
				Name: "test_group",
				Rules: []rulefmt.Rule{
					{
						Record: "test_rule",
						Expr:   "\n\n# comment\nup > 0\n",
					},
				},
			},
		},
	}

	updated, files, err := m.MapRules("user1", ruleConfigs)
	require.NoError(t, err)
	require.True(t, updated)
	require.Len(t, files, 1)

	// Verify the written file can be parsed back by the rulefmt parser.
	content, err := afero.ReadFile(m.FS, files[0])
	require.NoError(t, err)

	var rgs rulefmt.RuleGroups
	err = yaml.Unmarshal(content, &rgs)
	require.NoError(t, err)
	require.Len(t, rgs.Groups, 1)
	require.Len(t, rgs.Groups[0].Rules, 1)
	// Leading/trailing whitespace should be trimmed.
	require.Equal(t, "# comment\nup > 0", rgs.Groups[0].Rules[0].Expr)
}

func Test_cleanRuleGroupExprs(t *testing.T) {
	groups := []rulefmt.RuleGroup{
		{
			Name: "group1",
			Rules: []rulefmt.Rule{
				{Record: "r1", Expr: "\n\n  up > 0\n"},
				{Record: "r2", Expr: "rate(foo[5m])"},
			},
		},
	}

	cleaned := cleanRuleGroupExprs(groups)

	// Cleaned expressions should have whitespace trimmed.
	require.Equal(t, "up > 0", cleaned[0].Rules[0].Expr)
	require.Equal(t, "rate(foo[5m])", cleaned[0].Rules[1].Expr)
	// Original should be unmodified.
	require.Equal(t, "\n\n  up > 0\n", groups[0].Rules[0].Expr)
}

func Test_FSLoader_LoadRules(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	fs := afero.NewMemMapFs()
	m := &mapper{
		Path:   "/rules",
		FS:     fs,
		logger: l,
	}

	t.Run("basic rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserOnePath, files[0])
		require.NoError(t, err)

		loader := NewFSLoader(fs)
		loaded, errs := loader.Load(fileOneUserOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loaded)
		require.Len(t, loaded.Groups, 2)
		// Groups are sorted in reverse order by name, so "two" comes before "one".
		require.Equal(t, "rulegroup_two", loaded.Groups[0].Name)
		require.Equal(t, "rulegroup_one", loaded.Groups[1].Name)
	})

	t.Run("multiple files", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser1, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.NoError(t, err)

		loader := NewFSLoader(fs)
		loaded, errs := loader.Load(fileOneUserOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loaded)
		require.Len(t, loaded.Groups, 2)

		loaded2, errs := loader.Load(fileTwoUserOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loaded2)
		require.Len(t, loaded2.Groups, 1)
		require.Equal(t, "rulegroup_one", loaded2.Groups[0].Name)
	})

	t.Run("multiple tenants", func(t *testing.T) {
		// Map rules for testUser2.
		updated, files, err := m.MapRules(testUser2, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.NoError(t, err)

		loader := NewFSLoader(fs)
		loaded, errs := loader.Load(fileOneUserOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loaded)
		require.Len(t, loaded.Groups, 2)

		loadedUser2File1, errs := loader.Load(fileOneUserTwoPath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loadedUser2File1)
		require.Len(t, loadedUser2File1.Groups, 2)

		loadedUser2File2, errs := loader.Load(fileTwoUserTwoPath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loadedUser2File2)
		require.Len(t, loadedUser2File2.Groups, 1)
		require.Equal(t, "rulegroup_one", loadedUser2File2.Groups[0].Name)
	})
}

func Test_registryLoader_Load(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")

	t.Run("each load returns an equal, distinct result", func(t *testing.T) {
		setupRuleSets()
		r := newRuleRegistry("/rules", l)
		_, files, err := r.MapRules(testUser1, testRuleSetAsProto(testUser1, initialRuleSet))
		require.NoError(t, err)

		loader := newRegistryLoader(r, testUser1)
		first, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		second, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, first, second)
		require.NotSame(t, first, second)
	})

	t.Run("a load after the rules change returns the new groups", func(t *testing.T) {
		setupRuleSets()
		r := newRuleRegistry("/rules", l)
		_, files, err := r.MapRules(testUser1, testRuleSetAsProto(testUser1, initialRuleSet))
		require.NoError(t, err)

		loader := newRegistryLoader(r, testUser1)
		loaded, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Len(t, loaded.Groups, 2)

		_, _, err = r.MapRules(testUser1, testRuleSetAsProto(testUser1, updatedRuleSet))
		require.NoError(t, err)

		loaded, errs = loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Len(t, loaded.Groups, 3)
	})

	t.Run("each file loads its own groups", func(t *testing.T) {
		setupRuleSets()
		r := newRuleRegistry("/rules", l)
		_, files, err := r.MapRules(testUser1, testRuleSetAsProto(testUser1, twoFilesRuleSet))
		require.NoError(t, err)
		require.Len(t, files, 2)

		loader := newRegistryLoader(r, testUser1)
		one, errs := loader.Load(fileOneUserOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Len(t, one.Groups, 2)
		two, errs := loader.Load(fileTwoUserOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Len(t, two.Groups, 1)
	})

	t.Run("mutating a result does not change a later result", func(t *testing.T) {
		// tenant.NormalizeTenantIDs sorts and deduplicates SourceTenants in place.
		// The registry keeps its protos across loads.
		// So a loaded group must not share the SourceTenants slice with the registry.
		r := newRuleRegistry("/rules", l)
		ruleConfigs := map[string][]rulefmt.RuleGroup{
			"file /one": {
				{
					Name:          "federated_group",
					SourceTenants: []string{"tenant-b", "tenant-a"},
					Rules: []rulefmt.Rule{
						{Record: "example_rule", Expr: "example_expr"},
					},
				},
			},
		}
		_, files, err := r.MapRules(testUser1, testRuleSetAsProto(testUser1, ruleConfigs))
		require.NoError(t, err)

		loader := newRegistryLoader(r, testUser1)
		first, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		slices.Sort(first.Groups[0].SourceTenants)
		require.Equal(t, []string{"tenant-a", "tenant-b"}, first.Groups[0].SourceTenants)

		second, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, []string{"tenant-b", "tenant-a"}, second.Groups[0].SourceTenants)
	})
}

func Test_registryLoader_RetryAfterFailedLoad(t *testing.T) {
	// LoadGroups stops at the first file that fails to load.
	// Then Update returns, and the running groups do not change.
	// On the retry, the registry must still return the groups of every file.
	setupRuleSets()
	r := newRuleRegistry("/rules", log.NewNopLogger())
	broken := map[string][]rulefmt.RuleGroup{
		"file /one": initialRuleSet["file /one"],
		"file /two": {{Name: "broken", Rules: []rulefmt.Rule{{Record: "broken", Expr: "sum("}}}},
	}
	_, files, err := r.MapRules(testUser1, testRuleSetAsProto(testUser1, broken))
	require.NoError(t, err)
	require.Len(t, files, 2)

	manager := promRules.NewManager(&promRules.ManagerOptions{GroupLoader: newRegistryLoader(r, testUser1)})

	_, errs := manager.LoadGroups(time.Minute, labels.EmptyLabels(), "", nil, false, files...)
	require.NotEmpty(t, errs)
	require.ErrorContains(t, errs[0], fileTwoUserOnePath)

	fixed := map[string][]rulefmt.RuleGroup{
		"file /one": initialRuleSet["file /one"],
		"file /two": {{Name: "fixed", Rules: []rulefmt.Rule{{Record: "fixed", Expr: "up"}}}},
	}
	_, files, err = r.MapRules(testUser1, testRuleSetAsProto(testUser1, fixed))
	require.NoError(t, err)

	groups, errs := manager.LoadGroups(time.Minute, labels.EmptyLabels(), "", nil, false, files...)
	require.Empty(t, errs)
	require.Len(t, groups, 3)
}

func requireFileExists(t *testing.T, fs afero.Fs, path string) {
	t.Helper()

	exists, err := afero.Exists(fs, path)
	require.NoError(t, err)
	require.True(t, exists, "file %s did not exist", path)
}

func requireFileNotExists(t *testing.T, fs afero.Fs, path string) {
	t.Helper()

	exists, err := afero.Exists(fs, path)
	require.NoError(t, err)
	require.False(t, exists, "file %s existed, but shouldn't", path)
}
