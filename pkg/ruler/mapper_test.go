// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/mapper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"net/url"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

var (
	testUser  = "user1"
	testUser2 = "user2"

	fileOneEncoded = url.PathEscape("file /one")
	fileTwoEncoded = url.PathEscape("file /two")

	fileOnePath        = "/rules/user1/" + fileOneEncoded
	fileTwoPath        = "/rules/user1/" + fileTwoEncoded
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
		protoRules := testRuleSetAsProto(testUser, initialRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		_, ok = userRules[fileOnePath]
		require.True(t, ok)
	})

	t.Run("identical rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, initialRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		_, ok = userRules[fileOnePath]
		require.True(t, ok)
	})

	t.Run("out of order identical rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, outOfOrderRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		_, ok = userRules[fileOnePath]
		require.True(t, ok)
	})

	t.Run("updated rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, updatedRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		_, ok = userRules[fileOnePath]
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
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("out of order identical rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, outOfOrderRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("updated rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, updatedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
	})
}

func Test_registry_MapRulesMultipleFiles(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("basic rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, initialRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, userRules, fileOnePath)
	})

	t.Run("add a file", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, twoFilesRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.True(t, sliceContains(t, fileOnePath, files))
		require.True(t, sliceContains(t, fileTwoPath, files))
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, userRules, fileOnePath)
		require.Contains(t, userRules, fileTwoPath)
	})

	t.Run("update one file", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, twoFilesUpdatedRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.True(t, sliceContains(t, fileOnePath, files))
		require.True(t, sliceContains(t, fileTwoPath, files))
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, userRules, fileOnePath)
		require.Contains(t, userRules, fileTwoPath)
	})

	t.Run("delete one file", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, twoFilesDeletedRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, userRules, fileOnePath)
		require.NotContains(t, userRules, fileTwoPath)
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
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("add a file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.True(t, sliceContains(t, fileOnePath, files))
		require.True(t, sliceContains(t, fileTwoPath, files))
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, fileTwoPath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("update one file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, twoFilesUpdatedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.True(t, sliceContains(t, fileOnePath, files))
		require.True(t, sliceContains(t, fileTwoPath, files))
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, fileTwoPath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("delete one file", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, twoFilesDeletedRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, fileTwoPath)
		require.False(t, exists)
		require.NoError(t, err)
	})
}

func Test_registry_MapRulesMultipleTenants(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("basic rulegroup tenant 1", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, initialRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, userRules, fileOnePath)
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
		require.True(t, sliceContains(t, fileOneUserTwoPath, files))
		require.True(t, sliceContains(t, fileTwoUserTwoPath, files))
		require.NoError(t, err)

		user1Rules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, user1Rules, fileOnePath)
		user2Rules, ok := r.rules[testUser2]
		require.True(t, ok)
		require.Contains(t, user2Rules, fileOneUserTwoPath)
		require.Contains(t, user2Rules, fileTwoUserTwoPath)
	})

	t.Run("identical rulegroup tenant 1 not considered updated", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, initialRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, userRules, fileOnePath)
	})

	t.Run("removal of tenant 1 groups keeps tenant 2 groups", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, map[string][]rulefmt.RuleGroup{})
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
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
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("basic rulegroup tenant 2 still considered new", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser2, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOneUserTwoPath, files[0])
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOneUserTwoPath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("simultaneous update and add tenant 2", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser2, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.True(t, sliceContains(t, fileOneUserTwoPath, files))
		require.True(t, sliceContains(t, fileTwoUserTwoPath, files))
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, fileOneUserTwoPath)
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, fileTwoUserTwoPath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("identical rulegroup tenant 1 not considered updated", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.False(t, updated)
		require.Len(t, files, 1)
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("removal of tenant 1 groups keeps tenant 2 groups", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, map[string][]rulefmt.RuleGroup{})
		require.True(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)

		exists, err := afero.Exists(m.FS, fileOnePath)
		require.False(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, fileOneUserTwoPath)
		require.True(t, exists)
		require.NoError(t, err)
		exists, err = afero.Exists(m.FS, fileTwoUserTwoPath)
		require.True(t, exists)
		require.NoError(t, err)
	})
}

func Test_registry_MapRulesSpecialCharNamespace(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	r := newRuleRegistry("/rules", l)

	t.Run("create special characters rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, specialCharactersRuleSet)
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, specialCharFilePath, files[0])
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.Contains(t, userRules, specialCharFilePath)
	})

	t.Run("delete special characters rulegroup", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, map[string][]rulefmt.RuleGroup{})
		updated, files, err := r.MapRules(testUser, protoRules)
		require.True(t, updated)
		require.Len(t, files, 0)
		require.NoError(t, err)

		userRules, ok := r.rules[testUser]
		require.True(t, ok)
		require.NotContains(t, userRules, specialCharFilePath)
	})
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
		updated, files, err := m.MapRules(testUser, specialCharactersRuleSet)
		require.NoError(t, err)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, specialCharFilePath, files[0])

		exists, err := afero.Exists(m.FS, specialCharFilePath)
		require.NoError(t, err)
		require.True(t, exists)
	})

	t.Run("delete special characters rulegroup", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, map[string][]rulefmt.RuleGroup{})
		require.NoError(t, err)
		require.True(t, updated)
		require.Len(t, files, 0)

		exists, err := afero.Exists(m.FS, specialCharFilePath)
		require.NoError(t, err)
		require.False(t, exists)
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
		protoRules := testRuleSetAsProto(testUser, initialRuleSet)
		_, _, err := r.MapRules(testUser, protoRules)
		require.NoError(t, err)

		result, err := r.users()

		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Contains(t, result, testUser)
	})

	t.Run("adding a rulegroup for a second user returns both users", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser2, initialRuleSet)
		_, _, err := r.MapRules(testUser2, protoRules)
		require.NoError(t, err)

		result, err := r.users()

		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, testUser)
		require.Contains(t, result, testUser2)
	})

	t.Run("deleting a user's rule groups deschedules that user", func(t *testing.T) {
		protoRules := testRuleSetAsProto(testUser, map[string][]rulefmt.RuleGroup{})
		_, _, err := r.MapRules(testUser, protoRules)
		require.NoError(t, err)

		result, err := r.users()

		require.NoError(t, err)
		require.Len(t, result, 1)
		require.NotContains(t, result, testUser)
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
		_, _, err := m.MapRules(testUser, initialRuleSet)
		require.NoError(t, err)

		result, err := m.users()

		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Contains(t, result, testUser)
	})

	t.Run("adding a rulegroup for a second user returns both users", func(t *testing.T) {
		_, _, err := m.MapRules(testUser2, initialRuleSet)
		require.NoError(t, err)

		result, err := m.users()

		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, testUser)
		require.Contains(t, result, testUser2)
	})

	t.Run("deleting a user's rule groups keeps that user", func(t *testing.T) {
		_, _, err := m.MapRules(testUser, map[string][]rulefmt.RuleGroup{})
		require.NoError(t, err)

		// This happens because MapRules does not delete the user directory if it cleared all the files inside.
		// However, users() only looks at the set of user directories.
		// This is something that can be improved on in the future.
		result, err := m.users()

		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Contains(t, result, testUser)
		require.Contains(t, result, testUser2)
	})

	t.Run("cleanup removes all users", func(t *testing.T) {
		m.cleanup()

		result, err := m.users()

		require.NoError(t, err)
		require.Empty(t, result)
	})
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
		updated, files, err := m.MapRules(testUser, initialRuleSet)
		require.True(t, updated)
		require.Len(t, files, 1)
		require.Equal(t, fileOnePath, files[0])
		require.NoError(t, err)

		loader := NewFSLoader(fs)
		loaded, errs := loader.Load(fileOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loaded)
		require.Len(t, loaded.Groups, 2)
		// Groups are sorted in reverse order by name, so "two" comes before "one".
		require.Equal(t, "rulegroup_two", loaded.Groups[0].Name)
		require.Equal(t, "rulegroup_one", loaded.Groups[1].Name)
	})

	t.Run("multiple files", func(t *testing.T) {
		updated, files, err := m.MapRules(testUser, twoFilesRuleSet)
		require.True(t, updated)
		require.Len(t, files, 2)
		require.NoError(t, err)

		loader := NewFSLoader(fs)
		loaded, errs := loader.Load(fileOnePath, false, model.LegacyValidation)
		require.Empty(t, errs)
		require.NotNil(t, loaded)
		require.Len(t, loaded.Groups, 2)

		loaded2, errs := loader.Load(fileTwoPath, false, model.LegacyValidation)
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
		loaded, errs := loader.Load(fileOnePath, false, model.LegacyValidation)
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

func sliceContains(t *testing.T, find string, in []string) bool {
	t.Helper()

	for _, s := range in {
		if find == s {
			return true
		}
	}

	return false
}
