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
