// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/mapper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

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

		loader := NewFSLoader(fs, false, nil, nil)
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

		loader := NewFSLoader(fs, false, nil, nil)
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

		loader := NewFSLoader(fs, false, nil, nil)
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

func newTestCacheCounters() (prometheus.Counter, prometheus.Counter) {
	reg := prometheus.NewPedanticRegistry()
	hits := promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_rule_file_parse_cache_hits_total"})
	misses := promauto.With(reg).NewCounter(prometheus.CounterOpts{Name: "test_rule_file_parse_cache_misses_total"})
	return hits, misses
}

func Test_FSLoader_ParseCache(t *testing.T) {
	l := util_log.MakeLeveledLogger(os.Stdout, "info")

	t.Run("cache hit returns an equal, distinct result and increments hits", func(t *testing.T) {
		setupRuleSets()
		fs := afero.NewMemMapFs()
		m := &mapper{Path: "/rules", FS: fs, logger: l}
		_, files, err := m.MapRules(testUser1, initialRuleSet)
		require.NoError(t, err)

		hits, misses := newTestCacheCounters()
		loader := NewFSLoader(fs, true, hits, misses)

		first, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 1.0, testutil.ToFloat64(misses))

		second, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 1.0, testutil.ToFloat64(hits))
		require.Equal(t, 1.0, testutil.ToFloat64(misses))
		require.Equal(t, first, second)
		require.NotSame(t, first, second, "a cache hit must return a defensive copy, not the cached instance")
	})

	t.Run("cache misses again after the file content changes", func(t *testing.T) {
		setupRuleSets()
		fs := afero.NewMemMapFs()
		m := &mapper{Path: "/rules", FS: fs, logger: l}
		_, files, err := m.MapRules(testUser1, initialRuleSet)
		require.NoError(t, err)

		hits, misses := newTestCacheCounters()
		loader := NewFSLoader(fs, true, hits, misses)

		_, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 1.0, testutil.ToFloat64(misses))

		_, _, err = m.MapRules(testUser1, updatedRuleSet)
		require.NoError(t, err)

		loaded, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Len(t, loaded.Groups, 3)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 2.0, testutil.ToFloat64(misses))
	})

	t.Run("cache is scoped per file path", func(t *testing.T) {
		setupRuleSets()
		fs := afero.NewMemMapFs()
		m := &mapper{Path: "/rules", FS: fs, logger: l}
		_, files, err := m.MapRules(testUser1, twoFilesRuleSet)
		require.NoError(t, err)
		require.Len(t, files, 2)

		hits, misses := newTestCacheCounters()
		loader := NewFSLoader(fs, true, hits, misses)

		_, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 1.0, testutil.ToFloat64(misses))

		_, errs = loader.Load(files[1], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 2.0, testutil.ToFloat64(misses))

		_, errs = loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 1.0, testutil.ToFloat64(hits))
		require.Equal(t, 2.0, testutil.ToFloat64(misses))
	})

	t.Run("cache misses when parsing options differ", func(t *testing.T) {
		setupRuleSets()
		fs := afero.NewMemMapFs()
		m := &mapper{Path: "/rules", FS: fs, logger: l}
		_, files, err := m.MapRules(testUser1, initialRuleSet)
		require.NoError(t, err)

		hits, misses := newTestCacheCounters()
		loader := NewFSLoader(fs, true, hits, misses)

		_, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 1.0, testutil.ToFloat64(misses))

		_, errs = loader.Load(files[0], false, model.UTF8Validation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 2.0, testutil.ToFloat64(misses))
	})

	t.Run("cache disabled behaves like today and never hits", func(t *testing.T) {
		setupRuleSets()
		fs := afero.NewMemMapFs()
		m := &mapper{Path: "/rules", FS: fs, logger: l}
		_, files, err := m.MapRules(testUser1, initialRuleSet)
		require.NoError(t, err)

		loader := NewFSLoader(fs, false, nil, nil)
		first, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		second, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, first, second)
		require.NotSame(t, first, second)
	})

	t.Run("mutating a cache-miss result doesn't corrupt a later cache hit", func(t *testing.T) {
		// Regression test: LoadGroups keeps SourceTenants aliased by reference
		// (it isn't copied like Labels/Annotations are), and federated rule
		// evaluation later sorts that slice in place. The cache must never
		// hand out a value that shares memory with what it stores internally.
		setupRuleSets()
		fs := afero.NewMemMapFs()
		m := &mapper{Path: "/rules", FS: fs, logger: l}
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
		_, files, err := m.MapRules(testUser1, ruleConfigs)
		require.NoError(t, err)

		hits, misses := newTestCacheCounters()
		loader := NewFSLoader(fs, true, hits, misses)

		missResult, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hits))
		require.Equal(t, 1.0, testutil.ToFloat64(misses))

		// Simulate what tenant.NormalizeTenantIDs does in place during
		// federated rule evaluation on the caller's copy of SourceTenants.
		slices.Sort(missResult.Groups[0].SourceTenants)
		require.Equal(t, []string{"tenant-a", "tenant-b"}, missResult.Groups[0].SourceTenants)

		hitResult, errs := loader.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 1.0, testutil.ToFloat64(hits))
		require.Equal(t, 1.0, testutil.ToFloat64(misses))
		require.Equal(t, []string{"tenant-b", "tenant-a"}, hitResult.Groups[0].SourceTenants,
			"the cached entry must be unaffected by mutating a previously returned result")
	})

	t.Run("two loaders don't share cache state", func(t *testing.T) {
		setupRuleSets()
		fs := afero.NewMemMapFs()
		m := &mapper{Path: "/rules", FS: fs, logger: l}
		_, files, err := m.MapRules(testUser1, initialRuleSet)
		require.NoError(t, err)

		hitsA, missesA := newTestCacheCounters()
		loaderA := NewFSLoader(fs, true, hitsA, missesA)
		_, errs := loaderA.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)
		require.Equal(t, 0.0, testutil.ToFloat64(hitsA))
		require.Equal(t, 1.0, testutil.ToFloat64(missesA))

		hitsB, missesB := newTestCacheCounters()
		loaderB := NewFSLoader(fs, true, hitsB, missesB)
		_, errs = loaderB.Load(files[0], false, model.LegacyValidation)
		require.Empty(t, errs)

		require.Equal(t, 0.0, testutil.ToFloat64(hitsB))
		require.Equal(t, 1.0, testutil.ToFloat64(missesB))
	})
}

// BenchmarkFSLoader_Load simulates the steady-state case the parse cache
// targets: a tenant's namespace file that hasn't changed being reloaded on
// every rule sync. cache_enabled=false takes the same code path as before
// this cache existed (every Load call re-runs rulefmt.Parse); cache_enabled=true
// exercises the new cache-hit path.
func BenchmarkFSLoader_Load(b *testing.B) {
	l := util_log.MakeLeveledLogger(io.Discard, "info")
	fs := afero.NewMemMapFs()
	m := &mapper{Path: "/rules", FS: fs, logger: l}

	const numGroups = 20
	const rulesPerGroup = 5
	groups := make([]rulefmt.RuleGroup, numGroups)
	for i := range groups {
		rules := make([]rulefmt.Rule, rulesPerGroup)
		for j := range rules {
			rules[j] = rulefmt.Rule{
				Record: fmt.Sprintf("rule_%d_%d", i, j),
				Expr:   fmt.Sprintf("sum(rate(some_metric_%d_%d[5m]))", i, j),
				Labels: map[string]string{"team": "observability"},
			}
		}
		groups[i] = rulefmt.RuleGroup{Name: fmt.Sprintf("group_%d", i), Rules: rules}
	}

	_, files, err := m.MapRules("bench_user", map[string][]rulefmt.RuleGroup{"namespace": groups})
	if err != nil {
		b.Fatal(err)
	}

	for _, cacheEnabled := range []bool{false, true} {
		b.Run(fmt.Sprintf("cache_enabled=%v", cacheEnabled), func(b *testing.B) {
			var hits, misses prometheus.Counter
			if cacheEnabled {
				hits, misses = newTestCacheCounters()
			}
			loader := NewFSLoader(fs, cacheEnabled, hits, misses)
			// Warm up so the steady-state (already-parsed-once) case is measured.
			if _, errs := loader.Load(files[0], false, model.LegacyValidation); len(errs) > 0 {
				b.Fatal(errs)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, errs := loader.Load(files[0], false, model.LegacyValidation); len(errs) > 0 {
					b.Fatal(errs)
				}
			}
		})
	}
}

func Test_FSLoader_ParseCache_EvictsRemovedNamespaces(t *testing.T) {
	// Regression test for unbounded growth: a removed or renamed namespace's
	// cache entry must not live forever. It should age out within two more
	// passes of the tenant's remaining (stable) namespace, via generation
	// rotation, not accumulate for as long as the FSLoader exists.
	l := util_log.MakeLeveledLogger(os.Stdout, "info")
	setupRuleSets()
	fs := afero.NewMemMapFs()
	m := &mapper{Path: "/rules", FS: fs, logger: l}

	hits, misses := newTestCacheCounters()
	loader := NewFSLoader(fs, true, hits, misses)

	loadAll := func(files []string) {
		for _, f := range files {
			_, errs := loader.Load(f, false, model.LegacyValidation)
			require.Empty(t, errs)
		}
	}

	// Pass 1: two namespaces.
	_, files, err := m.MapRules(testUser1, twoFilesRuleSet)
	require.NoError(t, err)
	require.Len(t, files, 2)
	loadAll(files)
	require.Len(t, loader.cur, 2)
	require.Empty(t, loader.prev)

	// Pass 2: same two namespaces, unchanged -- triggers the first rotation.
	loadAll(files)
	require.Len(t, loader.cur, 2)
	require.Len(t, loader.prev, 2)

	// Pass 3: one namespace is removed. The survivor's rotation moves the
	// removed namespace's stale entry into prev, where it lingers once more.
	_, remainingFiles, err := m.MapRules(testUser1, twoFilesDeletedRuleSet)
	require.NoError(t, err)
	require.Len(t, remainingFiles, 1)
	loadAll(remainingFiles)
	require.Len(t, loader.cur, 1)
	require.Len(t, loader.prev, 2, "the removed namespace's entry should still be in prev for one more pass")

	// Pass 4: the next rotation overwrites prev, dropping the removed
	// namespace's entry for good.
	loadAll(remainingFiles)
	require.Len(t, loader.cur, 1)
	require.Len(t, loader.prev, 1, "the removed namespace's entry must be gone after a second rotation")
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
