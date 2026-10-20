// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"github.com/go-kit/log"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/spf13/afero"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

// RuleFileStore holds the rule groups of each user for the Prometheus rules managers.
// A rules manager loads the groups by the file names that mapRules returns.
type RuleFileStore interface {
	// mapRules stores the rule groups of a user.
	// It returns true if the groups changed, and the file names to load.
	mapRules(user string, groups rulespb.RuleGroupList) (bool, []string, error)
	groupLoader(user string) promRules.GroupLoader
	cleanupUser(user string)
	cleanup()
}

// NewRuleFileStore returns the RuleFileStore for the ruler config.
func NewRuleFileStore(cfg Config, logger log.Logger) RuleFileStore {
	return newFSRuleFileStore(cfg.RulePath, afero.NewMemMapFs(), logger)
}

// fsRuleFileStore writes the rule groups as YAML files.
// The rules managers then parse the files again.
type fsRuleFileStore struct {
	mapper *mapper
	fs     afero.Fs
}

func newFSRuleFileStore(path string, fs afero.Fs, logger log.Logger) *fsRuleFileStore {
	return &fsRuleFileStore{mapper: newMapper(path, fs, logger), fs: fs}
}

func (s *fsRuleFileStore) mapRules(user string, groups rulespb.RuleGroupList) (bool, []string, error) {
	return s.mapper.MapRules(user, groups.Formatted())
}

func (s *fsRuleFileStore) groupLoader(string) promRules.GroupLoader {
	return NewFSLoader(s.fs)
}

func (s *fsRuleFileStore) cleanupUser(user string) {
	s.mapper.cleanupUser(user)
}

func (s *fsRuleFileStore) cleanup() {
	s.mapper.cleanup()
}
