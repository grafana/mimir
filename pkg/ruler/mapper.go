// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/mapper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"bytes"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type ruleRegistry struct {
	prefix string
	rules  map[string]map[string]rulespb.RuleGroupList // tenant -> namespace -> groups
	mtx    sync.RWMutex
	logger log.Logger
}

func (r *ruleRegistry) cleanupUser(userID string) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	delete(r.rules, userID)
}

func (r *ruleRegistry) cleanup() {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	clear(r.rules)
}

func (r *ruleRegistry) users() ([]string, error) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	result := make([]string, 0, len(r.rules))
	for userID := range r.rules {
		result = append(result, userID)
	}
	return result, nil
}

func (r *ruleRegistry) MapRules(user string, ruleConfigs map[string]rulespb.RuleGroupList) (bool, []string, error) {
	logger := log.With(r.logger, "user", user)
	r.mtx.Lock()
	defer r.mtx.Unlock()

	path := filepath.Join(r.prefix, user)
	if _, ok := r.rules[user]; !ok {
		r.rules[user] = make(map[string]rulespb.RuleGroupList)
	}

	anyUpdated := false
	var filenames []string

	for filename, groups := range ruleConfigs {
		encodedFileName := url.PathEscape(filename)
		fullFileName := filepath.Join(path, encodedFileName)
		filenames = append(filenames, fullFileName)

		slices.SortFunc(groups, func(a, b *rulespb.RuleGroupDesc) int {
			return strings.Compare(b.Name, a.Name)
		})

		if !groups.Equal(r.rules[user][fullFileName]) {
			anyUpdated = true
			r.rules[user][fullFileName] = groups
		}
	}

	// and clean up any that shouldn't exist TODO
	existingFiles := make([]string, 0, len(r.rules[user]))
	for key := range r.rules[user] {
		existingFiles = append(existingFiles, key)
	}

	for _, existingFile := range existingFiles {
		name := filepath.Base(existingFile)
		decodedNamespace, err := url.PathUnescape(name)
		if err != nil {
			level.Warn(logger).Log("msg", "unable to remove rule file from registry", "file", existingFile, "err", err)
			continue
		}

		ruleGroups := ruleConfigs[decodedNamespace]

		if ruleGroups == nil {
			delete(r.rules[user], existingFile)
			anyUpdated = true
		}
	}

	return anyUpdated, filenames, nil
}

func (r *ruleRegistry) get(user string, identifier string) *rulefmt.RuleGroups {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	userRegistry := r.rules[user]
	if userRegistry == nil {
		return nil
	}
	groups := userRegistry[identifier]
	if groups == nil {
		return nil
	}

	fmtGroups := make([]rulefmt.RuleGroup, 0, len(groups))
	for _, group := range groups {
		fmtGroups = append(fmtGroups, rulespb.FromProto(group))
	}

	return &rulefmt.RuleGroups{Groups: fmtGroups}
}

// mapper is designed to enusre the provided rule sets are identical
// to the on-disk rules tracked by the prometheus manager
type mapper struct {
	Path string // Path specifies the directory in which rule files will be mapped.

	FS     afero.Fs
	logger log.Logger
}

func newMapper(path string, FS afero.Fs, logger log.Logger) *mapper {
	m := &mapper{
		Path:   path,
		FS:     FS,
		logger: logger,
	}
	m.cleanup()

	return m
}

func (m *mapper) cleanupUser(userID string) {
	dirPath := filepath.Join(m.Path, userID)
	err := m.FS.RemoveAll(dirPath)
	if err != nil {
		level.Warn(m.logger).Log("msg", "unable to remove user directory", "path", dirPath, "user", userID, "err", err)
	}
}

// cleanup removes all of the user directories in the path of the mapper
func (m *mapper) cleanup() {
	level.Info(m.logger).Log("msg", "cleaning up mapped rules directory", "path", m.Path)

	users, err := m.users()
	if err != nil {
		level.Error(m.logger).Log("msg", "unable to read rules directory", "path", m.Path, "err", err)
		return
	}

	for _, u := range users {
		m.cleanupUser(u)
	}
}

func (m *mapper) users() ([]string, error) {
	var result []string

	dirs, err := afero.ReadDir(m.FS, m.Path)
	if os.IsNotExist(err) {
		// The directory may have not been created yet. With regards to this function
		// it's like the ruler has no tenants and it shouldn't be considered an error.
		return nil, nil
	}

	for _, u := range dirs {
		if u.IsDir() {
			result = append(result, u.Name())
		}
	}

	return result, err
}

func (m *mapper) MapRules(user string, ruleConfigs map[string][]rulefmt.RuleGroup) (bool, []string, error) {
	logger := log.With(m.logger, "user", user)

	// user rule files will be stored as `/<path>/<userid>/<encoded filename>`
	path := filepath.Join(m.Path, user)
	err := m.FS.MkdirAll(path, 0777)
	if err != nil {
		return false, nil, err
	}

	anyUpdated := false
	var filenames []string

	// write all rule configs to disk
	for filename, groups := range ruleConfigs {
		// Store the encoded file name to better handle `/` characters
		encodedFileName := url.PathEscape(filename)
		fullFileName := filepath.Join(path, encodedFileName)

		fileUpdated, err := m.writeRuleGroupsIfNewer(groups, fullFileName, logger)
		if err != nil {
			return false, nil, err
		}
		filenames = append(filenames, fullFileName)
		anyUpdated = anyUpdated || fileUpdated
	}

	// and clean any up that shouldn't exist
	existingFiles, err := afero.ReadDir(m.FS, path)
	if err != nil {
		return false, nil, err
	}

	for _, existingFile := range existingFiles {
		fullFileName := filepath.Join(path, existingFile.Name())

		// Ensure the namespace is decoded from a url path encoding to see if it is still required
		decodedNamespace, err := url.PathUnescape(existingFile.Name())
		if err != nil {
			level.Warn(logger).Log("msg", "unable to remove rule file on disk", "file", fullFileName, "err", err)
			continue
		}

		ruleGroups := ruleConfigs[decodedNamespace]

		if ruleGroups == nil {
			err = m.FS.Remove(fullFileName)
			if err != nil {
				level.Warn(logger).Log("msg", "unable to remove rule file on disk", "file", fullFileName, "err", err)
			}
			anyUpdated = true
		}
	}

	return anyUpdated, filenames, nil
}

func (m *mapper) writeRuleGroupsIfNewer(groups []rulefmt.RuleGroup, filename string, logger log.Logger /* contextual logger with userID */) (bool, error) {
	slices.SortFunc(groups, func(a, b rulefmt.RuleGroup) int {
		return strings.Compare(b.Name, a.Name)
	})

	rgs := rulefmt.RuleGroups{Groups: groups}

	d, err := yaml.Marshal(&rgs)
	if err != nil {
		return false, err
	}

	_, err = m.FS.Stat(filename)
	if err == nil {
		current, err := afero.ReadFile(m.FS, filename)
		if err != nil {
			return false, err
		}

		// bailout if there is no update
		if bytes.Equal(current, d) {
			return false, nil
		}
	}

	level.Info(logger).Log("msg", "updating rule file", "file", filename)
	err = afero.WriteFile(m.FS, filename, d, 0777)
	if err != nil {
		return false, err
	}

	return true, nil
}

// registryLoader is a GroupLoader implementation that reads groups from a ruleRegistry.
type registryLoader struct {
	registry *ruleRegistry
	user     string
}

func newRegsitryLoader(registry *ruleRegistry, user string) registryLoader {
	return registryLoader{
		registry: registry,
		user:     user,
	}
}

func (r registryLoader) Load(identifier string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	rgs := r.registry.get(r.user, identifier)
	if rgs == nil {
		return nil, []error{fmt.Errorf("%s: rule group not found in registry", identifier)}
	}
	return rgs, nil
}

func (registryLoader) Parse(query string) (parser.Expr, error) { return parser.ParseExpr(query) }

// FSLoader a GroupLoader implementation that reads files from a given afero.Fs.
type FSLoader struct {
	fs afero.Fs
}

func NewFSLoader(fs afero.Fs) FSLoader {
	return FSLoader{
		fs: fs,
	}
}

func (f FSLoader) Load(identifier string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	return parseFile(f.fs, identifier, ignoreUnknownFields, nameValidationScheme)
}

func (FSLoader) Parse(query string) (parser.Expr, error) { return parser.ParseExpr(query) }

// parseFile reads and parses rules from a file.
// Duplicate of Prometheus' rulefmt.ParseFile, but injects the FS.
func parseFile(fs afero.Fs, file string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	b, err := afero.ReadFile(fs, file)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: %w", file, err)}
	}
	rgs, errs := rulefmt.Parse(b, ignoreUnknownFields, nameValidationScheme)
	for i := range errs {
		errs[i] = fmt.Errorf("%s: %w", file, errs[i])
	}
	return rgs, errs
}
