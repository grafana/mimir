// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/mapper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/namevalidationutil"
	"github.com/spf13/afero"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

type ruleRegistry struct {
	prefix string
	rules  map[string]map[string]rulespb.RuleGroupList // tenant -> rule file name -> groups
	mtx    sync.RWMutex
	logger log.Logger
}

func newRuleRegistry(prefix string, logger log.Logger) *ruleRegistry {
	return &ruleRegistry{
		prefix: prefix,
		rules:  map[string]map[string]rulespb.RuleGroupList{},
		mtx:    sync.RWMutex{},
		logger: logger,
	}
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
		if len(r.rules[userID]) > 0 {
			result = append(result, userID)
		}
	}
	return result, nil
}

func (r *ruleRegistry) MapRules(user string, ruleConfigs map[string]rulespb.RuleGroupList) (bool, []string, error) {
	logger := log.With(r.logger, "user", user)
	r.mtx.Lock()
	defer r.mtx.Unlock()

	// getLocalRules gets the namespace from the file name of each group.
	// So the registry uses the same file names as the mapper.
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

		// Equal compares in order.
		// The sort makes a new order of the same groups count as no change.
		slices.SortFunc(groups, func(a, b *rulespb.RuleGroupDesc) int {
			return strings.Compare(b.Name, a.Name)
		})

		if !groups.Equal(r.rules[user][fullFileName]) {
			level.Info(logger).Log("msg", "updating rule namespace", "file", fullFileName)
			cloneLabelStrings(groups)
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

// cloneLabelStrings copies the label and annotation strings of groups.
// Unmarshal makes these strings point into the buffer that it reads the groups from.
// The registry keeps the groups.
// Without a copy, it also keeps each buffer.
// The groups belong to this sync.
// So the copy can replace the strings in place.
func cloneLabelStrings(groups rulespb.RuleGroupList) {
	clone := func(ls []mimirpb.LabelAdapter) {
		for i := range ls {
			ls[i].Name, ls[i].Value = strings.Clone(ls[i].Name), strings.Clone(ls[i].Value)
		}
	}
	for _, g := range groups {
		clone(g.Labels)
		for _, r := range g.Rules {
			clone(r.Labels)
			clone(r.Annotations)
		}
	}
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
		fmtGroup := rulespb.FromProto(group)
		// tenant.NormalizeTenantIDs sorts and deduplicates SourceTenants in place.
		// The registry keeps its protos, so each load needs its own copy.
		fmtGroup.SourceTenants = slices.Clone(fmtGroup.SourceTenants)
		fmtGroups = append(fmtGroups, fmtGroup)
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

	rgs := rulefmt.RuleGroups{Groups: cleanRuleGroupExprs(groups)}

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
	parser   parser.Parser
}

func newRegistryLoader(registry *ruleRegistry, user string) registryLoader {
	return registryLoader{
		registry: registry,
		user:     user,
		parser:   promqlext.NewPromQLParser(),
	}
}

func (r registryLoader) Load(identifier string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	rgs := r.registry.get(r.user, identifier)
	if rgs == nil {
		return nil, []error{fmt.Errorf("%s: rule file not found in registry", identifier)}
	}
	if errs := validateRuleGroups(identifier, rgs, nameValidationScheme, r.parser); len(errs) > 0 {
		return nil, errs
	}
	return rgs, nil
}

// validateRuleGroups copies the checks of rulefmt.RuleGroups.Validate.
// That method needs the YAML nodes of a parsed file, and the registry has none.
// Compare it with vendor/github.com/prometheus/prometheus/model/rulefmt/rulefmt.go after each Prometheus update.
func validateRuleGroups(identifier string, rgs *rulefmt.RuleGroups, scheme model.ValidationScheme, p parser.Parser) []error {
	if err := namevalidationutil.CheckNameValidationScheme(scheme); err != nil {
		return []error{fmt.Errorf("%s: %w", identifier, err)}
	}

	var errs []error
	names := map[string]struct{}{}
	for _, g := range rgs.Groups {
		if g.Name == "" {
			errs = append(errs, fmt.Errorf("%s: Groupname must not be empty", identifier))
		}
		if _, ok := names[g.Name]; ok {
			errs = append(errs, fmt.Errorf("%s: groupname: \"%s\" is repeated in the same file", identifier, g.Name))
		}
		names[g.Name] = struct{}{}

		for k, v := range g.Labels {
			if !scheme.IsValidLabelName(k) || k == model.MetricNameLabel {
				errs = append(errs, fmt.Errorf("%s: invalid label name: %s", identifier, k))
			}
			if !model.LabelValue(v).IsValid() {
				errs = append(errs, fmt.Errorf("%s: invalid label value: %s", identifier, v))
			}
		}

		for i, r := range g.Rules {
			ruleName := r.Alert
			if ruleName == "" {
				ruleName = r.Record
			}
			for _, err := range r.Validate(rulefmt.RuleNode{}, scheme, p) {
				// The empty node gives some errors the position 0:0.
				// Unwrap drops that position.
				errs = append(errs, fmt.Errorf("%s: group %q, rule %d, %q: %w", identifier, g.Name, i+1, ruleName, errors.Unwrap(&err)))
			}
		}
	}
	return errs
}

func (r registryLoader) Parse(query string) (parser.Expr, error) { return r.parser.ParseExpr(query) }

// FSLoader a GroupLoader implementation that reads files from a given afero.Fs.
type FSLoader struct {
	fs     afero.Fs
	parser parser.Parser
	logger *slog.Logger
}

func NewFSLoader(fs afero.Fs) FSLoader {
	return FSLoader{
		fs:     fs,
		parser: promqlext.NewPromQLParser(),
		logger: promslog.NewNopLogger(),
	}
}

func (f FSLoader) Load(identifier string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	return f.parseFile(f.fs, identifier, ignoreUnknownFields, nameValidationScheme)
}

func (f FSLoader) Parse(query string) (parser.Expr, error) {
	return f.parser.ParseExpr(query)
}

// parseFile reads and parses rules from a file.
// Duplicate of Prometheus' rulefmt.ParseFile, but injects the FS.
func (f FSLoader) parseFile(fs afero.Fs, file string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	b, err := afero.ReadFile(fs, file)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: %w", file, err)}
	}
	rgs, errs := rulefmt.Parse(b, ignoreUnknownFields, nameValidationScheme, f.parser, f.logger)
	for i := range errs {
		errs[i] = fmt.Errorf("%s: %w", file, errs[i])
	}
	return rgs, errs
}

// cleanRuleGroupExprs returns a copy of groups with leading/trailing whitespace
// trimmed from rule expressions. This avoids yaml.v3 emitting explicit
// indentation indicators (e.g. "|4") for expressions that start with newlines
// or whitespace, which can cause parsing failures when the file is read back.
func cleanRuleGroupExprs(groups []rulefmt.RuleGroup) []rulefmt.RuleGroup {
	cleaned := make([]rulefmt.RuleGroup, len(groups))
	for i, g := range groups {
		cleaned[i] = g
		cleaned[i].Rules = make([]rulefmt.Rule, len(g.Rules))
		for j, r := range g.Rules {
			cleaned[i].Rules[j] = r
			cleaned[i].Rules[j].Expr = strings.TrimSpace(r.Expr)
		}
	}
	return cleaned
}
