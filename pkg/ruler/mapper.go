// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/mapper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"bytes"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/spf13/afero"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

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

// FSLoader a GroupLoader implementation that reads files from a given afero.Fs.
//
// If cacheEnabled is set, it caches the parsed result of each file.
// The cache key is the file's exact byte content plus the parsing options passed to Load.
// A file that hasn't changed since the last Load call skips rulefmt.Parse entirely.
//
// The cache holds two generations, cur and prev, to bound memory.
// This avoids needing an external signal for when a file is removed or renamed.
// rules.Manager.LoadGroups calls Load at most once per path per Update pass.
// manager.Update holds its own lock. Passes for one tenant never interleave.
// A path already present in cur means a new pass has started.
// Load then rotates the generations: prev = cur, cur = a new empty map.
//
// A hit in prev gets promoted into cur.
// This keeps a stable file triggering rotation on every later pass.
// Without promotion, a stable file would fall out of the cache after a single pass.
//
// A path that stops being loaded ages out within two passes.
// Its entry moves into prev on the next rotation.
// It is dropped for good when prev is next overwritten.
// This bounds the cache at roughly 2x the live file count, regardless of how many distinct paths a tenant has ever used.
//
// Known gap: this needs at least one stable path across two consecutive passes to trigger a rotation.
// A tenant whose entire namespace set changes to new paths on every single pass never triggers one.
// That is considered acceptable. It requires zero stable namespaces ever, not just occasional renames.
type FSLoader struct {
	fs     afero.Fs
	parser parser.Parser
	logger *slog.Logger

	cacheEnabled bool
	cacheHits    prometheus.Counter
	cacheMisses  prometheus.Counter

	mu        sync.Mutex
	cur, prev map[string]cachedRuleGroups
}

type cachedRuleGroups struct {
	rawBytes             []byte
	ignoreUnknownFields  bool
	nameValidationScheme model.ValidationScheme
	parsed               *rulefmt.RuleGroups
}

// NewFSLoader returns a GroupLoader that reads rule files from fs. When
// cacheEnabled is true, cacheHits and cacheMisses must be non-nil and are
// incremented on every Load call.
func NewFSLoader(fs afero.Fs, cacheEnabled bool, cacheHits, cacheMisses prometheus.Counter) *FSLoader {
	loader := &FSLoader{
		fs:           fs,
		parser:       promqlext.NewPromQLParser(),
		logger:       promslog.NewNopLogger(),
		cacheEnabled: cacheEnabled,
		cacheHits:    cacheHits,
		cacheMisses:  cacheMisses,
	}
	if cacheEnabled {
		loader.cur = make(map[string]cachedRuleGroups)
	}
	return loader
}

func (f *FSLoader) Load(identifier string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	return f.parseFile(f.fs, identifier, ignoreUnknownFields, nameValidationScheme)
}

func (f *FSLoader) Parse(query string) (parser.Expr, error) {
	return f.parser.ParseExpr(query)
}

// parseFile reads and parses rules from a file.
// Duplicate of Prometheus' rulefmt.ParseFile, but injects the FS.
func (f *FSLoader) parseFile(fs afero.Fs, file string, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, []error) {
	b, err := afero.ReadFile(fs, file)
	if err != nil {
		return nil, []error{fmt.Errorf("%s: %w", file, err)}
	}

	if f.cacheEnabled {
		if rgs, ok := f.lookupCache(file, b, ignoreUnknownFields, nameValidationScheme); ok {
			f.cacheHits.Inc()
			return rgs, nil
		}
		f.cacheMisses.Inc()
	}

	rgs, errs := rulefmt.Parse(b, ignoreUnknownFields, nameValidationScheme, f.parser, f.logger)
	for i := range errs {
		errs[i] = fmt.Errorf("%s: %w", file, errs[i])
	}
	if len(errs) == 0 && f.cacheEnabled {
		// Cache a copy, not rgs itself: its SourceTenants field is mutated in place by the caller.
		f.storeCache(file, b, ignoreUnknownFields, nameValidationScheme, copyRuleGroups(rgs))
	}
	return rgs, errs
}

func (f *FSLoader) lookupCache(path string, rawBytes []byte, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme) (*rulefmt.RuleGroups, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.cur[path]; ok {
		f.prev = f.cur
		f.cur = make(map[string]cachedRuleGroups)
	}

	entry, ok := f.prev[path]
	if !ok ||
		entry.ignoreUnknownFields != ignoreUnknownFields ||
		entry.nameValidationScheme != nameValidationScheme ||
		!bytes.Equal(entry.rawBytes, rawBytes) {
		return nil, false
	}

	f.cur[path] = entry
	return copyRuleGroups(entry.parsed), true
}

func (f *FSLoader) storeCache(path string, rawBytes []byte, ignoreUnknownFields bool, nameValidationScheme model.ValidationScheme, rgs *rulefmt.RuleGroups) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cur[path] = cachedRuleGroups{
		rawBytes:             rawBytes,
		ignoreUnknownFields:  ignoreUnknownFields,
		nameValidationScheme: nameValidationScheme,
		parsed:               rgs,
	}
}

// copyRuleGroups returns a deep copy of rgs.
func copyRuleGroups(rgs *rulefmt.RuleGroups) *rulefmt.RuleGroups {
	if rgs == nil {
		return nil
	}
	out := &rulefmt.RuleGroups{
		Groups: make([]rulefmt.RuleGroup, len(rgs.Groups)),
	}
	for i, g := range rgs.Groups {
		g.SourceTenants = slices.Clone(g.SourceTenants)
		g.Rules = make([]rulefmt.Rule, len(rgs.Groups[i].Rules))
		for j, r := range rgs.Groups[i].Rules {
			r.Labels = maps.Clone(r.Labels)
			r.Annotations = maps.Clone(r.Annotations)
			g.Rules[j] = r
		}
		out.Groups[i] = g
	}
	return out
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
