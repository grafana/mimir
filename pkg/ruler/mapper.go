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

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
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

// FSLoader a GroupLoader implementation that reads files from a given afero.Fs.
type FSLoader struct {
	fs     afero.Fs
	parser parser.Parser
}

func NewFSLoader(fs afero.Fs) FSLoader {
	return FSLoader{
		fs:     fs,
		parser: promqlext.NewPromQLParser(),
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
	rgs, errs := rulefmt.Parse(b, ignoreUnknownFields, nameValidationScheme, f.parser)
	for i := range errs {
		errs[i] = fmt.Errorf("%s: %w", file, errs[i])
	}
	return rgs, errs
}
