// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/local/local.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package local

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/rulefmt"
	promRules "github.com/prometheus/prometheus/rules"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
)

// Client expects to load already existing rules located at:
//
//	cfg.Directory / userID / namespace
type Client struct {
	cfg    rulestore.LocalStoreConfig
	loader promRules.GroupLoader
}

func NewLocalRulesClient(cfg rulestore.LocalStoreConfig, loader promRules.GroupLoader) (*Client, error) {
	if cfg.Directory == "" {
		return nil, errors.New("directory required for local rules config")
	}

	return &Client{
		cfg:    cfg,
		loader: loader,
	}, nil
}

func (l *Client) ListAllUsers(_ context.Context, _ ...rulestore.Option) ([]string, error) {
	root := l.cfg.Directory
	infos, err := os.ReadDir(root)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read dir %s", root)
	}

	var result []string
	for _, info := range infos {
		// After resolving link, info.Name() may be different than user, so keep original name.
		user := info.Name()
		isDir := info.IsDir()

		if info.Type()&os.ModeSymlink != 0 {
			// os.ReadDir only returns result of LStat. Calling Stat resolves symlink.
			finfo, err := os.Stat(filepath.Join(root, info.Name()))
			if err != nil {
				return nil, err
			}

			isDir = finfo.IsDir()
		}

		if isDir {
			result = append(result, user)
		}
	}

	return result, nil
}

// ListRuleGroupsForUserAndNamespace implements rules.RuleStore. This method also loads the rules.
func (l *Client) ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string, _ ...rulestore.Option) (rulespb.RuleGroupList, error) {
	if namespace == "" {
		return l.loadAllRulesGroupsForUser(ctx, userID)
	}

	rulegroups, err := l.loadRawRulesGroupsForUserAndNamespace(ctx, userID, namespace)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load rule group for user %s and namespace %s", userID, namespace)
	}

	var list rulespb.RuleGroupList
	for _, rg := range rulegroups.Groups {
		desc := rulespb.ToProto(userID, namespace, rg)
		list = append(list, desc)
	}
	return list, nil
}

func (l *Client) LoadRuleGroups(_ context.Context, _ map[string]rulespb.RuleGroupList) (rulespb.RuleGroupList, error) {
	// This Client already loads the rules in its List methods, there is nothing left to do here.
	return nil, nil
}

// GetRuleGroup implements RuleStore
func (l *Client) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (*rulespb.RuleGroupDesc, error) {
	if namespace == "" {
		return nil, errors.New("empty namespace")
	}

	rulegroups, err := l.loadRawRulesGroupsForUserAndNamespace(ctx, userID, namespace)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load rule group for user %s and namespace %s", userID, namespace)
	}

	for _, rg := range rulegroups.Groups {
		if rg.Name == group {
			return rulespb.ToProto(userID, namespace, rg), nil
		}
	}
	return nil, rulestore.ErrGroupNotFound
}

// SetRuleGroup implements RuleStore
func (l *Client) SetRuleGroup(_ context.Context, _, _ string, _ *rulespb.RuleGroupDesc) error {
	return errors.New("SetRuleGroup unsupported in rule local store")
}

// DeleteRuleGroup implements RuleStore
func (l *Client) DeleteRuleGroup(_ context.Context, _, _, _ string) error {
	return errors.New("DeleteRuleGroup unsupported in rule local store")
}

// DeleteNamespace implements RulerStore
func (l *Client) DeleteNamespace(_ context.Context, _, _ string) error {
	return errors.New("DeleteNamespace unsupported in rule local store")
}

func (l *Client) loadAllRulesGroupsForUser(ctx context.Context, userID string) (rulespb.RuleGroupList, error) {
	var list rulespb.RuleGroupList

	root := filepath.Join(l.cfg.Directory, userID)
	infos, err := os.ReadDir(root)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read rule dir %s", root)
	}

	for _, info := range infos {
		// After resolving link, info.Name() may be different than namespace, so keep original name.
		namespace := info.Name()
		isDir := info.IsDir()

		if info.Type()&os.ModeSymlink != 0 {
			// os.ReadDir only returns result of LStat. Calling Stat resolves symlink.
			path := filepath.Join(root, info.Name())
			finfo, err := os.Stat(path)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to stat rule file %s", path)
			}

			isDir = finfo.IsDir()
		}

		if isDir {
			continue
		}

		rulegroups, err := l.loadRawRulesGroupsForUserAndNamespace(ctx, userID, namespace)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load rule group for user %s and namespace %s", userID, namespace)
		}
		for _, rg := range rulegroups.Groups {
			desc := rulespb.ToProto(userID, namespace, rg)
			list = append(list, desc)
		}
	}

	return list, nil
}

func (l *Client) loadRawRulesGroupsForUserAndNamespace(_ context.Context, userID string, namespace string) (*rulefmt.RuleGroups, error) {
	filename := filepath.Join(l.cfg.Directory, userID, namespace)

	rulegroups, errs := l.loader.Load(filename)
	if len(errs) > 0 {
		return nil, errors.Wrapf(errs[0], "error parsing %s", filename)
	}
	return rulegroups, nil
}
