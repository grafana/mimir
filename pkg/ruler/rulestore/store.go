// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/store.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package rulestore

import (
	"context"
	"errors"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
)

var (
	// ErrGroupNotFound is returned if a rule group does not exist
	ErrGroupNotFound = errors.New("group does not exist")
	// ErrGroupNamespaceNotFound is returned if a namespace does not exist
	ErrGroupNamespaceNotFound = errors.New("group namespace does not exist")
	// ErrUserNotFound is returned if the user does not currently exist
	ErrUserNotFound = errors.New("no rule groups found for user")
)

// Options are per-call options that can be used to modify the behavior of RuleStore methods.
type Options struct {
	DisableCache bool
}

// CollectOptions applies one or more Option callbacks to produce an Options struct.
func CollectOptions(opts ...Option) *Options {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}

	return o
}

// Option is a callback the modifies per-call options for RuleStore methods.
type Option func(opts *Options)

// WithCacheDisabled returns an Option callback to disable any caching used
// by a RuleStore method call.
func WithCacheDisabled() Option {
	return func(opts *Options) {
		opts.DisableCache = true
	}
}

// RuleStore is used to store and retrieve rules.
// Methods starting with "List" prefix may return partially loaded groups: with only group Name, Namespace and User fields set.
// To make sure that rules within each group are loaded, client must use LoadRuleGroups method.
type RuleStore interface {
	// ListAllUsers returns all users with rule groups configured.
	ListAllUsers(ctx context.Context, opts ...Option) ([]string, error)

	// ListRuleGroupsForUserAndNamespace returns all the active rule groups for a user from given namespace.
	// It *MUST* populate fields User, Namespace, Name of all rule groups.
	// It *MAY* populate the actual rules.
	// If namespace is empty, groups from all namespaces are returned.
	ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string, opts ...Option) (rulespb.RuleGroupList, error)

	// LoadRuleGroups loads rules for each rule group in the map.
	//
	// Requirements:
	// - The groupsToLoad parameter  *MUST* be coming from one of the List methods.
	//   The groupsToLoad map can be filtered for sharding purposes before calling
	//   LoadRuleGroups.
	//
	// Specifications:
	// - LoadRuleGroups() *MUST* populate the rules if the List methods have
	//   not populated the rule groups with their actual rules.
	// - If, and only if, a rule group can't be loaded because missing in the storage
	//   then LoadRuleGroups() *MUST* not return error but return the missing rule groups.
	//   This means that missing list *MUST* contain only rule groups that don't exist
	//   in the storage, and not that we failed loading for other reasons.
	LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rulespb.RuleGroupList) (missing rulespb.RuleGroupList, err error)

	GetRuleGroup(ctx context.Context, userID, namespace, group string) (*rulespb.RuleGroupDesc, error)
	SetRuleGroup(ctx context.Context, userID, namespace string, group *rulespb.RuleGroupDesc) error

	// DeleteRuleGroup deletes single rule group.
	DeleteRuleGroup(ctx context.Context, userID, namespace string, group string) error

	// DeleteNamespace lists rule groups for given user and namespace, and deletes all rule groups.
	// If namespace is empty, deletes all rule groups for user.
	DeleteNamespace(ctx context.Context, userID, namespace string) error
}
