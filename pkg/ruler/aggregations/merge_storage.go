package aggregations

import (
	"context"
	"sort"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/util"
)

type MergeRuleStores struct {
	aggregations *AggregationsRuleStore
	rules        rulestore.RuleStore
}

func NewMergeRuleStores(aggr *AggregationsRuleStore, store rulestore.RuleStore) rulestore.RuleStore {
	return MergeRuleStores{
		aggregations: aggr,
		rules:        store,
	}
}

func (m MergeRuleStores) ListAllUsers(ctx context.Context) ([]string, error) {
	us1, err := m.aggregations.ListAllUsers(ctx)
	if err != nil {
		return nil, err
	}
	us2, err := m.rules.ListAllUsers(ctx)
	if err != nil {
		return nil, err
	}

	sort.Strings(us1)
	sort.Strings(us2)

	return util.MergeSlices(us1, us2), nil
}

func (m MergeRuleStores) ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string) (rulespb.RuleGroupList, error) {
	var l1, l2 rulespb.RuleGroupList
	var err error

	if namespace == "" || namespace == aggregationsNamespace {
		l1, err = m.aggregations.ListRuleGroupsForUserAndNamespace(ctx, userID, namespace)
		if err != nil {
			return nil, err
		}
	}
	if namespace != aggregationsNamespace { // also covers namespace == ""
		l2, err = m.rules.ListRuleGroupsForUserAndNamespace(ctx, userID, namespace)
		if err != nil {
			return nil, err
		}
	}

	return append(l1, l2...), nil
}

func (m MergeRuleStores) LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rulespb.RuleGroupList) error {
	aggrGroups := map[string]rulespb.RuleGroupList{}
	ruleGroups := map[string]rulespb.RuleGroupList{}

	for tenant, groups := range groupsToLoad {
		for _, g := range groups {
			if g.Namespace == aggregationsNamespace {
				aggrGroups[tenant] = append(aggrGroups[tenant], g)
			} else {
				ruleGroups[tenant] = append(ruleGroups[tenant], g)
			}
		}
	}

	if len(aggrGroups) > 0 {
		err := m.aggregations.LoadRuleGroups(ctx, aggrGroups)
		if err != nil {
			return err
		}
	}
	if len(ruleGroups) > 0 {
		err := m.rules.LoadRuleGroups(ctx, ruleGroups)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m MergeRuleStores) GetRuleGroup(ctx context.Context, userID, namespace, group string) (*rulespb.RuleGroupDesc, error) {
	if namespace == aggregationsNamespace {
		return m.aggregations.GetRuleGroup(ctx, userID, namespace, group)
	} else {
		return m.rules.GetRuleGroup(ctx, userID, namespace, group)
	}
}

func (m MergeRuleStores) SetRuleGroup(ctx context.Context, userID, namespace string, group *rulespb.RuleGroupDesc) error {
	if namespace == aggregationsNamespace {
		return m.aggregations.SetRuleGroup(ctx, userID, namespace, group)
	} else {
		return m.rules.SetRuleGroup(ctx, userID, namespace, group)
	}
}

func (m MergeRuleStores) DeleteRuleGroup(ctx context.Context, userID, namespace string, group string) error {
	if namespace == aggregationsNamespace {
		return m.aggregations.DeleteRuleGroup(ctx, userID, namespace, group)
	} else {
		return m.rules.DeleteRuleGroup(ctx, userID, namespace, group)
	}
}

func (m MergeRuleStores) DeleteNamespace(ctx context.Context, userID, namespace string) error {
	if namespace == aggregationsNamespace {
		return m.aggregations.DeleteNamespace(ctx, userID, namespace)
	} else {
		return m.rules.DeleteNamespace(ctx, userID, namespace)
	}
}
