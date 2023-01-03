package aggregations

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/ephemeral"
)

const (
	// We expect objects "aggregations/<tenant>/rules.json". Prefix is passed to PrefixedBucketClient, so we don't need to work with it anymore.
	bucketPrefix = "aggregations"
	rulesFile    = "rules.json"

	aggregationsNamespace = "aggregations"
)

// AggregationsRuleStore is loads aggregation rules from the bucket and supplies them as PromQL rules to the ruler.
// There is only one aggregations file per user. All PromQL groups are put into single namespace.
type AggregationsRuleStore struct {
	bucket      objstore.Bucket
	cfgProvider bucket.TenantConfigProvider
	kv          kv.Client
	logger      log.Logger
}

func NewAggregationsRuleStore(bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, kv kv.Client, logger log.Logger) *AggregationsRuleStore {
	return &AggregationsRuleStore{
		bucket:      bucket.NewPrefixedBucketClient(bkt, bucketPrefix),
		cfgProvider: cfgProvider,
		kv:          kv,
		logger:      logger,
	}
}

// ListAllUsers implements rules.RuleStore.
func (b *AggregationsRuleStore) ListAllUsers(ctx context.Context) ([]string, error) {
	var users []string
	err := b.bucket.Iter(ctx, "", func(path string) error {
		user := strings.TrimSuffix(path, objstore.DirDelim+rulesFile)
		if user == path {
			return nil
		}

		users = append(users, user)
		return nil
	}, objstore.WithRecursiveIter)
	if err != nil {
		return nil, fmt.Errorf("unable to list users in rule store bucket: %w", err)
	}

	b.removeUsersWithNoRulesFromKV(ctx, users)
	return users, nil
}

func (b *AggregationsRuleStore) removeUsersWithNoRulesFromKV(ctx context.Context, users []string) {
	keys, err := b.kv.List(ctx, "")
	if err != nil {
		level.Error(b.logger).Log("msg", "failed to list users with ephemeral metrics in the KV", "err", err)
		return
	}

	// If there are users with ephemeral metrics in the KV that have no rules anymore, remove them from KV.
	// TODO: memberlist doesn't support removal of keys yet. Instead of deleting, we remove all entries from ephemeral metrics map.
	for _, user := range keys {
		if util.StringsContain(users, user) {
			// Users still has rules. Ignore it.
			continue
		}

		// User not found, let's clear its ephemeral metrics map.
		err := b.kv.CAS(ctx, user, func(in interface{}) (out interface{}, retry bool, err error) {
			if in == nil {
				return nil, false, nil
			}

			em, ok := in.(*ephemeral.Metrics)
			if !ok {
				return nil, false, fmt.Errorf("invalid type found in KV store, key: %s, type: %T", user, in)
			}

			for _, m := range em.EphemeralMetrics() {
				em.RemoveEphemeral(m)
			}
			return em, true, nil
		})
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to clear ephemeral metrics for user", "user", user, "err", err)
			return
		}
	}
}

// ListRuleGroupsForUserAndNamespace implements rules.RuleStore.
func (b *AggregationsRuleStore) ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string) (rulespb.RuleGroupList, error) {
	// This store only handles aggregations namespace
	if namespace != "" && namespace != aggregationsNamespace {
		return nil, nil
	}

	r, err := b.bucket.Get(ctx, path.Join(userID, rulesFile))
	if err != nil {
		if b.bucket.IsObjNotFoundErr(err) {
			return nil, nil
		}
	}

	defer func() { _ = r.Close() }()

	rules, err := LoadRules(r)
	if err != nil {
		return nil, fmt.Errorf("failed to load rules: %w", err)
	}

	metricNames := map[string]bool{}
	groupList := rulespb.RuleGroupList{}

	for _, r := range rules.Decoded {
		rs := r.getTransforms()
		if rs == nil {
			return nil, fmt.Errorf("failed to generate PromQL rule for metric %s", r.Metric)
		}

		grp := &rulespb.RuleGroupDesc{
			Name:                         r.Metric,
			Namespace:                    aggregationsNamespace,
			Interval:                     time.Minute,
			Rules:                        rs,
			User:                         userID,
			EvaluationDelay:              time.Minute,
			AlignExecutionTimeOnInterval: true,
		}

		groupList = append(groupList, grp)
		metricNames[r.Metric] = true
	}

	// When we load new rules, we also update KV store with ephemeral metrics.
	b.updateEphemeralMetricsForUser(ctx, userID, metricNames)

	return groupList, nil
}

func (b *AggregationsRuleStore) updateEphemeralMetricsForUser(ctx context.Context, user string, metricNames map[string]bool) {
	err := b.kv.CAS(ctx, user, func(in interface{}) (out interface{}, retry bool, err error) {
		em, ok := in.(*ephemeral.Metrics)
		if !ok || em == nil {
			em = ephemeral.NewMetrics()
		}

		for _, m := range em.EphemeralMetrics() {
			if !metricNames[m] {
				em.RemoveEphemeral(m)
			}
		}

		for m := range metricNames {
			em.AddEphemeral(m)
		}

		return em, true, nil
	})
	if err != nil {
		level.Error(b.logger).Log("msg", "failed to update ephemeral metrics for user", "user", user, "err", err)
	}
}

// LoadRuleGroups implements rules.RuleStore.
func (b *AggregationsRuleStore) LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rulespb.RuleGroupList) error {
	// Nothing to do here, as List method already does the loading.
	return nil
}

// GetRuleGroup implements rules.RuleStore.
func (b *AggregationsRuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (*rulespb.RuleGroupDesc, error) {
	return nil, fmt.Errorf("individual rule group cannot be fetched")
}

// SetRuleGroup implements rules.RuleStore.
func (b *AggregationsRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rulespb.RuleGroupDesc) error {
	return fmt.Errorf("individual rule group cannot be set")
}

// DeleteRuleGroup implements rules.RuleStore.
func (b *AggregationsRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	return fmt.Errorf("individual rule group cannot be deleted")
}

// DeleteNamespace implements rules.RuleStore.
func (b *AggregationsRuleStore) DeleteNamespace(ctx context.Context, userID string, namespace string) error {
	return fmt.Errorf("aggregations namespace cannot be deleted")
}
