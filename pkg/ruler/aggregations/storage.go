package aggregations

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/storage/bucket"
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
	logger      log.Logger
}

func NewAggregationsRuleStore(bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *AggregationsRuleStore {
	return &AggregationsRuleStore{
		bucket:      bucket.NewPrefixedBucketClient(bkt, bucketPrefix),
		cfgProvider: cfgProvider,
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
	return users, nil
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
	}

	return groupList, nil
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
