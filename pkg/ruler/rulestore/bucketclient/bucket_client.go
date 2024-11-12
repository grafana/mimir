// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/bucketclient/bucket_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// RulesPrefix is the bucket prefix under which all tenants rule groups are stored.
	RulesPrefix = "rules"

	loadConcurrency = 10
)

var (
	errInvalidRuleGroupKey = errors.New("invalid rule group object key")
	errEmptyUser           = errors.New("empty user")
	errEmptyNamespace      = errors.New("empty namespace")
	errEmptyGroupName      = errors.New("empty group name")
)

// BucketRuleStore is used to support the RuleStore interface against an object storage backend. It is implemented
// using the Thanos objstore.Bucket interface
type BucketRuleStore struct {
	bucket      objstore.Bucket
	cfgProvider bucket.TenantConfigProvider
	logger      log.Logger
}

func NewBucketRuleStore(bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *BucketRuleStore {
	return &BucketRuleStore{
		bucket:      bucket.NewPrefixedBucketClient(bkt, RulesPrefix),
		cfgProvider: cfgProvider,
		logger:      logger,
	}
}

// getRuleGroup loads and return a rules group. If existing rule group is supplied, it is Reset and reused. If nil, new RuleGroupDesc is allocated.
func (b *BucketRuleStore) getRuleGroup(ctx context.Context, userID, namespace, groupName string, rg *rulespb.RuleGroupDesc, spanlog *spanlogger.SpanLogger) (*rulespb.RuleGroupDesc, error) {
	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	objectKey := getRuleGroupObjectKey(namespace, groupName)

	reader, err := userBucket.Get(ctx, objectKey)
	if userBucket.IsObjNotFoundErr(err) {
		spanlog.DebugLog("msg", "rule group does not exist", "user", userID, "key", objectKey)
		return nil, rulestore.ErrGroupNotFound
	}

	if err != nil {
		return nil, errors.Wrapf(err, "failed to get rule group %s", objectKey)
	}
	defer func() { _ = reader.Close() }()

	buf, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read rule group %s", objectKey)
	}

	if rg == nil {
		rg = &rulespb.RuleGroupDesc{}
	} else {
		rg.Reset()
	}

	err = proto.Unmarshal(buf, rg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal rule group %s", objectKey)
	}

	return rg, nil
}

// ListAllUsers implements rules.RuleStore.
func (b *BucketRuleStore) ListAllUsers(ctx context.Context, opts ...rulestore.Option) ([]string, error) {
	logger, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BucketRuleStore.ListAllUsers")
	defer logger.Finish()

	options := rulestore.CollectOptions(opts...)
	if options.DisableCache {
		ctx = bucketcache.WithCacheLookupEnabled(ctx, false)
	}

	var users []string
	err := b.bucket.Iter(ctx, "", func(user string) error {
		users = append(users, strings.TrimSuffix(user, objstore.DirDelim))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("unable to list users in rule store bucket: %w", err)
	}

	return users, nil
}

// ListRuleGroupsForUserAndNamespace implements rules.RuleStore.
func (b *BucketRuleStore) ListRuleGroupsForUserAndNamespace(ctx context.Context, userID string, namespace string, opts ...rulestore.Option) (rulespb.RuleGroupList, error) {
	logger, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BucketRuleStore.ListRuleGroupsForUserAndNamespace")
	defer logger.Finish()

	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	groupList := rulespb.RuleGroupList{}

	options := rulestore.CollectOptions(opts...)
	if options.DisableCache {
		ctx = bucketcache.WithCacheLookupEnabled(ctx, false)
	}

	// The prefix to list objects depends on whether the namespace has been
	// specified in the request.
	prefix := ""
	if namespace != "" {
		prefix = getNamespacePrefix(namespace)
	}

	err := userBucket.Iter(ctx, prefix, func(key string) error {
		namespace, group, err := parseRuleGroupObjectKey(key)
		if err != nil {
			level.Warn(logger).Log("msg", "invalid rule group object key found while listing rule groups", "user", userID, "key", key, "err", err)

			// Do not fail just because of a spurious item in the bucket.
			return nil
		}

		groupList = append(groupList, &rulespb.RuleGroupDesc{
			User:      userID,
			Namespace: namespace,
			Name:      group,
		})
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return nil, err
	}

	return groupList, nil
}

// LoadRuleGroups implements rules.RuleStore.
func (b *BucketRuleStore) LoadRuleGroups(ctx context.Context, groupsToLoad map[string]rulespb.RuleGroupList) (missing rulespb.RuleGroupList, err error) {
	logger, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BucketRuleStore.LoadRuleGroups")
	defer logger.Finish()

	var (
		ch        = make(chan *rulespb.RuleGroupDesc)
		missingMx sync.Mutex
	)

	// Given we store one file per rule group. With this, we create a pool of workers that will
	// download all rule groups in parallel. We limit the number of workers to avoid a
	// particular user having too many rule groups rate limiting us with the object storage.
	g, gCtx := errgroup.WithContext(ctx)
	for i := 0; i < loadConcurrency; i++ {
		g.Go(func() error {
			for inputGroup := range ch {
				user, namespace, groupName := inputGroup.GetUser(), inputGroup.GetNamespace(), inputGroup.GetName()
				if user == "" || namespace == "" || groupName == "" {
					return fmt.Errorf("invalid rule group: user=%q, namespace=%q, group=%q", user, namespace, groupName)
				}

				// Reuse group pointer from the map.
				loadedGroup, err := b.getRuleGroup(gCtx, user, namespace, groupName, inputGroup, logger)

				switch {
				case errors.Is(err, rulestore.ErrGroupNotFound):
					missingMx.Lock()
					missing = append(missing, inputGroup)
					missingMx.Unlock()

				case err != nil:
					return errors.Wrapf(err, "get rule group user=%q, namespace=%q, name=%q", user, namespace, groupName)

				case user != loadedGroup.User || namespace != loadedGroup.Namespace || groupName != loadedGroup.Name:
					return fmt.Errorf("mismatch between requested rule group and loaded rule group, requested: user=%q, namespace=%q, group=%q, loaded: user=%q, namespace=%q, group=%q", user, namespace, groupName, loadedGroup.User, loadedGroup.Namespace, loadedGroup.Name)
				}
			}

			return nil
		})
	}

outer:
	for _, gs := range groupsToLoad {
		for _, g := range gs {
			if g == nil {
				continue
			}
			select {
			case <-gCtx.Done():
				break outer
			case ch <- g:
				// ok
			}
		}
	}
	close(ch)

	return missing, g.Wait()
}

// GetRuleGroup implements rules.RuleStore.
func (b *BucketRuleStore) GetRuleGroup(ctx context.Context, userID string, namespace string, group string) (*rulespb.RuleGroupDesc, error) {
	logger, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BucketRuleStore.GetRuleGroup")
	defer logger.Finish()

	return b.getRuleGroup(ctx, userID, namespace, group, nil, logger)
}

// SetRuleGroup implements rules.RuleStore.
func (b *BucketRuleStore) SetRuleGroup(ctx context.Context, userID string, namespace string, group *rulespb.RuleGroupDesc) error {
	logger, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BucketRuleStore.SetRuleGroup")
	defer logger.Finish()

	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	data, err := proto.Marshal(group)
	if err != nil {
		return err
	}

	return userBucket.Upload(ctx, getRuleGroupObjectKey(namespace, group.Name), bytes.NewBuffer(data))
}

// DeleteRuleGroup implements rules.RuleStore.
func (b *BucketRuleStore) DeleteRuleGroup(ctx context.Context, userID string, namespace string, group string) error {
	logger, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BucketRuleStore.DeleteRuleGroup")
	defer logger.Finish()

	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	err := userBucket.Delete(ctx, getRuleGroupObjectKey(namespace, group))
	if b.bucket.IsObjNotFoundErr(err) {
		return rulestore.ErrGroupNotFound
	}
	return err
}

// DeleteNamespace implements rules.RuleStore.
func (b *BucketRuleStore) DeleteNamespace(ctx context.Context, userID string, namespace string) error {
	logger, ctx := spanlogger.NewWithLogger(ctx, b.logger, "BucketRuleStore.DeleteNamespace")
	defer logger.Finish()

	// Disable caching when listing all rule groups for a user since listing entries are not
	// invalidated in the cache when rule groups are modified and we need to delete everything.
	ruleGroupList, err := b.ListRuleGroupsForUserAndNamespace(ctx, userID, namespace, rulestore.WithCacheDisabled())
	if err != nil {
		return err
	}

	if len(ruleGroupList) == 0 {
		return rulestore.ErrGroupNamespaceNotFound
	}

	userBucket := bucket.NewUserBucketClient(userID, b.bucket, b.cfgProvider)
	for _, rg := range ruleGroupList {
		if err := ctx.Err(); err != nil {
			return err
		}
		objectKey := getRuleGroupObjectKey(rg.Namespace, rg.Name)
		logger.DebugLog("msg", "deleting rule group", "user", userID, "namespace", namespace, "key", objectKey)
		err = userBucket.Delete(ctx, objectKey)
		if err != nil {
			level.Error(logger).Log("msg", "unable to delete rule group from namespace", "user", userID, "namespace", namespace, "key", objectKey, "err", err)
			return err
		}
	}

	return nil
}

func getNamespacePrefix(namespace string) string {
	return base64.URLEncoding.EncodeToString([]byte(namespace)) + objstore.DirDelim
}

func getRuleGroupObjectKey(namespace, group string) string {
	return getNamespacePrefix(namespace) + base64.URLEncoding.EncodeToString([]byte(group))
}

// parseRuleGroupObjectKeyWithUser parses a bucket object key in the format "<user>/<namespace>/<rules group>".
func parseRuleGroupObjectKeyWithUser(key string) (user, namespace, group string, err error) {
	parts := strings.SplitN(key, objstore.DirDelim, 2)
	if len(parts) != 2 {
		return "", "", "", errInvalidRuleGroupKey
	}

	user = parts[0]
	if user == "" {
		return "", "", "", errEmptyUser
	}
	namespace, group, err = parseRuleGroupObjectKey(parts[1])
	return
}

// parseRuleGroupObjectKey parses a bucket object key in the format "<namespace>/<rules group>".
func parseRuleGroupObjectKey(key string) (namespace, group string, _ error) {
	parts := strings.Split(key, objstore.DirDelim)
	if len(parts) != 2 {
		return "", "", errInvalidRuleGroupKey
	}

	decodedNamespace, err := base64.URLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", "", err
	}

	if len(decodedNamespace) == 0 {
		return "", "", errEmptyNamespace
	}

	decodedGroup, err := base64.URLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", err
	}

	if len(decodedGroup) == 0 {
		return "", "", errEmptyGroupName
	}

	return string(decodedNamespace), string(decodedGroup), nil
}
