// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/rulestore/bucketclient/bucket_client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketclient

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/ruler/rulestore"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketcache"
)

type testGroup struct {
	user, namespace string
	ruleGroup       rulefmt.RuleGroup
}

func TestListRules(t *testing.T) {
	rs := NewBucketRuleStore(objstore.NewInMemBucket(), nil, log.NewNopLogger())

	groups := []testGroup{
		{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "first testGroup"}},
		{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup"}},
		{user: "user1", namespace: "world", ruleGroup: rulefmt.RuleGroup{Name: "another namespace testGroup"}},
		{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user"}},
	}

	for _, g := range groups {
		desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
		require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
	}

	{
		users, err := rs.ListAllUsers(context.Background())
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"user1", "user2"}, users)
	}

	{
		user1Groups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "")
		require.NoError(t, err)
		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user1", Namespace: "hello", Name: "first testGroup"},
			{User: "user1", Namespace: "hello", Name: "second testGroup"},
			{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
		}, user1Groups)
	}

	{
		helloGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "hello")
		require.NoError(t, err)
		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user1", Namespace: "hello", Name: "first testGroup"},
			{User: "user1", Namespace: "hello", Name: "second testGroup"},
		}, helloGroups)
	}

	{
		invalidUserGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "invalid", "")
		require.NoError(t, err)
		require.Empty(t, invalidUserGroups)
	}

	{
		invalidNamespaceGroups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "invalid")
		require.NoError(t, err)
		require.Empty(t, invalidNamespaceGroups)
	}

	{
		user2Groups, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), "user2", "")
		require.NoError(t, err)
		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
		}, user2Groups)
	}
}

func TestLoadRules(t *testing.T) {
	rs := NewBucketRuleStore(objstore.NewInMemBucket(), nil, log.NewNopLogger())
	groups := []testGroup{
		{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "first testGroup", Interval: model.Duration(time.Minute), Rules: []rulefmt.RuleNode{{
			For:           model.Duration(5 * time.Minute),
			KeepFiringFor: model.Duration(2 * time.Minute),
			Labels:        map[string]string{"label1": "value1"},
		}}}},
		{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup", Interval: model.Duration(2 * time.Minute)}},
		{user: "user1", namespace: "world", ruleGroup: rulefmt.RuleGroup{Name: "another namespace testGroup", Interval: model.Duration(1 * time.Hour)}},
		{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user", Interval: model.Duration(5 * time.Minute)}},
		{user: "user3", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "third user", SourceTenants: []string{"tenant-1"}}},
	}

	for _, g := range groups {
		desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
		require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
	}

	// Utility function used to run each test case in isolation.
	listAllRuleGroups := func() map[string]rulespb.RuleGroupList {
		allGroupsMap := map[string]rulespb.RuleGroupList{}
		for _, u := range []string{"user1", "user2", "user3"} {
			rgl, err := rs.ListRuleGroupsForUserAndNamespace(context.Background(), u, "")
			require.NoError(t, err)
			allGroupsMap[u] = rgl
		}
		return allGroupsMap
	}

	// Before load, rules are not loaded
	{
		allGroupsMap := listAllRuleGroups()

		require.Len(t, allGroupsMap, 3)
		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user1", Namespace: "hello", Name: "first testGroup"},
			{User: "user1", Namespace: "hello", Name: "second testGroup"},
			{User: "user1", Namespace: "world", Name: "another namespace testGroup"},
		}, allGroupsMap["user1"])
		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user2", Namespace: "+-!@#$%. ", Name: "different user"},
		}, allGroupsMap["user2"])
	}

	// After load, rules are loaded.
	{
		allGroupsMap := listAllRuleGroups()

		missing, err := rs.LoadRuleGroups(context.Background(), allGroupsMap)
		require.NoError(t, err)
		require.Empty(t, missing)

		require.NoError(t, err)
		require.Len(t, allGroupsMap, 3)

		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user1", Namespace: "hello", Name: "first testGroup", Interval: time.Minute, Rules: []*rulespb.RuleDesc{
				{
					For:           5 * time.Minute,
					KeepFiringFor: 2 * time.Minute,
					Labels:        []mimirpb.LabelAdapter{{Name: "label1", Value: "value1"}},
				},
			}},
			{User: "user1", Namespace: "hello", Name: "second testGroup", Interval: 2 * time.Minute},
			{User: "user1", Namespace: "world", Name: "another namespace testGroup", Interval: 1 * time.Hour},
		}, allGroupsMap["user1"])

		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user2", Namespace: "+-!@#$%. ", Name: "different user", Interval: 5 * time.Minute},
		}, allGroupsMap["user2"])

		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user3", Namespace: "hello", Name: "third user", SourceTenants: []string{"tenant-1"}},
		}, allGroupsMap["user3"])
	}

	// Load a missing rule groups doesn't fail but return missing ones.
	{
		allGroupsMap := listAllRuleGroups()

		require.NoError(t, rs.DeleteRuleGroup(context.Background(), "user1", "hello", "first testGroup"))
		missing, err := rs.LoadRuleGroups(context.Background(), allGroupsMap)
		require.NoError(t, err)
		require.ElementsMatch(t, rulespb.RuleGroupList{{User: "user1", Namespace: "hello", Name: "first testGroup"}}, missing)

		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user1", Namespace: "hello", Name: "first testGroup"}, // The missing one still exists in the rule groups map, but has not been loaded.
			{User: "user1", Namespace: "hello", Name: "second testGroup", Interval: 2 * time.Minute},
			{User: "user1", Namespace: "world", Name: "another namespace testGroup", Interval: 1 * time.Hour},
		}, allGroupsMap["user1"])

		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user2", Namespace: "+-!@#$%. ", Name: "different user", Interval: 5 * time.Minute},
		}, allGroupsMap["user2"])

		require.ElementsMatch(t, []*rulespb.RuleGroupDesc{
			{User: "user3", Namespace: "hello", Name: "third user", SourceTenants: []string{"tenant-1"}},
		}, allGroupsMap["user3"])
	}

	// Loading group with mismatched info fails.
	{
		require.NoError(t, rs.SetRuleGroup(context.Background(), "user1", "hello", &rulespb.RuleGroupDesc{User: "user2", Namespace: "world", Name: "first testGroup"}))

		allGroupsMap := listAllRuleGroups()
		missing, err := rs.LoadRuleGroups(context.Background(), allGroupsMap)
		require.EqualError(t, err, "mismatch between requested rule group and loaded rule group, requested: user=\"user1\", namespace=\"hello\", group=\"first testGroup\", loaded: user=\"user2\", namespace=\"world\", group=\"first testGroup\"")
		require.Empty(t, missing)
	}
}

func TestDelete(t *testing.T) {
	bucketClient := objstore.NewInMemBucket()
	rs := NewBucketRuleStore(bucketClient, nil, log.NewNopLogger())

	groups := []testGroup{
		{user: "user1", namespace: "A", ruleGroup: rulefmt.RuleGroup{Name: "1"}},
		{user: "user1", namespace: "A", ruleGroup: rulefmt.RuleGroup{Name: "2"}},
		{user: "user1", namespace: "B", ruleGroup: rulefmt.RuleGroup{Name: "3"}},
		{user: "user1", namespace: "C", ruleGroup: rulefmt.RuleGroup{Name: "4"}},
		{user: "user2", namespace: "second", ruleGroup: rulefmt.RuleGroup{Name: "group"}},
		{user: "user3", namespace: "third", ruleGroup: rulefmt.RuleGroup{Name: "group"}},
	}

	for _, g := range groups {
		desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
		require.NoError(t, rs.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
	}

	// Verify that nothing was deleted, because we used canceled context.
	{
		canceled, cancelFn := context.WithCancel(context.Background())
		cancelFn()

		require.Error(t, rs.DeleteNamespace(canceled, "user1", ""))

		require.Equal(t, []string{
			"rules/user1/" + getRuleGroupObjectKey("A", "1"),
			"rules/user1/" + getRuleGroupObjectKey("A", "2"),
			"rules/user1/" + getRuleGroupObjectKey("B", "3"),
			"rules/user1/" + getRuleGroupObjectKey("C", "4"),
			"rules/user2/" + getRuleGroupObjectKey("second", "group"),
			"rules/user3/" + getRuleGroupObjectKey("third", "group"),
		}, getSortedObjectKeys(bucketClient))
	}

	// Verify that we can delete individual rule group, or entire namespace.
	{
		require.NoError(t, rs.DeleteRuleGroup(context.Background(), "user2", "second", "group"))
		require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", "A"))

		require.Equal(t, []string{
			"rules/user1/" + getRuleGroupObjectKey("B", "3"),
			"rules/user1/" + getRuleGroupObjectKey("C", "4"),
			"rules/user3/" + getRuleGroupObjectKey("third", "group"),
		}, getSortedObjectKeys(bucketClient))
	}

	// Verify that we can delete all remaining namespaces for user1.
	{
		require.NoError(t, rs.DeleteNamespace(context.Background(), "user1", ""))

		require.Equal(t, []string{
			"rules/user3/" + getRuleGroupObjectKey("third", "group"),
		}, getSortedObjectKeys(bucketClient))
	}

	{
		// Trying to delete empty namespace again will result in error.
		require.Equal(t, rulestore.ErrGroupNamespaceNotFound, rs.DeleteNamespace(context.Background(), "user1", ""))
	}
}

func getSortedObjectKeys(bucketClient interface{}) []string {
	if typed, ok := bucketClient.(*objstore.InMemBucket); ok {
		var keys []string
		for key := range typed.Objects() {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		return keys
	}

	return nil
}

func TestParseRuleGroupObjectKey(t *testing.T) {
	decodedNamespace := "my-namespace"
	encodedNamespace := base64.URLEncoding.EncodeToString([]byte(decodedNamespace))

	decodedGroup := "my-group"
	encodedGroup := base64.URLEncoding.EncodeToString([]byte(decodedGroup))

	tests := map[string]struct {
		key               string
		expectedErr       error
		expectedNamespace string
		expectedGroup     string
	}{
		"empty object key": {
			key:         "",
			expectedErr: errInvalidRuleGroupKey,
		},
		"invalid object key pattern": {
			key:         "way/too/long",
			expectedErr: errInvalidRuleGroupKey,
		},
		"empty namespace": {
			key:         fmt.Sprintf("/%s", encodedGroup),
			expectedErr: errEmptyNamespace,
		},
		"invalid namespace encoding": {
			key:         fmt.Sprintf("invalid/%s", encodedGroup),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"empty group": {
			key:         fmt.Sprintf("%s/", encodedNamespace),
			expectedErr: errEmptyGroupName,
		},
		"invalid group encoding": {
			key:         fmt.Sprintf("%s/invalid", encodedNamespace),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"valid object key": {
			key:               fmt.Sprintf("%s/%s", encodedNamespace, encodedGroup),
			expectedNamespace: decodedNamespace,
			expectedGroup:     decodedGroup,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			namespace, group, err := parseRuleGroupObjectKey(testData.key)

			if testData.expectedErr != nil {
				assert.EqualError(t, err, testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, testData.expectedNamespace, namespace)
				assert.Equal(t, testData.expectedGroup, group)
			}
		})
	}
}

func TestParseRuleGroupObjectKeyWithUser(t *testing.T) {
	decodedNamespace := "my-namespace"
	encodedNamespace := base64.URLEncoding.EncodeToString([]byte(decodedNamespace))

	decodedGroup := "my-group"
	encodedGroup := base64.URLEncoding.EncodeToString([]byte(decodedGroup))

	tests := map[string]struct {
		key               string
		expectedErr       error
		expectedUser      string
		expectedNamespace string
		expectedGroup     string
	}{
		"empty object key": {
			key:         "",
			expectedErr: errInvalidRuleGroupKey,
		},
		"invalid object key pattern": {
			key:         "way/too/much/long",
			expectedErr: errInvalidRuleGroupKey,
		},
		"empty user": {
			key:         fmt.Sprintf("/%s/%s", encodedNamespace, encodedGroup),
			expectedErr: errEmptyUser,
		},
		"empty namespace": {
			key:         fmt.Sprintf("user-1//%s", encodedGroup),
			expectedErr: errEmptyNamespace,
		},
		"invalid namespace encoding": {
			key:         fmt.Sprintf("user-1/invalid/%s", encodedGroup),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"empty group name": {
			key:         fmt.Sprintf("user-1/%s/", encodedNamespace),
			expectedErr: errEmptyGroupName,
		},
		"invalid group encoding": {
			key:         fmt.Sprintf("user-1/%s/invalid", encodedNamespace),
			expectedErr: errors.New("illegal base64 data at input byte 4"),
		},
		"valid object key": {
			key:               fmt.Sprintf("user-1/%s/%s", encodedNamespace, encodedGroup),
			expectedUser:      "user-1",
			expectedNamespace: decodedNamespace,
			expectedGroup:     decodedGroup,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			user, namespace, group, err := parseRuleGroupObjectKeyWithUser(testData.key)

			if testData.expectedErr != nil {
				assert.EqualError(t, err, testData.expectedErr.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, testData.expectedUser, user)
				assert.Equal(t, testData.expectedNamespace, namespace)
				assert.Equal(t, testData.expectedGroup, group)
			}
		})
	}
}

func TestListAllRuleGroupsWithNoNamespaceOrGroup(t *testing.T) {
	obj := mockBucket{
		names: []string{
			"rules/",
			"rules/user1/",
			"rules/user2/bnM=/",         // namespace "ns", ends with '/'
			"rules/user3/bnM=/Z3JvdXAx", // namespace "ns", group "group1"
		},
	}

	s := NewBucketRuleStore(obj, nil, log.NewNopLogger())
	out, err := s.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "")
	require.NoError(t, err)
	require.Equal(t, 0, len(out))

	out, err = s.ListRuleGroupsForUserAndNamespace(context.Background(), "user2", "")
	require.NoError(t, err)
	require.Equal(t, 0, len(out))

	out, err = s.ListRuleGroupsForUserAndNamespace(context.Background(), "user3", "")
	require.NoError(t, err)
	require.Equal(t, 1, len(out))           // one group
	require.Equal(t, "group1", out[0].Name) // also verify its name
}

type mockBucket struct {
	objstore.Bucket

	names []string
}

func (mb mockBucket) Iter(_ context.Context, _ string, f func(string) error, _ ...objstore.IterOption) error {
	for _, n := range mb.names {
		if err := f(n); err != nil {
			return err
		}
	}
	return nil
}

func TestCachingAndInvalidation(t *testing.T) {
	fixtureGroups := []testGroup{
		{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "first testGroup"}},
		{user: "user1", namespace: "hello", ruleGroup: rulefmt.RuleGroup{Name: "second testGroup"}},
		{user: "user1", namespace: "world", ruleGroup: rulefmt.RuleGroup{Name: "another namespace testGroup"}},
		{user: "user2", namespace: "+-!@#$%. ", ruleGroup: rulefmt.RuleGroup{Name: "different user"}},
	}

	setup := func(t *testing.T) (*cache.InstrumentedMockCache, *BucketRuleStore) {
		iterCodec := &bucketcache.JSONIterCodec{}
		baseClient := objstore.NewInMemBucket()
		mockCache := cache.NewInstrumentedMockCache()

		cacheCfg := bucketcache.NewCachingBucketConfig()
		cacheCfg.CacheIter("rule-iter", mockCache, matchAll, time.Minute, iterCodec)
		cacheCfg.CacheGet("rule-groups", mockCache, matchAll, 1024^2, time.Minute, time.Minute, time.Minute)
		cacheClient, err := bucketcache.NewCachingBucket("rule-store", baseClient, cacheCfg, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.NoError(t, err)

		ruleStore := NewBucketRuleStore(cacheClient, nil, log.NewNopLogger())

		for _, g := range fixtureGroups {
			desc := rulespb.ToProto(g.user, g.namespace, g.ruleGroup)
			require.NoError(t, ruleStore.SetRuleGroup(context.Background(), g.user, g.namespace, desc))
		}

		return mockCache, ruleStore
	}

	t.Run("list users with cache", func(t *testing.T) {
		mockCache, ruleStore := setup(t)
		startStores := mockCache.CountStoreCalls()
		startFetches := mockCache.CountFetchCalls()

		users, err := ruleStore.ListAllUsers(context.Background())

		require.NoError(t, err)
		require.Equal(t, []string{"user1", "user2"}, users)

		require.Equal(t, 1, mockCache.CountStoreCalls()-startStores)
		require.Equal(t, 1, mockCache.CountFetchCalls()-startFetches)
	})

	t.Run("list users no cache", func(t *testing.T) {
		mockCache, rs := setup(t)
		startStores := mockCache.CountStoreCalls()
		startFetches := mockCache.CountFetchCalls()

		users, err := rs.ListAllUsers(context.Background(), rulestore.WithCacheDisabled())

		require.NoError(t, err)
		require.Equal(t, []string{"user1", "user2"}, users)

		require.Equal(t, 1, mockCache.CountStoreCalls()-startStores)
		require.Equal(t, 0, mockCache.CountFetchCalls()-startFetches)
	})

	t.Run("list rule groups with cache", func(t *testing.T) {
		mockCache, ruleStore := setup(t)
		startStores := mockCache.CountStoreCalls()
		startFetches := mockCache.CountFetchCalls()

		groups, err := ruleStore.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "")

		require.NoError(t, err)
		require.Equal(t, rulespb.RuleGroupList{
			{
				Name:      "first testGroup",
				User:      "user1",
				Namespace: "hello",
			},
			{
				Name:      "second testGroup",
				User:      "user1",
				Namespace: "hello",
			},
			{
				Name:      "another namespace testGroup",
				User:      "user1",
				Namespace: "world",
			},
		}, groups)

		require.Equal(t, 1, mockCache.CountStoreCalls()-startStores)
		require.Equal(t, 1, mockCache.CountFetchCalls()-startFetches)
	})

	t.Run("list rule groups no cache", func(t *testing.T) {
		mockCache, ruleStore := setup(t)
		startStores := mockCache.CountStoreCalls()
		startFetches := mockCache.CountFetchCalls()

		groups, err := ruleStore.ListRuleGroupsForUserAndNamespace(context.Background(), "user1", "", rulestore.WithCacheDisabled())

		require.NoError(t, err)
		require.Equal(t, rulespb.RuleGroupList{
			{
				Name:      "first testGroup",
				User:      "user1",
				Namespace: "hello",
			},
			{
				Name:      "second testGroup",
				User:      "user1",
				Namespace: "hello",
			},
			{
				Name:      "another namespace testGroup",
				User:      "user1",
				Namespace: "world",
			},
		}, groups)

		require.Equal(t, 1, mockCache.CountStoreCalls()-startStores)
		require.Equal(t, 0, mockCache.CountFetchCalls()-startFetches)
	})

	t.Run("get rule group from cache", func(t *testing.T) {
		mockCache, ruleStore := setup(t)
		startStores := mockCache.CountStoreCalls()
		startFetches := mockCache.CountFetchCalls()

		group, err := ruleStore.GetRuleGroup(context.Background(), "user1", "world", "another namespace testGroup")

		require.NoError(t, err)
		require.NotNil(t, group)

		require.Equal(t, 2, mockCache.CountStoreCalls()-startStores) // content set + exists set
		require.Equal(t, 1, mockCache.CountFetchCalls()-startFetches)
	})

	t.Run("get rule groups after invalidation", func(t *testing.T) {
		mockCache, ruleStore := setup(t)
		startStores := mockCache.CountStoreCalls()
		startFetches := mockCache.CountFetchCalls()
		startDeletes := mockCache.CountDeleteCalls()

		group, err := ruleStore.GetRuleGroup(context.Background(), "user1", "world", "another namespace testGroup")

		require.NoError(t, err)
		require.NotNil(t, group)
		require.Zero(t, group.QueryOffset)

		require.Equal(t, 2, mockCache.CountStoreCalls()-startStores) // content set + exists set
		require.Equal(t, 1, mockCache.CountFetchCalls()-startFetches)

		group.QueryOffset = 42 * time.Second
		require.NoError(t, ruleStore.SetRuleGroup(context.Background(), group.User, group.Namespace, group))

		require.Equal(t, 4, mockCache.CountStoreCalls()-startStores) // += content lock + exists lock
		require.Equal(t, 1, mockCache.CountFetchCalls()-startFetches)
		require.Equal(t, 2, mockCache.CountDeleteCalls()-startDeletes)

		modifiedGroup, err := ruleStore.GetRuleGroup(context.Background(), "user1", "world", "another namespace testGroup")
		require.NoError(t, err)
		require.NotNil(t, modifiedGroup)
		require.Equal(t, 42*time.Second, modifiedGroup.QueryOffset)

		require.Equal(t, 6, mockCache.CountStoreCalls()-startStores) // += content set + exists set
		require.Equal(t, 2, mockCache.CountFetchCalls()-startFetches)
		require.Equal(t, 2, mockCache.CountDeleteCalls()-startDeletes)
	})
}

func matchAll(string) bool {
	return true
}
