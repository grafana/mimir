// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertstore/store_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertstore

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/cluster/clusterpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
)

func TestAlertStore_ListAllUsers(t *testing.T) {
	ctx := context.Background()
	user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
	user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}
	user2GrafanaCfg := alertspb.GrafanaAlertConfigDesc{User: "user-2", RawConfig: "content-grafana-2"}
	user3GrafanaCfg := alertspb.GrafanaAlertConfigDesc{User: "user-3", RawConfig: "content-grafana-3"}

	t.Run("fetching grafana configs disabled", func(t *testing.T) {
		bucket := objstore.NewInMemBucket()
		store := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, bucket, nil, log.NewNopLogger())

		// The storage is empty.
		{
			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.Empty(t, users)
		}

		// The storage contains users.
		{
			require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))
			require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"user-1", "user-2"}, users)
		}

		// The storage contains Grafana configurations but fetching is disabled.
		{
			require.NoError(t, store.SetGrafanaAlertConfig(ctx, user3GrafanaCfg))

			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"user-1", "user-2"}, users)
		}
	})

	t.Run("fetching grafana configs enabled", func(t *testing.T) {
		bucket := objstore.NewInMemBucket()
		cfg := bucketclient.BucketAlertStoreConfig{FetchGrafanaConfig: true}
		store := bucketclient.NewBucketAlertStore(cfg, bucket, nil, log.NewNopLogger())

		// The storage is empty.
		{
			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.Empty(t, users)
		}

		// The storage contains user with only Mimir config.
		{
			require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))

			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"user-1"}, users)
		}

		// The storage contains user with only Grafana config.
		{
			require.NoError(t, store.SetGrafanaAlertConfig(ctx, user3GrafanaCfg))

			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"user-1", "user-3"}, users)
		}

		// The storage contains a user with both configs.
		{
			require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))
			require.NoError(t, store.SetGrafanaAlertConfig(ctx, user2GrafanaCfg))

			users, err := store.ListAllUsers(ctx)
			require.NoError(t, err)
			assert.ElementsMatch(t, []string{"user-1", "user-2", "user-3"}, users)
		}
	})
}

func TestAlertStore_SetAndGetAlertConfig(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	cfg := bucketclient.BucketAlertStoreConfig{FetchGrafanaConfig: true}
	store := bucketclient.NewBucketAlertStore(cfg, bucket, nil, log.NewNopLogger())

	ctx := context.Background()
	user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
	user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}

	// The user has no config.
	{
		_, err := store.GetAlertConfig(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)
	}

	// The user has a config
	{
		require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))
		require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

		config, err := store.GetAlertConfig(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, user1Cfg, config)

		config, err = store.GetAlertConfig(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, user2Cfg, config)

		// Ensure the config is stored at the expected location. Without this check
		// we have no guarantee that the objects are stored at the expected location.
		exists, err := bucket.Exists(ctx, "alerts/user-1")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = bucket.Exists(ctx, "alerts/user-2")
		require.NoError(t, err)
		assert.True(t, exists)
	}
}

func TestStore_GetAlertConfigs(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	cfg := bucketclient.BucketAlertStoreConfig{FetchGrafanaConfig: true}
	store := bucketclient.NewBucketAlertStore(cfg, bucket, nil, log.NewNopLogger())

	ctx := context.Background()
	user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
	user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}
	user2GrafanaCfg := alertspb.GrafanaAlertConfigDesc{User: "user-2", RawConfig: "content-2"}

	// The storage is empty.
	{
		configs, err := store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
		require.NoError(t, err)
		assert.Empty(t, configs)
	}

	// The storage contains some configs.
	{
		require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))

		configs, err := store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
		require.NoError(t, err)
		assert.Contains(t, configs, "user-1")
		assert.NotContains(t, configs, "user-2")
		assert.Equal(t, user1Cfg, configs["user-1"].Mimir)

		// Add another user config.
		require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))
		require.NoError(t, store.SetGrafanaAlertConfig(ctx, user2GrafanaCfg))

		// Should return both Mimir and Grafana Alertmanager configurations.
		configs, err = store.GetAlertConfigs(ctx, []string{"user-1", "user-2"})
		require.NoError(t, err)
		assert.Contains(t, configs, "user-1")
		assert.Contains(t, configs, "user-2")
		assert.Equal(t, user1Cfg, configs["user-1"].Mimir)
		assert.Equal(t, alertspb.GrafanaAlertConfigDesc{}, configs["user-1"].Grafana)
		assert.Equal(t, user2Cfg, configs["user-2"].Mimir)
		assert.Equal(t, user2GrafanaCfg, configs["user-2"].Grafana)
	}
}

func TestAlertStore_DeleteAlertConfig(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, bucket, nil, log.NewNopLogger())

	ctx := context.Background()
	user1Cfg := alertspb.AlertConfigDesc{User: "user-1", RawConfig: "content-1"}
	user2Cfg := alertspb.AlertConfigDesc{User: "user-2", RawConfig: "content-2"}

	// Upload the config for 2 users.
	require.NoError(t, store.SetAlertConfig(ctx, user1Cfg))
	require.NoError(t, store.SetAlertConfig(ctx, user2Cfg))

	// Ensure the config has been correctly uploaded.
	config, err := store.GetAlertConfig(ctx, "user-1")
	require.NoError(t, err)
	assert.Equal(t, user1Cfg, config)

	config, err = store.GetAlertConfig(ctx, "user-2")
	require.NoError(t, err)
	assert.Equal(t, user2Cfg, config)

	// Delete the config for user-1.
	require.NoError(t, store.DeleteAlertConfig(ctx, "user-1"))

	// Ensure the correct config has been deleted.
	_, err = store.GetAlertConfig(ctx, "user-1")
	assert.Equal(t, alertspb.ErrNotFound, err)

	config, err = store.GetAlertConfig(ctx, "user-2")
	require.NoError(t, err)
	assert.Equal(t, user2Cfg, config)

	// Delete again (should be idempotent).
	require.NoError(t, store.DeleteAlertConfig(ctx, "user-1"))
}

func makeTestFullState(content string) alertspb.FullStateDesc {
	return alertspb.FullStateDesc{
		State: &clusterpb.FullState{
			Parts: []clusterpb.Part{
				{
					Key:  "key",
					Data: []byte(content),
				},
			},
		},
	}
}

func makeTestGrafanaAlertConfig(t *testing.T, user, cfg, hash string, createdAtTimestamp int64, isDefault bool) alertspb.GrafanaAlertConfigDesc {
	t.Helper()

	return alertspb.GrafanaAlertConfigDesc{
		User:               user,
		RawConfig:          cfg,
		Hash:               hash,
		CreatedAtTimestamp: createdAtTimestamp,
		Default:            isDefault,
	}
}

func TestBucketAlertStore_GetSetDeleteFullState(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, bucket, nil, log.NewNopLogger())

	ctx := context.Background()
	state1 := makeTestFullState("one")
	state2 := makeTestFullState("two")

	// The storage is empty.
	{
		_, err := store.GetFullState(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		_, err = store.GetFullState(ctx, "user-2")
		assert.Equal(t, alertspb.ErrNotFound, err)

		users, err := store.ListUsersWithFullState(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{}, users)
	}

	// The storage contains users.
	{
		require.NoError(t, store.SetFullState(ctx, "user-1", state1))
		require.NoError(t, store.SetFullState(ctx, "user-2", state2))

		res, err := store.GetFullState(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, state1, res)

		res, err = store.GetFullState(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, state2, res)

		// Ensure the config is stored at the expected location. Without this check
		// we have no guarantee that the objects are stored at the expected location.
		exists, err := bucket.Exists(ctx, "alertmanager/user-1/fullstate")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = bucket.Exists(ctx, "alertmanager/user-2/fullstate")
		require.NoError(t, err)
		assert.True(t, exists)

		users, err := store.ListUsersWithFullState(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"user-1", "user-2"}, users)
	}

	// The storage has had user-1 deleted.
	{
		require.NoError(t, store.DeleteFullState(ctx, "user-1"))

		// Ensure the correct entry has been deleted.
		_, err := store.GetFullState(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		res, err := store.GetFullState(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, state2, res)

		users, err := store.ListUsersWithFullState(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"user-2"}, users)

		// Delete again (should be idempotent).
		require.NoError(t, store.DeleteFullState(ctx, "user-1"))
	}
}

func TestBucketAlertStore_GetSetDeleteGrafanaState(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, bucket, nil, log.NewNopLogger())

	ctx := context.Background()
	state1 := makeTestFullState("one")
	state2 := makeTestFullState("two")

	// The storage is empty.
	{
		_, err := store.GetFullGrafanaState(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		_, err = store.GetFullGrafanaState(ctx, "user-2")
		assert.Equal(t, alertspb.ErrNotFound, err)
	}

	// The storage contains users.
	{
		require.NoError(t, store.SetFullGrafanaState(ctx, "user-1", state1))
		require.NoError(t, store.SetFullGrafanaState(ctx, "user-2", state2))

		res, err := store.GetFullGrafanaState(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, state1, res)

		res, err = store.GetFullGrafanaState(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, state2, res)

		// Ensure the state is stored at the expected location. Without this check
		// we have no guarantee that the objects are stored at the expected location.
		exists, err := bucket.Exists(ctx, "grafana_alertmanager/user-1/grafana_fullstate")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = bucket.Exists(ctx, "grafana_alertmanager/user-2/grafana_fullstate")
		require.NoError(t, err)
		assert.True(t, exists)
	}

	// The storage has had user-1 deleted.
	{
		require.NoError(t, store.DeleteFullGrafanaState(ctx, "user-1"))

		// Ensure the correct entry has been deleted.
		_, err := store.GetFullGrafanaState(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		res, err := store.GetFullGrafanaState(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, state2, res)

		// Delete again (should be idempotent).
		require.NoError(t, store.DeleteGrafanaAlertConfig(ctx, "user-1"))
	}
}

func TestBucketAlertStore_GetSetDeleteGrafanaAlertConfig(t *testing.T) {
	bucket := objstore.NewInMemBucket()
	store := bucketclient.NewBucketAlertStore(bucketclient.BucketAlertStoreConfig{}, bucket, nil, log.NewNopLogger())

	ctx := context.Background()
	now := time.Now().UnixMilli()
	cfg1 := makeTestGrafanaAlertConfig(t, "user-1", "config one", "3edf15da6a1e11c454e7285d9443071a", now, false)
	cfg2 := makeTestGrafanaAlertConfig(t, "user-2", "config two", "b7aed2b102aa09fe21f324392ace74eb", now, false)

	// The storage is empty.
	{
		_, err := store.GetGrafanaAlertConfig(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		_, err = store.GetGrafanaAlertConfig(ctx, "user-2")
		assert.Equal(t, alertspb.ErrNotFound, err)
	}

	// The storage contains users.
	{
		require.NoError(t, store.SetGrafanaAlertConfig(ctx, cfg1))
		require.NoError(t, store.SetGrafanaAlertConfig(ctx, cfg2))

		res, err := store.GetGrafanaAlertConfig(ctx, "user-1")
		require.NoError(t, err)
		assert.Equal(t, cfg1, res)

		res, err = store.GetGrafanaAlertConfig(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, cfg2, res)

		// Ensure the config is stored at the expected location. Without this check
		// we have no guarantee that the objects are stored at the expected location.
		exists, err := bucket.Exists(ctx, "grafana_alertmanager/user-1/grafana_config")
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = bucket.Exists(ctx, "grafana_alertmanager/user-2/grafana_config")
		require.NoError(t, err)
		assert.True(t, exists)
	}

	// The storage has had user-1 deleted.
	{
		require.NoError(t, store.DeleteGrafanaAlertConfig(ctx, "user-1"))

		// Ensure the correct entry has been deleted.
		_, err := store.GetGrafanaAlertConfig(ctx, "user-1")
		assert.Equal(t, alertspb.ErrNotFound, err)

		res, err := store.GetGrafanaAlertConfig(ctx, "user-2")
		require.NoError(t, err)
		assert.Equal(t, cfg2, res)

		// Delete again (should be idempotent).
		require.NoError(t, store.DeleteGrafanaAlertConfig(ctx, "user-1"))
	}
}
