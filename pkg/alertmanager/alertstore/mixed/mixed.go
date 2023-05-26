package mixed

import (
	"context"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/local"
)

func NewMixedStore(stateStore *bucketclient.BucketAlertStore, configStore *local.Store) *MixedStore {
	return &MixedStore{
		stateStore:  stateStore,
		configStore: configStore,
	}
}

type MixedStore struct {
	stateStore  *bucketclient.BucketAlertStore
	configStore *local.Store
}

// ListAllUsers implements alertstore.Alertstore.
func (store *MixedStore) ListAllUsers(ctx context.Context) ([]string, error) {
	return store.configStore.ListAllUsers(ctx)
}

// GetAlertConfigs implements alertstore.Alertstore.
func (store *MixedStore) GetAlertConfigs(ctx context.Context, userIDs []string) (map[string]alertspb.AlertConfigDesc, error) {
	return store.configStore.GetAlertConfigs(ctx, userIDs)
}

// GetAlertConfig implements alertstore.Alertstore.
func (store *MixedStore) GetAlertConfig(ctx context.Context, user string) (alertspb.AlertConfigDesc, error) {
	return store.configStore.GetAlertConfig(ctx, user)
}

// SetAlertConfig implements alertstore.Alertstore.
func (store *MixedStore) SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error {
	return store.configStore.SetAlertConfig(ctx, cfg)
}

// DeleteAlertConfig implements alertstore.Alertstore.
func (store *MixedStore) DeleteAlertConfig(ctx context.Context, user string) error {
	return store.configStore.DeleteAlertConfig(ctx, user)
}

// ListUsersWithFullState implements alertstore.Alertstore.
func (store *MixedStore) ListUsersWithFullState(ctx context.Context) ([]string, error) {
	return store.stateStore.ListUsersWithFullState(ctx)
}

// GetFullState implements alertstore.Alertstore.
func (store *MixedStore) GetFullState(ctx context.Context, user string) (alertspb.FullStateDesc, error) {
	return store.stateStore.GetFullState(ctx, user)
}

// SetFullState implements alertstore.Alertstore.
func (store *MixedStore) SetFullState(ctx context.Context, user string, fs alertspb.FullStateDesc) error {
	return store.stateStore.SetFullState(ctx, user, fs)
}

// DeleteFullState implements alertstore.Alertstore.
func (store *MixedStore) DeleteFullState(ctx context.Context, user string) error {
	return store.stateStore.DeleteFullState(ctx, user)
}
