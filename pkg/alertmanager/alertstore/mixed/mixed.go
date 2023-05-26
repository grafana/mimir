package mixed

import (
	"context"
	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore"
)

func NewMixedStore(stateStore alertstore.AlertStore, configStore alertstore.AlertStore) alertstore.AlertStore {
	return &mixedStore{
		stateStore:  stateStore,
		configStore: configStore,
	}
}

type mixedStore struct {
	stateStore  alertstore.AlertStore
	configStore alertstore.AlertStore
}

// ListAllUsers implements alertstore.Alertstore.
func (store *mixedStore) ListAllUsers(ctx context.Context) ([]string, error) {
	return store.configStore.ListAllUsers(ctx)
}

// GetAlertConfigs implements alertstore.Alertstore.
func (store *mixedStore) GetAlertConfigs(ctx context.Context, userIDs []string) (map[string]alertspb.AlertConfigDesc, error) {
	return store.configStore.GetAlertConfigs(ctx, userIDs)
}

// GetAlertConfig implements alertstore.Alertstore.
func (store *mixedStore) GetAlertConfig(ctx context.Context, user string) (alertspb.AlertConfigDesc, error) {
	return store.configStore.GetAlertConfig(ctx, user)
}

// SetAlertConfig implements alertstore.Alertstore.
func (store *mixedStore) SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error {
	return store.configStore.SetAlertConfig(ctx, cfg)
}

// DeleteAlertConfig implements alertstore.Alertstore.
func (store *mixedStore) DeleteAlertConfig(ctx context.Context, user string) error {
	return store.configStore.DeleteAlertConfig(ctx, user)
}

// ListUsersWithFullState implements alertstore.Alertstore.
func (store *mixedStore) ListUsersWithFullState(ctx context.Context) ([]string, error) {
	return store.stateStore.ListUsersWithFullState(ctx)
}

// GetFullState implements alertstore.Alertstore.
func (store *mixedStore) GetFullState(ctx context.Context, user string) (alertspb.FullStateDesc, error) {
	return store.stateStore.GetFullState(ctx, user)
}

// SetFullState implements alertstore.Alertstore.
func (store *mixedStore) SetFullState(ctx context.Context, user string, fs alertspb.FullStateDesc) error {
	return store.stateStore.SetFullState(ctx, user, fs)
}

// DeleteFullState implements alertstore.Alertstore.
func (store *mixedStore) DeleteFullState(ctx context.Context, user string) error {
	return store.stateStore.DeleteFullState(ctx, user)
}
