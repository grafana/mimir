// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertstore/local/store.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package local

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/alertmanager/config"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

const (
	Name = "local"
)

var (
	errReadOnly              = errors.New("local alertmanager config storage is read-only")
	errState                 = errors.New("local alertmanager storage does not support state persistency")
	errGrafanaStateAndConfig = errors.New("local alertmanager storage does not support Grafana configuration endpoints")
)

// StoreConfig configures a static file alertmanager store
type StoreConfig struct {
	Path string `yaml:"path"`
}

// RegisterFlags registers flags related to the alertmanager local storage.
func (cfg *StoreConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Path, prefix+"local.path", "", "Path at which alertmanager configurations are stored.")
}

// Store is used to load user alertmanager configs from a local disk
type Store struct {
	cfg StoreConfig
}

func (f *Store) GetFullGrafanaState(_ context.Context, _ string) (alertspb.FullStateDesc, error) {
	return alertspb.FullStateDesc{}, errGrafanaStateAndConfig
}

func (f *Store) SetFullGrafanaState(_ context.Context, _ string, _ alertspb.FullStateDesc) error {
	return errGrafanaStateAndConfig
}

func (f *Store) DeleteFullGrafanaState(_ context.Context, _ string) error {
	return errGrafanaStateAndConfig
}

// NewStore returns a new file alert store.
func NewStore(cfg StoreConfig) (*Store, error) {
	return &Store{cfg}, nil
}

// ListAllUsers implements alertstore.AlertStore.
func (f *Store) ListAllUsers(_ context.Context) ([]string, error) {
	configs, err := f.reloadConfigs()
	if err != nil {
		return nil, err
	}

	userIDs := make([]string, 0, len(configs))
	for userID := range configs {
		userIDs = append(userIDs, userID)
	}

	return userIDs, nil
}

// GetAlertConfigs implements alertstore.AlertStore.
func (f *Store) GetAlertConfigs(_ context.Context, userIDs []string) (map[string]alertspb.AlertConfigDescs, error) {
	configs, err := f.reloadConfigs()
	if err != nil {
		return nil, err
	}

	filtered := make(map[string]alertspb.AlertConfigDescs, len(userIDs))
	for _, userID := range userIDs {
		if cfg, ok := configs[userID]; ok {
			filtered[userID] = alertspb.AlertConfigDescs{Mimir: cfg}
		}
	}

	return filtered, nil
}

// GetAlertConfig implements alertstore.AlertStore.
func (f *Store) GetAlertConfig(_ context.Context, user string) (alertspb.AlertConfigDesc, error) {
	cfgs, err := f.reloadConfigs()
	if err != nil {
		return alertspb.AlertConfigDesc{}, err
	}

	cfg, exists := cfgs[user]

	if !exists {
		return alertspb.AlertConfigDesc{}, alertspb.ErrNotFound
	}

	return cfg, nil
}

// SetAlertConfig implements alertstore.AlertStore.
func (f *Store) SetAlertConfig(_ context.Context, _ alertspb.AlertConfigDesc) error {
	return errReadOnly
}

// DeleteAlertConfig implements alertstore.AlertStore.
func (f *Store) DeleteAlertConfig(_ context.Context, _ string) error {
	return errReadOnly
}

func (f *Store) GetGrafanaAlertConfig(_ context.Context, _ string) (alertspb.GrafanaAlertConfigDesc, error) {
	return alertspb.GrafanaAlertConfigDesc{}, errGrafanaStateAndConfig
}

func (f *Store) SetGrafanaAlertConfig(_ context.Context, _ alertspb.GrafanaAlertConfigDesc) error {
	return errGrafanaStateAndConfig
}

func (f *Store) DeleteGrafanaAlertConfig(_ context.Context, _ string) error {
	return errGrafanaStateAndConfig
}

// ListUsersWithFullState implements alertstore.AlertStore.
func (f *Store) ListUsersWithFullState(_ context.Context) ([]string, error) {
	return []string{}, nil
}

// GetFullState implements alertstore.AlertStore.
func (f *Store) GetFullState(_ context.Context, _ string) (alertspb.FullStateDesc, error) {
	return alertspb.FullStateDesc{}, alertspb.ErrNotFound
}

// SetFullState implements alertstore.AlertStore.
func (f *Store) SetFullState(_ context.Context, _ string, _ alertspb.FullStateDesc) error {
	return errState
}

// DeleteFullState implements alertstore.AlertStore.
func (f *Store) DeleteFullState(_ context.Context, _ string) error {
	return errState
}

func (f *Store) reloadConfigs() (map[string]alertspb.AlertConfigDesc, error) {
	configs := map[string]alertspb.AlertConfigDesc{}
	err := filepath.Walk(f.cfg.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Wrapf(err, "unable to walk file path at %s", path)
		}

		// Ignore files that are directories or not yaml files
		ext := filepath.Ext(info.Name())
		if info.IsDir() || (ext != ".yml" && ext != ".yaml") {
			return nil
		}

		// Ensure the file is a valid Alertmanager Config.
		_, err = config.LoadFile(path)
		if err != nil {
			return errors.Wrapf(err, "unable to load alertmanager config %s", path)
		}

		// Load the file to be returned by the store.
		content, err := os.ReadFile(path)
		if err != nil {
			return errors.Wrapf(err, "unable to read alertmanager config %s", path)
		}

		// The file name must correspond to the user tenant ID
		user := strings.TrimSuffix(info.Name(), ext)

		configs[user] = alertspb.AlertConfigDesc{
			User:      user,
			RawConfig: string(content),
		}
		return nil
	})

	return configs, err
}
