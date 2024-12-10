// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertstore/store.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package alertstore

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/bucketclient"
	"github.com/grafana/mimir/pkg/alertmanager/alertstore/local"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

// AlertStore stores and configures users rule configs
type AlertStore interface {
	// ListAllUsers returns all users with alertmanager configuration.
	ListAllUsers(ctx context.Context) ([]string, error)

	// GetAlertConfigs loads and returns the alertmanager configuration for given users.
	// If any of the provided users has no configuration, then this function does not return an
	// error but the returned configs will not include the missing users.
	GetAlertConfigs(ctx context.Context, userIDs []string) (map[string]alertspb.AlertConfigDescs, error)

	// GetAlertConfig loads and returns the alertmanager configuration for the given user.
	GetAlertConfig(ctx context.Context, user string) (alertspb.AlertConfigDesc, error)

	// SetAlertConfig stores the alertmanager configuration for a user.
	SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error

	// DeleteAlertConfig deletes the alertmanager configuration for a user.
	// If configuration for the user doesn't exist, no error is reported.
	DeleteAlertConfig(ctx context.Context, user string) error

	// GetGrafanaAlertConfig returns the Grafana Alertmanager configuration for a user.
	GetGrafanaAlertConfig(ctx context.Context, user string) (alertspb.GrafanaAlertConfigDesc, error)

	// SetGrafanaAlertConfig stores the Grafana Alertmanager configuration for a user.
	SetGrafanaAlertConfig(ctx context.Context, cfg alertspb.GrafanaAlertConfigDesc) error

	// DeleteGrafanaAlertConfig delete the Grafana Alertmanager configuration for a user.
	// If configuration for the user doesn't exist, no error is reported.
	DeleteGrafanaAlertConfig(ctx context.Context, user string) error

	// GetFullGrafanaState returns the Grafana Alertmanager state for a user.
	GetFullGrafanaState(ctx context.Context, user string) (alertspb.FullStateDesc, error)

	// SetFullGrafanaState stores the Grafana Alertmanager state for a user.
	SetFullGrafanaState(ctx context.Context, user string, fs alertspb.FullStateDesc) error

	// DeleteFullGrafanaState delete the Grafana Alertmanager state for a user.
	// If state for the user doesn't exist, no error is reported.
	DeleteFullGrafanaState(ctx context.Context, user string) error

	// ListUsersWithFullState returns the list of users which have had state written.
	ListUsersWithFullState(ctx context.Context) ([]string, error)

	// GetFullState loads and returns the alertmanager state for the given user.
	GetFullState(ctx context.Context, user string) (alertspb.FullStateDesc, error)

	// SetFullState stores the alertmanager state for the given user.
	SetFullState(ctx context.Context, user string, fs alertspb.FullStateDesc) error

	// DeleteFullState deletes the alertmanager state for an user.
	// If state for the user doesn't exist, no error is reported.
	DeleteFullState(ctx context.Context, user string) error
}

// NewAlertStore returns a alertmanager store backend client based on the provided cfg.
func NewAlertStore(ctx context.Context, cfg Config, cfgProvider bucket.TenantConfigProvider, bucketConfig bucketclient.BucketAlertStoreConfig, logger log.Logger, reg prometheus.Registerer) (AlertStore, error) {
	if cfg.Backend == local.Name {
		level.Warn(logger).Log("msg", "-alertmanager-storage.backend=local is not suitable for persisting alertmanager state between replicas (silences, notifications); you should switch to an external object store for production use")
		return local.NewStore(cfg.Local)
	}

	if cfg.Backend == bucket.Filesystem {
		level.Warn(logger).Log("msg", "-alertmanager-storage.backend=filesystem is for development and testing only; you should switch to an external object store for production use or use a shared filesystem")
	}

	bucketClient, err := bucket.NewClient(ctx, cfg.Config, "alertmanager-storage", logger, reg)
	if err != nil {
		return nil, err
	}

	return bucketclient.NewBucketAlertStore(bucketConfig, bucketClient, cfgProvider, logger), nil
}
