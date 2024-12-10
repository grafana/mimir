// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/alertmanager/alertstore/bucketclient/bucket_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketclient

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/runutil"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
	"github.com/grafana/mimir/pkg/storage/bucket"
)

const (
	// AlertsPrefix is the bucket prefix under which all tenants alertmanager configs are stored.
	// Note that objects stored under this prefix follow the pattern:
	//     alerts/<user-id>
	AlertsPrefix = "alerts"

	// AlertmanagerPrefix is the bucket prefix under which other alertmanager state is stored.
	// Note that objects stored under this prefix follow the pattern:
	//     alertmanager/<user-id>/<object>
	AlertmanagerPrefix = "alertmanager"

	// GrafanaAlertmanagerPrefix is the bucket prefix under which Grafana's alertmanager configuration and state are stored.
	// Note that objects stored under this prefix follow the pattern:
	//		grafana_alertmanager/<user-id>/<object>
	GrafanaAlertmanagerPrefix = "grafana_alertmanager"

	// The name of alertmanager full state objects (notification log + silences).
	fullStateName = "fullstate"

	// How many users to load concurrently.
	fetchConcurrency = 16

	grafanaConfigName = "grafana_config"
	grafanaStateName  = "grafana_fullstate"
)

// BucketAlertStore is used to support the AlertStore interface against an object storage backend. It is implemented
// using the Thanos objstore.Bucket interface
type BucketAlertStore struct {
	alertsBucket    objstore.Bucket
	amBucket        objstore.Bucket
	grafanaAMBucket objstore.Bucket

	cfgProvider     bucket.TenantConfigProvider
	fetchGrafanaCfg bool
	logger          log.Logger
}

type BucketAlertStoreConfig struct {
	// Retrieve Grafana AM configs alongside Mimir AM configs.
	FetchGrafanaConfig bool
}

func NewBucketAlertStore(cfg BucketAlertStoreConfig, bkt objstore.Bucket, cfgProvider bucket.TenantConfigProvider, logger log.Logger) *BucketAlertStore {
	return &BucketAlertStore{
		alertsBucket:    bucket.NewPrefixedBucketClient(bkt, AlertsPrefix),
		amBucket:        bucket.NewPrefixedBucketClient(bkt, AlertmanagerPrefix),
		grafanaAMBucket: bucket.NewPrefixedBucketClient(bkt, GrafanaAlertmanagerPrefix),
		cfgProvider:     cfgProvider,
		fetchGrafanaCfg: cfg.FetchGrafanaConfig,
		logger:          logger,
	}
}

// ListAllUsers implements alertstore.AlertStore.
func (s *BucketAlertStore) ListAllUsers(ctx context.Context) ([]string, error) {
	userIDs := make(map[string]struct{})

	err := s.alertsBucket.Iter(ctx, "", func(key string) error {
		userIDs[key] = struct{}{}
		return nil
	})

	if s.fetchGrafanaCfg {
		err = s.grafanaAMBucket.Iter(ctx, "", func(key string) error {
			// Unlike standard configurations, for the Grafana bucket has a hierarchy per user.
			userIDs[strings.TrimRight(key, "/")] = struct{}{}
			return nil
		})
	}

	result := make([]string, 0, len(userIDs))
	for userID := range userIDs {
		result = append(result, userID)
	}

	return result, err
}

// GetAlertConfigs implements alertstore.AlertStore.
func (s *BucketAlertStore) GetAlertConfigs(ctx context.Context, userIDs []string) (map[string]alertspb.AlertConfigDescs, error) {
	var (
		cfgsMx = sync.Mutex{}
		cfgs   = make(map[string]alertspb.AlertConfigDescs, len(userIDs))
	)

	err := concurrency.ForEachJob(ctx, len(userIDs), fetchConcurrency, func(ctx context.Context, idx int) error {
		userID := userIDs[idx]
		var cfg alertspb.AlertConfigDescs

		mimirCfg, err := s.getAlertConfig(ctx, userID)
		if s.alertsBucket.IsObjNotFoundErr(err) {
			return nil
		} else if err != nil {
			return errors.Wrapf(err, "failed to fetch alertmanager config for user %s", userID)
		}
		cfg.Mimir = mimirCfg

		if s.fetchGrafanaCfg {
			grafanaCfg, err := s.getGrafanaAlertConfig(ctx, userID)
			// Users not having a Grafana alerting configuration is expected.
			if err != nil && !s.alertsBucket.IsObjNotFoundErr(err) {
				return errors.Wrapf(err, "failed to fetch grafana alertmanager config for user %s", userID)
			}
			cfg.Grafana = grafanaCfg
		}

		cfgsMx.Lock()
		cfgs[userID] = cfg
		cfgsMx.Unlock()

		return nil
	})

	return cfgs, err
}

// GetAlertConfig implements alertstore.AlertStore.
func (s *BucketAlertStore) GetAlertConfig(ctx context.Context, userID string) (alertspb.AlertConfigDesc, error) {
	cfg, err := s.getAlertConfig(ctx, userID)
	if s.alertsBucket.IsObjNotFoundErr(err) {
		return cfg, alertspb.ErrNotFound
	}

	return cfg, err
}

// SetAlertConfig implements alertstore.AlertStore.
func (s *BucketAlertStore) SetAlertConfig(ctx context.Context, cfg alertspb.AlertConfigDesc) error {
	cfgBytes, err := cfg.Marshal()
	if err != nil {
		return err
	}

	return s.getUserBucket(cfg.User).Upload(ctx, cfg.User, bytes.NewBuffer(cfgBytes))
}

// DeleteAlertConfig implements alertstore.AlertStore.
func (s *BucketAlertStore) DeleteAlertConfig(ctx context.Context, userID string) error {
	userBkt := s.getUserBucket(userID)

	err := userBkt.Delete(ctx, userID)
	if userBkt.IsObjNotFoundErr(err) {
		return nil
	}
	return err
}

func (s *BucketAlertStore) GetGrafanaAlertConfig(ctx context.Context, userID string) (alertspb.GrafanaAlertConfigDesc, error) {
	config, err := s.getGrafanaAlertConfig(ctx, userID)
	if s.grafanaAMBucket.IsObjNotFoundErr(err) {
		return config, alertspb.ErrNotFound
	}
	return config, err
}

func (s *BucketAlertStore) SetGrafanaAlertConfig(ctx context.Context, cfg alertspb.GrafanaAlertConfigDesc) error {
	cfgBytes, err := cfg.Marshal()
	if err != nil {
		return err
	}

	return s.getGrafanaAlertmanagerUserBucket(cfg.User).Upload(ctx, grafanaConfigName, bytes.NewBuffer(cfgBytes))
}

func (s *BucketAlertStore) DeleteGrafanaAlertConfig(ctx context.Context, userID string) error {
	userBkt := s.getGrafanaAlertmanagerUserBucket(userID)

	err := userBkt.Delete(ctx, grafanaConfigName)
	if userBkt.IsObjNotFoundErr(err) {
		return nil
	}

	return err
}

// ListUsersWithFullState implements alertstore.AlertStore.
func (s *BucketAlertStore) ListUsersWithFullState(ctx context.Context) ([]string, error) {
	var userIDs []string

	err := s.amBucket.Iter(ctx, "", func(key string) error {
		userIDs = append(userIDs, strings.TrimRight(key, "/"))
		return nil
	})

	return userIDs, err
}

// GetFullState implements alertstore.AlertStore.
func (s *BucketAlertStore) GetFullState(ctx context.Context, userID string) (alertspb.FullStateDesc, error) {
	bkt := s.getAlertmanagerUserBucket(userID)
	fs := alertspb.FullStateDesc{}

	err := s.get(ctx, bkt, fullStateName, &fs)
	if s.amBucket.IsObjNotFoundErr(err) {
		return fs, alertspb.ErrNotFound
	}

	return fs, err
}

// SetFullState implements alertstore.AlertStore.
func (s *BucketAlertStore) SetFullState(ctx context.Context, userID string, fs alertspb.FullStateDesc) error {
	bkt := s.getAlertmanagerUserBucket(userID)

	fsBytes, err := fs.Marshal()
	if err != nil {
		return err
	}

	return bkt.Upload(ctx, fullStateName, bytes.NewBuffer(fsBytes))
}

// DeleteFullState implements alertstore.AlertStore.
func (s *BucketAlertStore) DeleteFullState(ctx context.Context, userID string) error {
	userBkt := s.getAlertmanagerUserBucket(userID)

	err := userBkt.Delete(ctx, fullStateName)
	if userBkt.IsObjNotFoundErr(err) {
		return nil
	}
	return err
}

func (s *BucketAlertStore) GetFullGrafanaState(ctx context.Context, userID string) (alertspb.FullStateDesc, error) {
	bkt := s.getGrafanaAlertmanagerUserBucket(userID)
	fs := alertspb.FullStateDesc{}

	err := s.get(ctx, bkt, grafanaStateName, &fs)
	if s.grafanaAMBucket.IsObjNotFoundErr(err) {
		return fs, alertspb.ErrNotFound
	}

	return fs, err
}

func (s *BucketAlertStore) SetFullGrafanaState(ctx context.Context, userID string, fs alertspb.FullStateDesc) error {
	bkt := s.getGrafanaAlertmanagerUserBucket(userID)

	fsBytes, err := fs.Marshal()
	if err != nil {
		return err
	}

	return bkt.Upload(ctx, grafanaStateName, bytes.NewBuffer(fsBytes))
}

func (s *BucketAlertStore) DeleteFullGrafanaState(ctx context.Context, userID string) error {
	bkt := s.getGrafanaAlertmanagerUserBucket(userID)

	err := bkt.Delete(ctx, grafanaStateName)
	if bkt.IsObjNotFoundErr(err) {
		return nil
	}

	return err
}

func (s *BucketAlertStore) getAlertConfig(ctx context.Context, userID string) (alertspb.AlertConfigDesc, error) {
	config := alertspb.AlertConfigDesc{}
	err := s.get(ctx, s.getUserBucket(userID), userID, &config)
	return config, err
}

func (s *BucketAlertStore) getGrafanaAlertConfig(ctx context.Context, userID string) (alertspb.GrafanaAlertConfigDesc, error) {
	config := alertspb.GrafanaAlertConfigDesc{}
	err := s.get(ctx, s.getGrafanaAlertmanagerUserBucket(userID), grafanaConfigName, &config)
	return config, err
}

func (s *BucketAlertStore) get(ctx context.Context, bkt objstore.Bucket, name string, msg proto.Message) error {
	readCloser, err := bkt.Get(ctx, name)
	if err != nil {
		return err
	}

	defer runutil.CloseWithLogOnErr(s.logger, readCloser, "close bucket reader")

	buf, err := io.ReadAll(readCloser)
	if err != nil {
		return errors.Wrapf(err, "failed to read alertmanager config for user %s", name)
	}

	err = proto.Unmarshal(buf, msg)
	if err != nil {
		return errors.Wrapf(err, "failed to deserialize alertmanager config for user %s", name)
	}

	return nil
}

func (s *BucketAlertStore) getUserBucket(userID string) objstore.Bucket {
	// Inject server-side encryption based on the tenant config.
	return bucket.NewSSEBucketClient(userID, s.alertsBucket, s.cfgProvider)
}

func (s *BucketAlertStore) getAlertmanagerUserBucket(userID string) objstore.Bucket {
	return bucket.NewUserBucketClient(userID, s.amBucket, s.cfgProvider).WithExpectedErrs(s.amBucket.IsObjNotFoundErr)
}

func (s *BucketAlertStore) getGrafanaAlertmanagerUserBucket(userID string) objstore.Bucket {
	return bucket.NewUserBucketClient(userID, s.grafanaAMBucket, s.cfgProvider).WithExpectedErrs(s.amBucket.IsObjNotFoundErr)
}
