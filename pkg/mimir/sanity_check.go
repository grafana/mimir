// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/runutil"

	alertstorelocal "github.com/grafana/mimir/pkg/alertmanager/alertstore/local"
	rulestorelocal "github.com/grafana/mimir/pkg/ruler/rulestore/local"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util/fs"
)

var (
	errObjectStorage = "unable to successfully send a request to object storage"
)

type dirExistsFunc func(string) (bool, error)
type isDirReadWritableFunc func(dir string) error

func runSanityCheck(ctx context.Context, cfg Config, logger log.Logger) error {
	level.Info(logger).Log("msg", "Checking configured filesystem paths")
	if err := checkFilesystemPathsOvelapping(cfg, logger); err != nil {
		level.Error(logger).Log("msg", err.Error())
		return err
	}
	level.Info(logger).Log("msg", "Configured filesystem paths successfully checked")

	level.Info(logger).Log("msg", "Checking directories read/write access")
	if err := checkDirectoriesReadWriteAccess(cfg, fs.DirExists, fs.IsDirReadWritable); err != nil {
		level.Error(logger).Log("msg", "Unable to access directory", "err", err)
		return err
	}
	level.Info(logger).Log("msg", "Directories read/write access successfully checked")

	level.Info(logger).Log("msg", "Checking object storage config")
	if err := checkObjectStoresConfigWithRetries(ctx, cfg, logger); err != nil {
		level.Error(logger).Log("msg", "Unable to successfully connect to configured object storage", "err", err)
		return err
	}
	level.Info(logger).Log("msg", "Object storage config successfully checked")

	return nil
}

func checkDirectoriesReadWriteAccess(
	cfg Config,
	dirExistFn dirExistsFunc,
	isDirReadWritableFn isDirReadWritableFunc,
) error {
	errs := multierror.New()

	if cfg.isAnyModuleEnabled(All, Ingester, Write) {
		errs.Add(errors.Wrap(checkDirReadWriteAccess(cfg.Ingester.BlocksStorageConfig.TSDB.Dir, dirExistFn, isDirReadWritableFn), "ingester"))
	}
	if cfg.isAnyModuleEnabled(All, StoreGateway, Backend) {
		errs.Add(errors.Wrap(checkDirReadWriteAccess(cfg.BlocksStorage.BucketStore.SyncDir, dirExistFn, isDirReadWritableFn), "store-gateway"))
	}
	if cfg.isAnyModuleEnabled(All, Compactor, Backend) {
		errs.Add(errors.Wrap(checkDirReadWriteAccess(cfg.Compactor.DataDir, dirExistFn, isDirReadWritableFn), "compactor"))
	}
	if cfg.isAnyModuleEnabled(All, Ruler, Backend) {
		errs.Add(errors.Wrap(checkDirReadWriteAccess(cfg.Ruler.RulePath, dirExistFn, isDirReadWritableFn), "ruler"))
	}
	if cfg.isAnyModuleEnabled(AlertManager, Backend) {
		errs.Add(errors.Wrap(checkDirReadWriteAccess(cfg.Alertmanager.DataDir, dirExistFn, isDirReadWritableFn), "alertmanager"))
	}

	return errs.Err()
}

func checkDirReadWriteAccess(dir string, dirExistFn dirExistsFunc, isDirReadWritableFn isDirReadWritableFunc) error {
	d := dir
	for {
		exists, err := dirExistFn(d)
		if err != nil {
			return err
		}
		if exists {
			break
		}

		dNext := filepath.Dir(d)
		if d == dNext {
			break // Root path reached.
		}
		d = dNext
	}

	if err := isDirReadWritableFn(d); err != nil {
		return fmt.Errorf("failed to access directory %s: %w", dir, err)
	}
	return nil
}

func checkObjectStoresConfigWithRetries(ctx context.Context, cfg Config, logger log.Logger) (err error) {
	const maxRetries = 20

	backoff := backoff.New(ctx, backoff.Config{
		MinBackoff: time.Second,
		MaxBackoff: 5 * time.Second,
		MaxRetries: maxRetries,
	})

	for backoff.Ongoing() {
		err = checkObjectStoresConfig(ctx, cfg, logger)
		if err == nil {
			return nil
		}

		if backoff.NumRetries() < maxRetries-1 {
			level.Warn(logger).Log("msg", "Unable to successfully connect to configured object storage (will retry)", "err", err)
		} else {
			level.Warn(logger).Log("msg", "Unable to successfully connect to configured object storage", "err", err)
		}

		backoff.Wait()
	}

	return err
}

func checkObjectStoresConfig(ctx context.Context, cfg Config, logger log.Logger) error {
	errs := multierror.New()

	// Check blocks storage config only if running at least one component using it.
	if cfg.isAnyModuleEnabled(All, Ingester, Querier, Ruler, StoreGateway, Compactor, Write, Read, Backend) {
		errs.Add(errors.Wrap(checkObjectStoreConfig(ctx, cfg.BlocksStorage.Bucket, logger), "blocks storage"))
	}

	// Check alertmanager storage config.
	if cfg.isAnyModuleEnabled(AlertManager, Backend) && cfg.AlertmanagerStorage.Backend != alertstorelocal.Name {
		errs.Add(errors.Wrap(checkObjectStoreConfig(ctx, cfg.AlertmanagerStorage.Config, logger), "alertmanager storage"))
	}

	// Check ruler storage config.
	if cfg.isAnyModuleEnabled(All, Ruler, Backend) && cfg.RulerStorage.Backend != rulestorelocal.Name {
		errs.Add(errors.Wrap(checkObjectStoreConfig(ctx, cfg.RulerStorage.Config, logger), "ruler storage"))
	}

	return errs.Err()
}

func checkObjectStoreConfig(ctx context.Context, cfg bucket.Config, logger log.Logger) error {
	// Hardcoded but relatively high timeout.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client, err := bucket.NewClient(ctx, cfg, "sanity-check", logger, nil)
	if err != nil {
		return errors.Wrap(err, errObjectStorage)
	}

	defer func() {
		// Ensure the client gets closed. Ignore the returned error here.
		_ = client.Close()
	}()

	// Read an object we don't expect to exist, just to check if the object storage
	// replies with the expected error.
	reader, err := client.Get(ctx, "sanity-check-at-startup")
	if err != nil {
		if client.IsObjNotFoundErr(err) {
			return nil
		}
		return errors.Wrap(err, errObjectStorage)
	}

	runutil.ExhaustCloseWithErrCapture(&err, reader, "error while reading from object store")
	return err
}

// checkFilesystemPathsOvelapping checks all configured filesystem paths and return error if it finds two of them
// overlapping (Mimir expects all filesystem paths to not overlap).
func checkFilesystemPathsOvelapping(cfg Config, logger log.Logger) error {
	type pathConfig struct {
		name     string
		value    string
		absValue string
	}

	var paths []pathConfig

	if cfg.BlocksStorage.Bucket.Backend == bucket.Filesystem {
		paths = append(paths, pathConfig{name: "blocks storage filesystem directory", value: cfg.BlocksStorage.Bucket.Filesystem.Directory})
	}

	// Ingester.
	if cfg.isAnyModuleEnabled(All, Ingester, Write) {
		paths = append(paths, pathConfig{name: "tsdb directory", value: cfg.BlocksStorage.TSDB.Dir})
	}

	// Store-gateway.
	if cfg.isAnyModuleEnabled(All, StoreGateway, Backend) {
		paths = append(paths, pathConfig{name: "bucket store sync directory", value: cfg.BlocksStorage.BucketStore.SyncDir})
	}

	// Compactor.
	if cfg.isAnyModuleEnabled(All, Compactor, Backend) {
		paths = append(paths, pathConfig{name: "compactor data directory", value: cfg.Compactor.DataDir})
	}

	// Ruler.
	if cfg.isAnyModuleEnabled(All, Ruler, Backend) {
		paths = append(paths, pathConfig{name: "ruler data directory", value: cfg.Ruler.RulePath})

		if cfg.RulerStorage.Backend == bucket.Filesystem {
			paths = append(paths, pathConfig{name: "ruler storage filesystem directory", value: cfg.RulerStorage.Filesystem.Directory})
		}
		if cfg.RulerStorage.Backend == rulestorelocal.Name {
			paths = append(paths, pathConfig{name: "ruler storage local directory", value: cfg.RulerStorage.Local.Directory})
		}
	}

	// Alertmanager.
	if cfg.isAnyModuleEnabled(AlertManager) {
		paths = append(paths, pathConfig{name: "alertmanager data directory", value: cfg.Alertmanager.DataDir})

		if cfg.AlertmanagerStorage.Backend == bucket.Filesystem {
			paths = append(paths, pathConfig{name: "alertmanager storage filesystem directory", value: cfg.AlertmanagerStorage.Filesystem.Directory})
		}

		if cfg.AlertmanagerStorage.Backend == alertstorelocal.Name {
			paths = append(paths, pathConfig{name: "alertmanager storage local directory", value: cfg.AlertmanagerStorage.Local.Path})
		}
	}

	// Convert all configured paths to absolute clean paths.
	for idx, path := range paths {
		abs, err := filepath.Abs(path.value)
		if err != nil {
			// We prefer to log a warning instead of returning an error to ensure that if we're unable to
			// run the sanity check Mimir could start anyway.
			level.Warn(logger).Log("msg", "the configuration sanity check for the filesystem directory has been skipped because can't get the absolute path", "path", path, "err", err)
			continue
		}

		paths[idx].absValue = abs
	}

	for _, firstPath := range paths {
		for _, secondPath := range paths {
			// Skip the same config field.
			if firstPath.name == secondPath.name {
				continue
			}

			// Skip if we've been unable to get the absolute path of one of the two paths.
			if firstPath.absValue == "" || secondPath.absValue == "" {
				continue
			}

			if isAbsPathOverlapping(firstPath.absValue, secondPath.absValue) {
				return fmt.Errorf("the configured %s %q cannot overlap with the configured %s %q; please set different paths, also ensuring one is not a subdirectory of the other one", firstPath.name, firstPath.value, secondPath.name, secondPath.value)
			}
		}
	}

	return nil
}

// isAbsPathOverlapping returns whether the two input absolute paths overlap.
func isAbsPathOverlapping(firstAbsPath, secondAbsPath string) bool {
	firstBase, firstName := filepath.Split(firstAbsPath)
	secondBase, secondName := filepath.Split(secondAbsPath)

	if firstBase == secondBase {
		// The base directories are the same, so they overlap if the last segment of the path (name)
		// is the same or it's missing (happens when the input path is the root "/").
		return firstName == secondName || firstName == "" || secondName == ""
	}

	// The base directories are different, but they could still overlap if one is the child of the other one.
	return strings.HasPrefix(firstAbsPath, secondAbsPath) || strings.HasPrefix(secondAbsPath, firstAbsPath)
}
