// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

func TestCheckObjectStoresConfig(t *testing.T) {
	tests := map[string]struct {
		setup    func(cfg *Config)
		expected string
	}{
		"should succeed with the default config": {
			setup:    nil,
			expected: "",
		},
		"should succeed with the default config running Alertmanager along with target=all": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))
			},
			expected: "",
		},
		"should succeed with filesystem backend and non-existent directory (components create the dir at startup)": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.Filesystem
					bucketCfg.Filesystem.Directory = "/does/not/exists"
				}
			},
			expected: "",
		},
		"should check only blocks storage config when target=ingester": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("ingester"))

				// Configure alertmanager and ruler storage to fail, but expect to succeed.
				for _, bucketCfg := range []*bucket.Config{&cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.GCS
					bucketCfg.GCS.BucketName = "invalid"
				}
			},
			expected: "",
		},
		"should check only alertmanager storage config when target=alertmanager": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("alertmanager"))

				// Configure blocks storage and ruler storage to fail, but expect to succeed.
				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.GCS
					bucketCfg.GCS.BucketName = "invalid"
				}
			},
			expected: "",
		},
		"should check blocks and ruler storage config when target=ruler": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("ruler"))

				// Configure alertmanager storage to fail, but expect to succeed.
				cfg.AlertmanagerStorage.Config.Backend = bucket.GCS
				cfg.AlertmanagerStorage.Config.GCS.BucketName = "invalid"
			},
			expected: "",
		},
		"should fail on invalid AWS S3 config": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for i, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.S3
					bucketCfg.S3.Region = "us-east-1"
					bucketCfg.S3.Endpoint = "s3.dualstack.us-east-1.amazonaws.com"
					bucketCfg.S3.BucketName = "invalid"
					bucketCfg.S3.AccessKeyID = "xxx"
					bucketCfg.S3.SecretAccessKey = flagext.SecretWithValue("yyy")

					// Set a different bucket name for blocks storage to avoid config validation error.
					if i == 0 {
						bucketCfg.S3.BucketName = "invalid"
					} else {
						bucketCfg.S3.BucketName = "invalid-1"
					}
				}
			},
			expected: errObjectStorage,
		},
		"should fail on invalid GCS config": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for i, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.GCS

					// Set a different bucket name for blocks storage to avoid config validation error.
					if i == 0 {
						bucketCfg.GCS.BucketName = "invalid"
					} else {
						bucketCfg.GCS.BucketName = "invalid-1"
					}
				}
			},
			expected: errObjectStorage,
		},
		"should fail on invalid Azure config": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for i, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.Azure
					bucketCfg.Azure.ContainerName = "invalid"
					bucketCfg.Azure.StorageAccountName = ""
					bucketCfg.Azure.StorageAccountKey = flagext.SecretWithValue("eHh4")

					// Set a different container name for blocks storage to avoid config validation error.
					if i == 0 {
						bucketCfg.Azure.ContainerName = "invalid"
					} else {
						bucketCfg.Azure.ContainerName = "invalid-1"
					}
				}
			},
			expected: errObjectStorage,
		},
		"should fail on invalid Swift config": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for i, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.Swift
					bucketCfg.Swift.AuthURL = "http://127.0.0.1/"

					// Set a different project name for blocks storage to avoid config validation error.
					if i == 0 {
						bucketCfg.Swift.ProjectName = "invalid"
					} else {
						bucketCfg.Swift.ProjectName = "invalid-1"
					}
				}
			},
			expected: errObjectStorage,
		},
	}

	for testName, testData := range tests {
		// Change scope since we're running each test in parallel.
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			cfg := Config{}
			flagext.DefaultValues(&cfg)

			if testData.setup != nil {
				testData.setup(&cfg)
			}

			require.NoError(t, cfg.Validate(log.NewNopLogger()), "pre-condition: the config static validation should pass")

			actual := checkObjectStoresConfig(context.Background(), cfg, log.NewNopLogger())
			if testData.expected == "" {
				require.NoError(t, actual)
			} else {
				require.Error(t, actual)
				require.Contains(t, actual.Error(), testData.expected)
			}
		})
	}

}

func TestCheckDirectoryReadWriteAccess(t *testing.T) {
	const configuredPath = "/path/to/dir"

	configs := map[string]func(t *testing.T, cfg *Config){
		"ingester": func(t *testing.T, cfg *Config) {
			require.NoError(t, cfg.Target.Set("ingester"))
			cfg.Ingester.BlocksStorageConfig.TSDB.Dir = configuredPath
		},
		"store-gateway": func(t *testing.T, cfg *Config) {
			require.NoError(t, cfg.Target.Set("store-gateway"))
			cfg.BlocksStorage.BucketStore.SyncDir = configuredPath
		},
		"compactor": func(t *testing.T, cfg *Config) {
			require.NoError(t, cfg.Target.Set("compactor"))
			cfg.Compactor.DataDir = configuredPath
		},
		"ruler": func(t *testing.T, cfg *Config) {
			require.NoError(t, cfg.Target.Set("ruler"))
			cfg.Ruler.RulePath = configuredPath
		},
		"alertmanager": func(t *testing.T, cfg *Config) {
			require.NoError(t, cfg.Target.Set("alertmanager"))
			cfg.Alertmanager.DataDir = configuredPath
		},
	}

	tests := map[string]struct {
		dirExistsFn       dirExistsFunc
		isDirReadWritable isDirReadWritableFunc
		expected          string
	}{
		"should fail on directory without write access": {
			dirExistsFn: func(dir string) (bool, error) {
				return true, nil
			},
			isDirReadWritable: func(dir string) error {
				return errors.New("read only")
			},
			expected: fmt.Sprintf("failed to access directory %s: read only", configuredPath),
		},
		"should pass on directory with read-write access": {
			dirExistsFn: func(dir string) (bool, error) {
				return true, nil
			},
			isDirReadWritable: func(dir string) error {
				return nil
			},
			expected: "",
		},
		"should pass if directory doesn't exist but parent existing folder has read-write access": {
			dirExistsFn: func(dir string) (bool, error) {
				return dir == "/", nil
			},
			isDirReadWritable: func(dir string) error {
				if dir == "/" {
					return nil
				}
				return errors.New("not exists")
			},
			expected: "",
		},
		"should fail if directory doesn't exist and parent existing folder has no read-write access": {
			dirExistsFn: func(dir string) (bool, error) {
				return dir == "/", nil
			},
			isDirReadWritable: func(dir string) error {
				if dir == "/" {
					return errors.New("read only")
				}
				return errors.New("not exists")
			},
			expected: fmt.Sprintf("failed to access directory %s: read only", configuredPath),
		},
	}

	for configName, configSetup := range configs {
		t.Run(configName, func(t *testing.T) {
			for testName, testData := range tests {
				t.Run(testName, func(t *testing.T) {
					cfg := Config{}
					flagext.DefaultValues(&cfg)
					configSetup(t, &cfg)

					actual := checkDirectoriesReadWriteAccess(cfg, testData.dirExistsFn, testData.isDirReadWritable)
					if testData.expected == "" {
						require.NoError(t, actual)
					} else {
						require.Error(t, actual)
						require.Contains(t, actual.Error(), testData.expected)
					}
				})
			}
		})
	}
}

func TestCheckFilesystemPathsOvelapping(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)

	tests := map[string]struct {
		setup       func(cfg *Config)
		expectedErr string
	}{
		"should succeed with the default configuration": {
			setup: func(cfg *Config) {},
		},
		"should fail if alertmanager filesystem backend directory is equal to alertmanager data directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "/path/to/alertmanager"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/alertmanager/"
			},
			expectedErr: `the configured alertmanager data directory "/path/to/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager/"`,
		},
		"should fail if alertmanager filesystem backend directory is a subdirectory of alertmanager data directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "/path/to/alertmanager"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/alertmanager/subdir"
			},
			expectedErr: `the configured alertmanager data directory "/path/to/alertmanager" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager/subdir"`,
		},
		"should fail if alertmanager data directory is a subdirectory of alertmanager filesystem backend directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "/path/to/alertmanager/subdir"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/alertmanager"
			},
			expectedErr: `the configured alertmanager data directory "/path/to/alertmanager/subdir" cannot overlap with the configured alertmanager storage filesystem directory "/path/to/alertmanager"`,
		},
		"should fail if alertmanager data directory (relative) is a subdirectory of alertmanager filesystem backend directory (absolute)": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{AlertManager}
				cfg.Alertmanager.DataDir = "./data/subdir"
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.AlertmanagerStorage.Config.StorageBackendConfig.Filesystem.Directory = filepath.Join(cwd, "data")
			},
			expectedErr: fmt.Sprintf(`the configured alertmanager data directory "./data/subdir" cannot overlap with the configured alertmanager storage filesystem directory "%s"`, filepath.Join(cwd, "data")),
		},
		"should fail if ruler filesystem backend directory is equal to ruler data directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ruler}
				cfg.Ruler.RulePath = "/path/to/ruler"
				cfg.RulerStorage.Config.StorageBackendConfig.Backend = bucket.Filesystem
				cfg.RulerStorage.Config.StorageBackendConfig.Filesystem.Directory = "/path/to/ruler/"
			},
			expectedErr: `the configured ruler data directory "/path/to/ruler" cannot overlap with the configured ruler storage filesystem directory "/path/to/ruler/"`,
		},
		"should fail if store-gateway and compactor data directory overlap": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{StoreGateway, Compactor}
				cfg.BlocksStorage.BucketStore.SyncDir = "/path/to/data"
				cfg.Compactor.DataDir = "/path/to/data/compactor"
			},
			expectedErr: `the configured bucket store sync directory "/path/to/data" cannot overlap with the configured compactor data directory "/path/to/data/compactor"`,
		},
		"should succeed if store-gateway and compactor data directory overlap, but it's running only the store-gateway": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{StoreGateway}
				cfg.BlocksStorage.BucketStore.SyncDir = "/path/to/data"
				cfg.Compactor.DataDir = "/path/to/data/compactor"
			},
		},
		"should fail if tsdb directory and blocks storage filesystem directory overlap": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/path/to/data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/path/to/data/blocks"
			},
			expectedErr: `the configured blocks storage filesystem directory "/path/to/data/blocks" cannot overlap with the configured tsdb directory "/path/to/data"`,
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/path/to/data/tsdb"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/path/to/data/blocks"
			},
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap and one has the same prefix of the other one": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/path/to/data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/path/to/data-blocks"
			},
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap and they're both root directories": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "/data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "/data-blocks"
			},
		},
		"should succeed if tsdb directory and blocks storage filesystem directory don't overlap and they're both child of the same directory": {
			setup: func(cfg *Config) {
				cfg.Target = flagext.StringSliceCSV{Ingester}
				cfg.BlocksStorage.TSDB.Dir = "./data"
				cfg.BlocksStorage.Bucket.Backend = bucket.Filesystem
				cfg.BlocksStorage.Bucket.Filesystem.Directory = "./data-blocks"
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			testData.setup(&cfg)

			actualErr := checkFilesystemPathsOvelapping(cfg, log.NewNopLogger())

			if testData.expectedErr != "" {
				require.Error(t, actualErr)
				require.Contains(t, actualErr.Error(), testData.expectedErr)
			} else {
				require.NoError(t, actualErr)
			}
		})
	}
}

func TestIsAbsPathOverlapping(t *testing.T) {
	tests := []struct {
		first    string
		second   string
		expected bool
	}{
		{
			first:    "/",
			second:   "/",
			expected: true,
		},
		{
			first:    "/data",
			second:   "/",
			expected: true,
		},
		{
			first:    "/",
			second:   "/data",
			expected: true,
		},
		{
			first:    "/data",
			second:   "/data-more",
			expected: false,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/data",
			expected: true,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/data/more",
			expected: true,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/data-more",
			expected: false,
		},
		{
			first:    "/path/to/data",
			second:   "/path/to/more/data",
			expected: false,
		},
	}

	for _, testData := range tests {
		t.Run(fmt.Sprintf("check if %q overlaps %q", testData.first, testData.second), func(t *testing.T) {
			assert.Equal(t, testData.expected, isAbsPathOverlapping(testData.first, testData.second))
		})
	}
}
