// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
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

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.S3
					bucketCfg.S3.Region = "us-east-1"
					bucketCfg.S3.Endpoint = "s3.dualstack.us-east-1.amazonaws.com"
					bucketCfg.S3.BucketName = "invalid"
					bucketCfg.S3.AccessKeyID = "xxx"
					bucketCfg.S3.SecretAccessKey = flagext.Secret{Value: "yyy"}
				}
			},
			expected: errObjectStorage,
		},
		"should fail on invalid GCS config": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.GCS
					bucketCfg.GCS.BucketName = "invalid"
				}
			},
			expected: errObjectStorage,
		},
		"should fail on invalid Azure config": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.Azure
					bucketCfg.Azure.ContainerName = "invalid"
					bucketCfg.Azure.StorageAccountName = "xxx"
					bucketCfg.Azure.StorageAccountKey = flagext.Secret{Value: "eHh4"}
				}
			},
			expected: errObjectStorage,
		},
		"should fail on invalid Swift config": {
			setup: func(cfg *Config) {
				require.NoError(t, cfg.Target.Set("all,alertmanager"))

				for _, bucketCfg := range []*bucket.Config{&cfg.BlocksStorage.Bucket, &cfg.AlertmanagerStorage.Config, &cfg.RulerStorage.Config} {
					bucketCfg.Backend = bucket.Swift
					bucketCfg.Swift.AuthURL = "http://127.0.0.1/"
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
