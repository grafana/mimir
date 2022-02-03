// SPDX-License-Identifier: AGPL-3.0-only

package alertstore

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/alertmanager/alertstore/configdb"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/alertmanager/alertstore/local"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/aws"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/azure"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/gcp"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/configs/client"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/bucket"
)

type LegacyConfig struct {
	Type     string        `yaml:"type"`
	ConfigDB client.Config `yaml:"configdb"`

	// Object Storage Configs
	Azure azure.BlobStorageConfig `yaml:"azure"`
	GCS   gcp.GCSConfig           `yaml:"gcs"`
	S3    aws.S3Config            `yaml:"s3"`
	Local local.StoreConfig       `yaml:"local"`
}

type Config struct {
	bucket.Config `yaml:",inline"`
	ConfigDB      client.Config     `yaml:"configdb"`
	Local         local.StoreConfig `yaml:"local"`
}

func (cfg *LegacyConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("alertmanager.", f)
	f.StringVar(&cfg.Type, "alertmanager.storage.type", configdb.Name, "Type of backend to use to store alertmanager configs. Supported values are: \"configdb\", \"gcs\", \"s3\", \"local\".")

	cfg.Azure.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.GCS.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.S3.RegisterFlagsWithPrefix("alertmanager.storage.", f)
	cfg.Local.RegisterFlagsWithPrefix("alertmanager.storage.", f)
}
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	prefix := "alertmanager-storage."

	cfg.ExtraBackends = []string{configdb.Name, local.Name}
	cfg.ConfigDB.RegisterFlagsWithPrefix(prefix, f)
	cfg.Local.RegisterFlagsWithPrefix(prefix, f)
	cfg.RegisterFlagsWithPrefix(prefix, f)
}
