// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"flag"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/aws"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/azure"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/gcp"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/openstack"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/configs/client"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/ruler/rulestore/local"
)

type RuleStoreConfig struct {
	Type     string        `yaml:"type"`
	ConfigDB client.Config `yaml:"configdb"`

	// Object Storage Configs
	Azure azure.BlobStorageConfig `yaml:"azure"`
	GCS   gcp.GCSConfig           `yaml:"gcs"`
	S3    aws.S3Config            `yaml:"s3"`
	Swift openstack.SwiftConfig   `yaml:"swift"`
	Local local.Config            `yaml:"local"`
}

func (cfg *RuleStoreConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.ConfigDB.RegisterFlagsWithPrefix("ruler.", f)
	cfg.Azure.RegisterFlagsWithPrefix("ruler.storage.", f)
	cfg.GCS.RegisterFlagsWithPrefix("ruler.storage.", f)
	cfg.S3.RegisterFlagsWithPrefix("ruler.storage.", f)
	cfg.Swift.RegisterFlagsWithPrefix("ruler.storage.", f)
	cfg.Local.RegisterFlagsWithPrefix("ruler.storage.", f)

	f.StringVar(&cfg.Type, "ruler.storage.type", "configdb", "Method to use for backend rule storage (configdb, azure, gcs, s3, swift, local)")
}
