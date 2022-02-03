// SPDX-License-Identifier: AGPL-3.0-only

package storage

import (
	"flag"
	"time"

	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/aws"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/azure"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/cache"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/cassandra"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/gcp"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/grpc"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/local"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/openstack"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/chunk/purger"
)

type Config struct {
	Engine                 string                  `yaml:"engine"`
	AWSStorageConfig       aws.StorageConfig       `yaml:"aws"`
	AzureStorageConfig     azure.BlobStorageConfig `yaml:"azure"`
	GCPStorageConfig       gcp.Config              `yaml:"bigtable"`
	GCSConfig              gcp.GCSConfig           `yaml:"gcs"`
	CassandraStorageConfig cassandra.Config        `yaml:"cassandra"`
	BoltDBConfig           local.BoltDBConfig      `yaml:"boltdb"`
	FSConfig               local.FSConfig          `yaml:"filesystem"`
	Swift                  openstack.SwiftConfig   `yaml:"swift"`

	IndexCacheValidity time.Duration `yaml:"index_cache_validity"`

	IndexQueriesCacheConfig cache.Config `yaml:"index_queries_cache_config"`

	DeleteStoreConfig purger.DeleteStoreConfig `yaml:"delete_store"`

	GrpcConfig grpc.Config `yaml:"grpc_store"`
}

const (
	StorageEngineChunks = "chunks"
	StorageEngineBlocks = "blocks"
)

// Supported storage clients
const (
	StorageTypeAWS            = "aws"
	StorageTypeAWSDynamo      = "aws-dynamo"
	StorageTypeAzure          = "azure"
	StorageTypeBoltDB         = "boltdb"
	StorageTypeCassandra      = "cassandra"
	StorageTypeInMemory       = "inmemory"
	StorageTypeBigTable       = "bigtable"
	StorageTypeBigTableHashed = "bigtable-hashed"
	StorageTypeFileSystem     = "filesystem"
	StorageTypeGCP            = "gcp"
	StorageTypeGCPColumnKey   = "gcp-columnkey"
	StorageTypeGCS            = "gcs"
	StorageTypeGrpc           = "grpc-store"
	StorageTypeS3             = "s3"
	StorageTypeSwift          = "swift"
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.AWSStorageConfig.RegisterFlags(f)
	cfg.AzureStorageConfig.RegisterFlags(f)
	cfg.GCPStorageConfig.RegisterFlags(f)
	cfg.GCSConfig.RegisterFlags(f)
	cfg.CassandraStorageConfig.RegisterFlags(f)
	cfg.BoltDBConfig.RegisterFlags(f)
	cfg.FSConfig.RegisterFlags(f)
	cfg.DeleteStoreConfig.RegisterFlags(f)
	cfg.Swift.RegisterFlags(f)
	cfg.GrpcConfig.RegisterFlags(f)

	f.StringVar(&cfg.Engine, "store.engine", "chunks", "The storage engine to use: chunks (deprecated) or blocks.")
	cfg.IndexQueriesCacheConfig.RegisterFlagsWithPrefix("store.index-cache-read.", "Cache config for index entry reading. ", f)
	f.DurationVar(&cfg.IndexCacheValidity, "store.index-cache-validity", 5*time.Minute, "Cache validity for active index entries. Should be no higher than -ingester.max-chunk-idle.")
}
