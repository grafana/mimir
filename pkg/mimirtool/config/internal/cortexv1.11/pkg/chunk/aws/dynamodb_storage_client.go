// SPDX-License-Identifier: AGPL-3.0-only

package aws

import (
	"flag"
	"time"

	"github.com/grafana/dskit/backoff"

	"github.com/grafana/dskit/flagext"
)

type DynamoDBConfig struct {
	DynamoDB               flagext.URLValue         `yaml:"dynamodb_url"`
	APILimit               float64                  `yaml:"api_limit"`
	ThrottleLimit          float64                  `yaml:"throttle_limit"`
	Metrics                MetricsAutoScalingConfig `yaml:"metrics"`
	ChunkGangSize          int                      `yaml:"chunk_gang_size"`
	ChunkGetMaxParallelism int                      `yaml:"chunk_get_max_parallelism"`
	BackoffConfig          backoff.Config           `yaml:"backoff_config"`
}

type StorageConfig struct {
	DynamoDBConfig `yaml:"dynamodb"`
	S3Config       `yaml:",inline"`
}

const (
	hashKey  = "h"
	rangeKey = "r"
	valueKey = "c"

	// For dynamodb errors
	tableNameLabel   = "table"
	errorReasonLabel = "error"
	otherError       = "other"

	// See http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html.
	dynamoDBMaxWriteBatchSize = 25
	dynamoDBMaxReadBatchSize  = 100
	validationException       = "ValidationException"
)

func (cfg *DynamoDBConfig) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.DynamoDB, "dynamodb.url", "DynamoDB endpoint URL with escaped Key and Secret encoded. "+
		"If only region is specified as a host, proper endpoint will be deduced. Use inmemory:///<table-name> to use a mock in-memory implementation.")
	f.Float64Var(&cfg.APILimit, "dynamodb.api-limit", 2.0, "DynamoDB table management requests per second limit.")
	f.Float64Var(&cfg.ThrottleLimit, "dynamodb.throttle-limit", 10.0, "DynamoDB rate cap to back off when throttled.")
	f.IntVar(&cfg.ChunkGangSize, "dynamodb.chunk-gang-size", 10, "Number of chunks to group together to parallelise fetches (zero to disable)")
	f.IntVar(&cfg.ChunkGetMaxParallelism, "dynamodb.chunk.get-max-parallelism", 32, "Max number of chunk-get operations to start in parallel")
	f.DurationVar(&cfg.BackoffConfig.MinBackoff, "dynamodb.min-backoff", 100*time.Millisecond, "Minimum backoff time")
	f.DurationVar(&cfg.BackoffConfig.MaxBackoff, "dynamodb.max-backoff", 50*time.Second, "Maximum backoff time")
	f.IntVar(&cfg.BackoffConfig.MaxRetries, "dynamodb.max-retries", 20, "Maximum number of times to retry an operation")
	cfg.Metrics.RegisterFlags(f)
}
func (cfg *StorageConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.DynamoDBConfig.RegisterFlags(f)
	cfg.S3Config.RegisterFlags(f)
}
