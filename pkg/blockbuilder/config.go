// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcclient"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

type Config struct {
	InstanceID string `yaml:"instance_id" doc:"default=<hostname>" category:"advanced"`
	DataDir    string `yaml:"data_dir"`

	SchedulerConfig SchedulerConfig `yaml:"scheduler_config" doc:"description=Configures block-builder-scheduler RPC communications."`

	ApplyMaxGlobalSeriesPerUserBelow int `yaml:"apply_max_global_series_per_user_below" category:"experimental"`

	// Config parameters defined outside the block-builder config and are injected dynamically.
	Kafka         ingest.KafkaConfig       `yaml:"-"`
	BlocksStorage tsdb.BlocksStorageConfig `yaml:"-"`
}

type SchedulerConfig struct {
	Address          string            `yaml:"address"`
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Configures the gRPC client used to communicate between the block-builders and block-builder-schedulers."`
	UpdateInterval   time.Duration     `yaml:"update_interval" doc:"description=Interval between scheduler updates."`
	MaxUpdateAge     time.Duration     `yaml:"max_update_age" doc:"description=Maximum age of jobs to continue sending to the scheduler."`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	f.StringVar(&cfg.InstanceID, "block-builder.instance-id", hostname, "Instance id.")
	f.StringVar(&cfg.DataDir, "block-builder.data-dir", "./data-block-builder/", "Directory to temporarily store blocks during building. This directory is wiped out between the restarts.")
	f.IntVar(&cfg.ApplyMaxGlobalSeriesPerUserBelow, "block-builder.apply-max-global-series-per-user-below", 0, "Apply the global series limit per partition if the global series limit for the user is <= this given value. 0 means limits are disabled. If a user's limit is more than the given value, then the limits are not applied as well.")

	cfg.SchedulerConfig.RegisterFlags(f)
}

func (cfg *SchedulerConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "block-builder.scheduler.address", "", "GRPC listen address of the block-builder-scheduler service.")
	f.DurationVar(&cfg.UpdateInterval, "block-builder.scheduler.update-interval", 20*time.Second, "Interval between scheduler updates.")
	f.DurationVar(&cfg.MaxUpdateAge, "block-builder.scheduler.max-update-age", 30*time.Minute, "Maximum age of jobs to continue sending to the scheduler.")
	cfg.GRPCClientConfig.RegisterFlagsWithPrefix("block-builder.scheduler.grpc-client-config", f)
}

func (cfg *Config) Validate() error {
	if err := cfg.Kafka.Validate(); err != nil {
		return fmt.Errorf("kafka: %w", err)
	}

	if err := cfg.SchedulerConfig.GRPCClientConfig.Validate(); err != nil {
		return fmt.Errorf("scheduler grpc config: %w", err)
	}

	if cfg.InstanceID == "" {
		return fmt.Errorf("instance id is required")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}

	return nil
}
