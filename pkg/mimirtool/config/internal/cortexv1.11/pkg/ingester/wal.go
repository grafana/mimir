// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"flag"
	"time"
)

type WALConfig struct {
	WALEnabled         bool          `yaml:"wal_enabled"`
	CheckpointEnabled  bool          `yaml:"checkpoint_enabled"`
	Recover            bool          `yaml:"recover_from_wal"`
	Dir                string        `yaml:"wal_dir"`
	CheckpointDuration time.Duration `yaml:"checkpoint_duration"`
	FlushOnShutdown    bool          `yaml:"flush_on_shutdown_with_wal_enabled"`
	// We always checkpoint during shutdown. This option exists for the tests.
	checkpointDuringShutdown bool
}

const (
	// WALRecordSeries is the type for the WAL record on Prometheus TSDB record for series.
	WALRecordSeries RecordType = 1
	// WALRecordSamples is the type for the WAL record based on Prometheus TSDB record for samples.
	WALRecordSamples RecordType = 2

	// CheckpointRecord is the type for the Checkpoint record based on protos.
	CheckpointRecord RecordType = 3
)

type RecordType byte

func (cfg *WALConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Dir, "ingester.wal-dir", "wal", "Directory to store the WAL and/or recover from WAL.")
	f.BoolVar(&cfg.Recover, "ingester.recover-from-wal", false, "Recover data from existing WAL irrespective of WAL enabled/disabled.")
	f.BoolVar(&cfg.WALEnabled, "ingester.wal-enabled", false, "Enable writing of ingested data into WAL.")
	f.BoolVar(&cfg.CheckpointEnabled, "ingester.checkpoint-enabled", true, "Enable checkpointing of in-memory chunks. It should always be true when using normally. Set it to false iff you are doing some small tests as there is no mechanism to delete the old WAL yet if checkpoint is disabled.")
	f.DurationVar(&cfg.CheckpointDuration, "ingester.checkpoint-duration", 30*time.Minute, "Interval at which checkpoints should be created.")
	f.BoolVar(&cfg.FlushOnShutdown, "ingester.flush-on-shutdown-with-wal-enabled", false, "When WAL is enabled, should chunks be flushed to long-term storage on shutdown. Useful eg. for migration to blocks engine.")
	cfg.checkpointDuringShutdown = true
}
