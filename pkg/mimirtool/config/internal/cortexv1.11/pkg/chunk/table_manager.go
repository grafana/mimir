// SPDX-License-Identifier: AGPL-3.0-only

package chunk

import (
	"flag"
	"time"

	"github.com/prometheus/common/model"
)

type TableManagerConfig struct {
	// Master 'off-switch' for table capacity updates, e.g. when troubleshooting
	ThroughputUpdatesDisabled bool `yaml:"throughput_updates_disabled"`

	// Master 'on-switch' for table retention deletions
	RetentionDeletesEnabled bool `yaml:"retention_deletes_enabled"`

	// How far back tables will be kept before they are deleted
	RetentionPeriod time.Duration `yaml:"-"`
	// This is so that we can accept 1w, 1y in the YAML.
	RetentionPeriodModel model.Duration `yaml:"retention_period"`

	// Period with which the table manager will poll for tables.
	PollInterval time.Duration `yaml:"poll_interval"`

	// duration a table will be created before it is needed.
	CreationGracePeriod time.Duration `yaml:"creation_grace_period"`

	IndexTables ProvisionConfig `yaml:"index_tables_provisioning"`
	ChunkTables ProvisionConfig `yaml:"chunk_tables_provisioning"`
}

const (
	readLabel  = "read"
	writeLabel = "write"

	bucketRetentionEnforcementInterval = 12 * time.Hour
)

func (cfg *TableManagerConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {

	// If we call unmarshal on TableManagerConfig, it will call UnmarshalYAML leading to infinite recursion.
	// To make unmarshal fill the plain data struct rather than calling UnmarshalYAML
	// again, we have to hide it using a type indirection.
	type plain TableManagerConfig
	if err := unmarshal((*plain)(cfg)); err != nil {
		return err
	}

	if cfg.RetentionPeriodModel > 0 {
		cfg.RetentionPeriod = time.Duration(cfg.RetentionPeriodModel)
	}

	return nil
}
func (cfg *TableManagerConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.ThroughputUpdatesDisabled, "table-manager.throughput-updates-disabled", false, "If true, disable all changes to DB capacity")
	f.BoolVar(&cfg.RetentionDeletesEnabled, "table-manager.retention-deletes-enabled", false, "If true, enables retention deletes of DB tables")
	f.Var(&cfg.RetentionPeriodModel, "table-manager.retention-period", "Tables older than this retention period are deleted. Must be either 0 (disabled) or a multiple of 24h. When enabled, be aware this setting is destructive to data!")
	f.DurationVar(&cfg.PollInterval, "table-manager.poll-interval", 2*time.Minute, "How frequently to poll backend to learn our capacity.")
	f.DurationVar(&cfg.CreationGracePeriod, "table-manager.periodic-table.grace-period", 10*time.Minute, "Periodic tables grace period (duration which table will be created/deleted before/after it's needed).")

	cfg.IndexTables.RegisterFlags("table-manager.index-table", f)
	cfg.ChunkTables.RegisterFlags("table-manager.chunk-table", f)
}
