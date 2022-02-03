// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"flag"
	"fmt"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/auxiliary/compact"

	cortex_tsdb "github.com/grafana/mimir/pkg/mimirtool/config/internal/cortexv1.11/pkg/storage/tsdb"
)

type Config struct {
	BlockRanges           cortex_tsdb.DurationList `yaml:"block_ranges"`
	BlockSyncConcurrency  int                      `yaml:"block_sync_concurrency"`
	MetaSyncConcurrency   int                      `yaml:"meta_sync_concurrency"`
	ConsistencyDelay      time.Duration            `yaml:"consistency_delay"`
	DataDir               string                   `yaml:"data_dir"`
	CompactionInterval    time.Duration            `yaml:"compaction_interval"`
	CompactionRetries     int                      `yaml:"compaction_retries"`
	CompactionConcurrency int                      `yaml:"compaction_concurrency"`
	CleanupInterval       time.Duration            `yaml:"cleanup_interval"`
	CleanupConcurrency    int                      `yaml:"cleanup_concurrency"`
	DeletionDelay         time.Duration            `yaml:"deletion_delay"`
	TenantCleanupDelay    time.Duration            `yaml:"tenant_cleanup_delay"`

	// Whether the migration of block deletion marks to the global markers location is enabled.
	BlockDeletionMarksMigrationEnabled bool `yaml:"block_deletion_marks_migration_enabled"`

	EnabledTenants  flagext.StringSliceCSV `yaml:"enabled_tenants"`
	DisabledTenants flagext.StringSliceCSV `yaml:"disabled_tenants"`

	// Compactors sharding.
	ShardingEnabled bool       `yaml:"sharding_enabled"`
	ShardingRing    RingConfig `yaml:"sharding_ring"`

	// No need to add options to customize the retry backoff,
	// given the defaults should be fine, but allow to override
	// it in tests.
	retryMinBackoff time.Duration `yaml:"-"`
	retryMaxBackoff time.Duration `yaml:"-"`

	// Allow downstream projects to customise the blocks compactor.
}

const (
	blocksMarkedForDeletionName = "cortex_compactor_blocks_marked_for_deletion_total"
	blocksMarkedForDeletionHelp = "Total number of blocks marked for deletion in compactor."
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.ShardingRing.RegisterFlags(f)

	cfg.BlockRanges = cortex_tsdb.DurationList{2 * time.Hour, 12 * time.Hour, 24 * time.Hour}
	cfg.retryMinBackoff = 10 * time.Second
	cfg.retryMaxBackoff = time.Minute

	f.Var(&cfg.BlockRanges, "compactor.block-ranges", "List of compaction time ranges.")
	f.DurationVar(&cfg.ConsistencyDelay, "compactor.consistency-delay", 0, fmt.Sprintf("Minimum age of fresh (non-compacted) blocks before they are being processed. Malformed blocks older than the maximum of consistency-delay and %s will be removed.", compact.PartialUploadThresholdAge))
	f.IntVar(&cfg.BlockSyncConcurrency, "compactor.block-sync-concurrency", 20, "Number of Go routines to use when syncing block index and chunks files from the long term storage.")
	f.IntVar(&cfg.MetaSyncConcurrency, "compactor.meta-sync-concurrency", 20, "Number of Go routines to use when syncing block meta files from the long term storage.")
	f.StringVar(&cfg.DataDir, "compactor.data-dir", "./data", "Data directory in which to cache blocks and process compactions")
	f.DurationVar(&cfg.CompactionInterval, "compactor.compaction-interval", time.Hour, "The frequency at which the compaction runs")
	f.IntVar(&cfg.CompactionRetries, "compactor.compaction-retries", 3, "How many times to retry a failed compaction within a single compaction run.")
	f.IntVar(&cfg.CompactionConcurrency, "compactor.compaction-concurrency", 1, "Max number of concurrent compactions running.")
	f.DurationVar(&cfg.CleanupInterval, "compactor.cleanup-interval", 15*time.Minute, "How frequently compactor should run blocks cleanup and maintenance, as well as update the bucket index.")
	f.IntVar(&cfg.CleanupConcurrency, "compactor.cleanup-concurrency", 20, "Max number of tenants for which blocks cleanup and maintenance should run concurrently.")
	f.BoolVar(&cfg.ShardingEnabled, "compactor.sharding-enabled", false, "Shard tenants across multiple compactor instances. Sharding is required if you run multiple compactor instances, in order to coordinate compactions and avoid race conditions leading to the same tenant blocks simultaneously compacted by different instances.")
	f.DurationVar(&cfg.DeletionDelay, "compactor.deletion-delay", 12*time.Hour, "Time before a block marked for deletion is deleted from bucket. "+
		"If not 0, blocks will be marked for deletion and compactor component will permanently delete blocks marked for deletion from the bucket. "+
		"If 0, blocks will be deleted straight away. Note that deleting blocks immediately can cause query failures.")
	f.DurationVar(&cfg.TenantCleanupDelay, "compactor.tenant-cleanup-delay", 6*time.Hour, "For tenants marked for deletion, this is time between deleting of last block, and doing final cleanup (marker files, debug files) of the tenant.")
	f.BoolVar(&cfg.BlockDeletionMarksMigrationEnabled, "compactor.block-deletion-marks-migration-enabled", true, "When enabled, at compactor startup the bucket will be scanned and all found deletion marks inside the block location will be copied to the markers global location too. This option can (and should) be safely disabled as soon as the compactor has successfully run at least once.")

	f.Var(&cfg.EnabledTenants, "compactor.enabled-tenants", "Comma separated list of tenants that can be compacted. If specified, only these tenants will be compacted by compactor, otherwise all tenants can be compacted. Subject to sharding.")
	f.Var(&cfg.DisabledTenants, "compactor.disabled-tenants", "Comma separated list of tenants that cannot be compacted by this compactor. If specified, and compactor would normally pick given tenant for compaction (via -compactor.enabled-tenants or sharding), it will be ignored instead.")
}
