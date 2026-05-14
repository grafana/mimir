// SPDX-License-Identifier: AGPL-3.0-only

package readcache

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

// Config configures the Readcache service.
type Config struct {
	// InstanceID identifies the readcache pod. Defaults to the hostname.
	InstanceID string `yaml:"instance_id" doc:"default=<hostname>" category:"experimental"`

	// DataDir is the root directory under which per-(tenant, partition)
	// TSDBs are stored.
	DataDir string `yaml:"data_dir" category:"experimental"`

	// KafkaTopic overrides the topic readcache consumes. Empty value
	// inherits the global ingest-storage topic. The plan uses a
	// dedicated experimental topic (default: "nautilus_ingest") so the
	// dev-cell readcache fleet stays isolated from production
	// ingesters.
	KafkaTopic string `yaml:"kafka_topic" category:"experimental"`

	// OwnedPartitions is the static partition assignment for Phase 2A
	// (before the rebalancer log is wired in). Comma-separated list of
	// int32 partition IDs.
	//
	// In Phase 2B this becomes obsolete: ownership is supplied by the
	// rebalancer's WatchReadcacheAssignments stream. For Phase 2A it
	// is the only ownership source.
	//
	// Service discovery: the readcache pod doesn't yet register in a
	// dedicated ring; until the rebalancer is wired up in 2B,
	// distributors find readcache pods by direct config
	// (-distributor.readcache.addresses, added in Phase 2C). The ring
	// lifecycler will land alongside the WatchReadcacheAssignments
	// subscription so that the rebalancer discovers active instances
	// via the same KV that supplies their partition leases.
	OwnedPartitions string `yaml:"owned_partitions" category:"experimental"`

	// HeadCompactionInterval is how often each partitionTSDB's head is
	// considered for compaction. Compaction keeps the in-memory head
	// small even though readcache never ships blocks (blockbuilder
	// handles long-term blocks).
	HeadCompactionInterval time.Duration `yaml:"head_compaction_interval" category:"experimental"`

	// LocalBlockRetention is how long readcache keeps locally-compacted
	// blocks queryable after they leave the head. Beyond this window,
	// store-gateway (fed by blockbuilder) is the canonical source.
	LocalBlockRetention time.Duration `yaml:"local_block_retention" category:"experimental"`

	// Config parameters injected dynamically from outside readcache's own config.
	Kafka         ingest.KafkaConfig       `yaml:"-"`
	BlocksStorage tsdb.BlocksStorageConfig `yaml:"-"`
}

// RegisterFlags registers the readcache flags on f.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname", "err", err)
		os.Exit(1)
	}

	f.StringVar(&cfg.InstanceID, "readcache.instance-id", hostname, "Instance ID. Defaults to the hostname.")
	f.StringVar(&cfg.DataDir, "readcache.data-dir", "./data-readcache/", "Directory under which per-(tenant, partition) TSDBs are stored. Wiped on restart.")
	f.StringVar(&cfg.KafkaTopic, "readcache.kafka-topic", "nautilus_ingest", "Kafka topic readcache consumes from. The plan uses a dedicated experimental topic to isolate the readcache fleet from production ingesters.")
	f.StringVar(&cfg.OwnedPartitions, "readcache.owned-partitions", "", "Static comma-separated list of int32 partition IDs this readcache instance owns (Phase 2A). Empty means no partitions are owned; the rebalancer log will populate ownership in Phase 2B.")
	f.DurationVar(&cfg.HeadCompactionInterval, "readcache.head-compaction-interval", 1*time.Hour, "How often each partitionTSDB head is considered for compaction.")
	f.DurationVar(&cfg.LocalBlockRetention, "readcache.local-block-retention", 6*time.Hour, "How long readcache keeps locally-compacted blocks queryable after they leave the head.")
}

// Validate returns nil if the Config is internally consistent.
func (cfg *Config) Validate() error {
	if cfg.InstanceID == "" {
		return fmt.Errorf("instance id is required")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("data-dir is required")
	}
	if cfg.KafkaTopic == "" {
		return fmt.Errorf("kafka-topic is required")
	}
	if cfg.HeadCompactionInterval <= 0 {
		return fmt.Errorf("head-compaction-interval must be positive")
	}
	if cfg.LocalBlockRetention < 0 {
		return fmt.Errorf("local-block-retention must be non-negative")
	}
	if _, err := cfg.ParseOwnedPartitions(); err != nil {
		return fmt.Errorf("owned-partitions: %w", err)
	}
	return nil
}

// ParseOwnedPartitions parses the comma-separated owned-partitions
// flag into a sorted, deduplicated slice of int32 partition IDs. An
// empty string yields a nil slice.
func (cfg *Config) ParseOwnedPartitions() ([]int32, error) {
	s := strings.TrimSpace(cfg.OwnedPartitions)
	if s == "" {
		return nil, nil
	}

	parts := strings.Split(s, ",")
	seen := make(map[int32]struct{}, len(parts))
	out := make([]int32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseInt(p, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid partition %q: %w", p, err)
		}
		if v < 0 {
			return nil, fmt.Errorf("partition id %d is negative", v)
		}
		pid := int32(v)
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		out = append(out, pid)
	}
	return out, nil
}
