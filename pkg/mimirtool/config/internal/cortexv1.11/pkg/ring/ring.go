// SPDX-License-Identifier: AGPL-3.0-only

package ring

import (
	"flag"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/pkg/errors"
)

type Config struct {
	KVStore              kv.Config     `yaml:"kvstore"`
	HeartbeatTimeout     time.Duration `yaml:"heartbeat_timeout"`
	ReplicationFactor    int           `yaml:"replication_factor"`
	ZoneAwarenessEnabled bool          `yaml:"zone_awareness_enabled"`

	// Whether the shuffle-sharding subring cache is disabled. This option is set
	// internally and never exposed to the user.
}

const (
	unhealthy = "Unhealthy"

	// IngesterRingKey is the key under which we store the ingesters ring in the KVStore.
	IngesterRingKey = "ring"

	// RulerRingKey is the key under which we store the rulers ring in the KVStore.
	RulerRingKey = "ring"

	// DistributorRingKey is the key under which we store the distributors ring in the KVStore.
	DistributorRingKey = "distributor"

	// CompactorRingKey is the key under which we store the compactors ring in the KVStore.
	CompactorRingKey = "compactor"

	// GetBufferSize is the suggested size of buffers passed to Ring.Get(). It's based on
	// a typical replication factor 3, plus extra room for a JOINING + LEAVING instance.
	GetBufferSize = 5
)

var (
	// ErrEmptyRing is the error returned when trying to get an element when nothing has been added to hash.
	ErrEmptyRing = errors.New("empty ring")

	// ErrInstanceNotFound is the error returned when trying to get information for an instance
	// not registered within the ring.
	ErrInstanceNotFound = errors.New("instance not found in the ring")

	// ErrTooManyUnhealthyInstances is the error returned when there are too many failed instances for a
	// specific operation.
	ErrTooManyUnhealthyInstances = errors.New("too many unhealthy instances in the ring")

	// ErrInconsistentTokensInfo is the error returned if, due to an internal bug, the mapping between
	// a token and its own instance is missing or unknown.
	ErrInconsistentTokensInfo = errors.New("inconsistent ring tokens information")
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.KVStore.RegisterFlagsWithPrefix(prefix, "collectors/", f)

	f.DurationVar(&cfg.HeartbeatTimeout, prefix+"ring.heartbeat-timeout", time.Minute, "The heartbeat timeout after which ingesters are skipped for reads/writes. 0 = never (timeout disabled).")
	f.IntVar(&cfg.ReplicationFactor, prefix+"distributor.replication-factor", 3, "The number of ingesters to write to and read from.")
	f.BoolVar(&cfg.ZoneAwarenessEnabled, prefix+"distributor.zone-awareness-enabled", false, "True to enable the zone-awareness and replicate ingested samples across different availability zones.")
}
