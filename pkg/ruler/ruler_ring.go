// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/ruler_ring.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"flag"
	"net"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/util"
)

var (
	// RuleEvalRingOp is the operation used for distributing rule groups between rulers.
	RuleEvalRingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, func(s ring.InstanceState) bool {
		// Only ACTIVE rulers get any rule groups. If instance is not ACTIVE, we need to find another ruler.
		return s != ring.ACTIVE
	})

	RuleSyncRingOp = ring.NewOp([]ring.InstanceState{ring.JOINING, ring.ACTIVE}, func(s ring.InstanceState) bool {
		// Only ACTIVE or JOINING rulers can sync rule groups. If instance is not ACTIVE NOR JOINING, we need to find another ruler.
		return s != ring.ACTIVE && s != ring.JOINING
	})
)

// RingConfig masks the ring lifecycler config which contains
// many options not really required by the rulers ring. This config
// is used to strip down the config to the minimum, and avoid confusion
// to the user.
type RingConfig struct {
	Common util.CommonRingConfig `yaml:",inline"`

	AutoForgetUnhealthyPeriods int `yaml:"auto_forget_unhealthy_periods" category:"advanced"`

	NumTokens int `yaml:"num_tokens" category:"advanced"`

	ZoneAwarenessEnabled bool                   `yaml:"zone_awareness_enabled" category:"experimental"`
	InstanceZone         string                 `yaml:"instance_availability_zone" category:"experimental"`
	ExcludedZones        flagext.StringSliceCSV `yaml:"excluded_zones" category:"experimental"`

	// Used for testing
	SkipUnregister bool `yaml:"-"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *RingConfig) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	const flagNamePrefix = "ruler.ring."
	const kvStorePrefix = "rulers/"
	const componentPlural = "rulers"
	cfg.Common.RegisterFlags(flagNamePrefix, kvStorePrefix, componentPlural, f, logger)

	f.IntVar(&cfg.AutoForgetUnhealthyPeriods, flagNamePrefix+"auto-forget-unhealthy-periods", 2, "Number of consecutive timeout periods after which Mimir automatically removes an unhealthy instance in the ring. Set to 0 to disable auto-forget.")

	f.IntVar(&cfg.NumTokens, flagNamePrefix+"num-tokens", 128, "Number of tokens for each ruler.")

	f.BoolVar(&cfg.ZoneAwarenessEnabled, flagNamePrefix+"zone-awareness-enabled", false, "True to enable zone-awareness. Each tenant's shuffle shard then takes the same number of rulers from each availability zone, and takes more rulers from the other zones when a zone has too few.")
	f.StringVar(&cfg.InstanceZone, flagNamePrefix+"instance-availability-zone", "", "The availability zone where this instance is running. Required if zone-awareness is enabled.")
	f.Var(&cfg.ExcludedZones, flagNamePrefix+"excluded-zones", "Comma-separated list of zones to exclude from the ring. Instances in excluded zones will be filtered out from the ring. The list can't contain the ruler's own zone.")
}

// ToLifecyclerConfig returns a LifecyclerConfig based on the ruler
// ring config.
func (cfg *RingConfig) ToLifecyclerConfig(logger log.Logger) (ring.BasicLifecyclerConfig, error) {
	instanceAddr, err := ring.GetInstanceAddr(cfg.Common.InstanceAddr, cfg.Common.InstanceInterfaceNames, logger, cfg.Common.EnableIPv6)
	if err != nil {
		return ring.BasicLifecyclerConfig{}, err
	}

	instancePort := ring.GetInstancePort(cfg.Common.InstancePort, cfg.Common.ListenPort)

	return ring.BasicLifecyclerConfig{
		ID:                  cfg.Common.InstanceID,
		Addr:                net.JoinHostPort(instanceAddr, strconv.Itoa(instancePort)),
		HeartbeatPeriod:     cfg.Common.HeartbeatPeriod,
		HeartbeatTimeout:    cfg.Common.HeartbeatTimeout,
		TokensObservePeriod: 0,
		Zone:                cfg.InstanceZone,
		NumTokens:           cfg.NumTokens,
	}, nil
}

func (cfg *RingConfig) toRingConfig() ring.Config {
	rc := cfg.Common.ToRingConfig()
	rc.SubringCacheDisabled = true
	rc.ZoneAwarenessEnabled = cfg.ZoneAwarenessEnabled
	rc.ExcludedZones = cfg.ExcludedZones

	// Each rule group is loaded to *exactly* one ruler.
	rc.ReplicationFactor = 1

	return rc
}

// tenantShard returns the shuffle shard of rulers for a tenant.
// With zone awareness, dskit gives each zone the same number of rulers.
// A zone with fewer rulers gives all it has, so the shard gets smaller.
// tenantShard raises the per-zone count until the shard has at least size rulers, or all rulers.
// Raising the per-zone count keeps every ruler that a lower count picks.
func tenantShard(r ring.ReadRing, userID string, size int, zoneAware bool) ring.ReadRing {
	zones := r.Zones()
	if !zoneAware || len(zones) == 0 {
		return r.ShuffleShard(userID, size)
	}

	// The ring can change between reads.
	// Each zone count is read once, so the loop ends when perZone reaches the largest count.
	counts := make([]int, len(zones))
	total := 0
	for i, zone := range zones {
		counts[i] = r.InstancesInZoneCount(zone)
		total += counts[i]
	}

	want := min(size, total)
	perZone := util.ShuffleShardExpectedInstancesPerZone(size, len(zones))
	for shardInstancesCount(counts, perZone) < want {
		perZone++
	}

	return r.ShuffleShard(userID, perZone*len(zones))
}

// shardInstancesCount returns the number of instances in a zone-aware shard with perZone instances per zone.
func shardInstancesCount(counts []int, perZone int) int {
	n := 0
	for _, c := range counts {
		n += min(perZone, c)
	}
	return n
}
