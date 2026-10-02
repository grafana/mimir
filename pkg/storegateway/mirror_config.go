// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"errors"
	"flag"
)

// mirrorRingName is the name of the client of the mirrored ring.
const mirrorRingName = "store-gateway-mirror"

// MirrorConfig is the configuration of the store-gateway mirror mode.
type MirrorConfig struct {
	Enabled      bool   `yaml:"enabled" category:"experimental"`
	RingKVPrefix string `yaml:"ring_kvstore_prefix" category:"experimental"`
}

// RegisterFlagsWithPrefix registers the MirrorConfig flags.
func (cfg *MirrorConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.BoolVar(&cfg.Enabled, prefix+"mirror.enabled", false, "If true, the store-gateway does not shard the blocks with the ring that it joins. It loads the same blocks as the instance with the same instance ID in the mirrored ring. The mirrored ring uses the same KV store and sharding ring options, but a different KV prefix.")
	f.StringVar(&cfg.RingKVPrefix, prefix+"mirror.ring-kvstore-prefix", "collectors/", "The KV store prefix of the mirrored ring. It must be different from the prefix of the ring that the store-gateway joins.")
}

// Validate validates the MirrorConfig.
func (cfg *MirrorConfig) Validate(shardingRing RingConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.RingKVPrefix == shardingRing.KVStore.Prefix {
		return errors.New("the store-gateway mirror mode requires a mirrored ring KV store prefix that is different from the sharding ring KV store prefix")
	}
	return nil
}
