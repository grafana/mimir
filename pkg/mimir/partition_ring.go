// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"flag"
	"fmt"
	"math"
)

const maxDerivedTokenPartitionsFlag = "partition-ring.max-derived-token-partitions"

// PartitionRingConfig holds the settings shared by every partition ring in the process.
type PartitionRingConfig struct {
	MaxDerivedTokenPartitions int `yaml:"max_derived_token_partitions" category:"experimental"`
}

func (cfg *PartitionRingConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxDerivedTokenPartitions, maxDerivedTokenPartitionsFlag, 0, "The number of partition IDs, starting from 0, that can use derived tokens. "+
		"Only supported by the ingester partition ring for now. "+
		"Components that read the ring generate the tokens of these partitions at startup.")
}

func (cfg *PartitionRingConfig) Validate() error {
	if cfg.MaxDerivedTokenPartitions < 0 || cfg.MaxDerivedTokenPartitions > math.MaxInt32 {
		return fmt.Errorf("-%s must be between 0 and %d, got %d", maxDerivedTokenPartitionsFlag, math.MaxInt32, cfg.MaxDerivedTokenPartitions)
	}
	return nil
}
