// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

func TestWCClientConfigs(t *testing.T) {
	base := ingest.KafkaConfig{
		Topic:   "mimir-read",
		Address: flagext.StringSliceCSV{"kafka-write-<write-compartment-id>:9092"},
	}

	t.Run("compartments disabled returns the single base config", func(t *testing.T) {
		got := wcClientConfigs(base, compartments.Config{})
		require.Len(t, got, 1)
		assert.Equal(t, base.Address.String(), got[0].Address.String())
	})

	t.Run("compartments enabled returns one config per write WC", func(t *testing.T) {
		comp := compartments.Config{
			Enabled: true,
			Read:    compartments.ReadConfig{NumCompartments: 1},
			Write:   compartments.WriteConfig{NumCompartments: 2},
		}
		got := wcClientConfigs(base, comp)
		require.Len(t, got, 2)
		assert.Equal(t, "kafka-write-0:9092", got[0].Address.String())
		assert.Equal(t, "kafka-write-1:9092", got[1].Address.String())
	})
}
