// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/storage/factory_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storage

import (
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestFactoryStop(t *testing.T) {
	var (
		cfg          Config
		storeConfig  chunk.StoreConfig
		schemaConfig chunk.SchemaConfig
		defaults     validation.Limits
	)
	flagext.DefaultValues(&cfg, &storeConfig, &schemaConfig, &defaults)
	schemaConfig.Configs = []chunk.PeriodConfig{
		{
			From:      chunk.DayTime{Time: model.Time(0)},
			IndexType: "inmemory",
			Schema:    "v3",
		},
		{
			From:      chunk.DayTime{Time: model.Time(1)},
			IndexType: "inmemory",
			Schema:    "v9",
		},
	}

	limits, err := validation.NewOverrides(defaults, nil)
	require.NoError(t, err)

	store, err := NewStore(cfg, storeConfig, schemaConfig, limits, nil, nil, log.NewNopLogger())
	require.NoError(t, err)

	store.Stop()
}
