// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"

	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type tenantEngine interface {
	// Ingest commits valid samples and exemplars, reporting soft errors through push.
	// A hard append error rolls back the batch.
	Ingest(ctx context.Context, timeseries []mimirpb.PreallocTimeseries, push *pushContext) error
}

type prometheusEngine struct {
	db *tsdb.DB
}
