// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type tenantEngine interface {
	// Ingest commits valid samples and exemplars, reporting soft errors through ctx.
	// A hard append error rolls back the batch.
	Ingest(ctx *ingestContext, timeseries []mimirpb.PreallocTimeseries) error
}

type prometheusEngine struct {
	db *tsdb.DB
}
