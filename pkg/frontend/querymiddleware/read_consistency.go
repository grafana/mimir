// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	apierror "github.com/grafana/mimir/pkg/api/error"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type readConsistencyRoundTripper struct {
	next http.RoundTripper

	headerKey     string
	offsetsReader *ingest.TopicOffsetsReader
	limits        Limits
	logger        log.Logger
	metrics       *ingest.StrongReadConsistencyInstrumentation[map[int32]int64]
}

func NewReadConsistencyRoundTripper(next http.RoundTripper, headerKey string, offsetsReader *ingest.TopicOffsetsReader, limits Limits, logger log.Logger, metrics *ingest.StrongReadConsistencyInstrumentation[map[int32]int64]) http.RoundTripper {
	return &readConsistencyRoundTripper{
		next:          next,
		headerKey:     headerKey,
		limits:        limits,
		logger:        logger,
		offsetsReader: offsetsReader,
		metrics:       metrics,
	}
}

func (r *readConsistencyRoundTripper) RoundTrip(req *http.Request) (_ *http.Response, returnErr error) {
	ctx := req.Context()

	spanLog, ctx := spanlogger.NewWithLogger(ctx, r.logger, "readConsistencyRoundTripper.RoundTrip")
	defer spanLog.Finish()

	// Fetch the tenant ID(s).
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Detect the requested read consistency level.
	level, ok := querierapi.ReadConsistencyLevelFromContext(req.Context())
	if !ok {
		level = getDefaultReadConsistency(tenantIDs, r.limits)
	}

	if level != querierapi.ReadConsistencyStrong {
		return r.next.RoundTrip(req)
	}

	// Fetch last produced offsets.
	offsets, err := r.metrics.Observe(false, func() (map[int32]int64, error) {
		return r.offsetsReader.WaitNextFetchLastProducedOffset(ctx)
	})
	if err != nil {
		return nil, errors.Wrap(err, "wait for last produced offsets")
	}

	req.Header.Add(r.headerKey, string(querierapi.EncodeOffsets(offsets)))

	return r.next.RoundTrip(req)
}

// getDefaultReadConsistency returns the default read consistency for the input tenantIDs,
// giving preference to strong consistency if enabled for any of the tenants.
func getDefaultReadConsistency(tenantIDs []string, limits Limits) string {
	for _, tenantID := range tenantIDs {
		if limits.IngestStorageReadConsistency(tenantID) == querierapi.ReadConsistencyStrong {
			return querierapi.ReadConsistencyStrong
		}
	}

	return querierapi.ReadConsistencyEventual
}

func newReadConsistencyMetrics(reg prometheus.Registerer) *ingest.StrongReadConsistencyInstrumentation[map[int32]int64] {
	const component = "query-frontend"
	return ingest.NewStrongReadConsistencyInstrumentation[map[int32]int64](component, reg)
}
