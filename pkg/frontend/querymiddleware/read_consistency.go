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

	offsetsReader *ingest.SingleClusterTopicOffsetsReader

	limits  Limits
	logger  log.Logger
	metrics *ingest.StrongReadConsistencyMetrics
}

func newReadConsistencyRoundTripper(next http.RoundTripper, offsetsReader *ingest.SingleClusterTopicOffsetsReader, limits Limits, logger log.Logger, metrics *ingest.StrongReadConsistencyMetrics) http.RoundTripper {
	return &readConsistencyRoundTripper{
		next:          next,
		offsetsReader: offsetsReader,
		limits:        limits,
		logger:        logger,
		metrics:       metrics,
	}
}

func (r *readConsistencyRoundTripper) RoundTrip(req *http.Request) (_ *http.Response, returnErr error) {
	ctx := req.Context()

	spanLog, ctx := spanlogger.New(ctx, r.logger, tracer, "readConsistencyRoundTripper.RoundTrip")
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
		spanLog.DebugLog("msg", "evaluating query with eventually consistent read consistency")
		return r.next.RoundTrip(req)
	}

	offsets, err := ingest.ObserveStrongReadConsistency(r.metrics, r.offsetsReader.Topic(), false, func() (map[int32]int64, error) {
		return r.offsetsReader.WaitNextFetchLastProducedOffset(ctx)
	})
	if err != nil {
		return nil, errors.Wrapf(err, "wait for last produced offsets of topic '%s'", r.offsetsReader.Topic())
	}

	headerValue := string(querierapi.EncodeOffsets(offsets))
	req.Header.Add(querierapi.ReadConsistencyOffsetsHeader, headerValue)

	spanLog.DebugLog("msg", "got offsets for strong read consistency", "header", querierapi.ReadConsistencyOffsetsHeader, "value", headerValue)
	spanLog.DebugLog("msg", "evaluating query with strong read consistency")

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

func newReadConsistencyMetrics(reg prometheus.Registerer, offsetsReader *ingest.SingleClusterTopicOffsetsReader) *ingest.StrongReadConsistencyMetrics {
	const component = "query-frontend"

	topics := []string{offsetsReader.Topic()}
	return ingest.NewStrongReadConsistencyMetrics(reg, component, topics)
}
