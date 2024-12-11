// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"net/http"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	apierror "github.com/grafana/mimir/pkg/api/error"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type readConsistencyRoundTripper struct {
	next http.RoundTripper

	// offsetsReaders is a map of offsets readers keyed by the request header the offsets get attached to.
	offsetsReaders map[string]*ingest.TopicOffsetsReader

	limits  Limits
	logger  log.Logger
	metrics *ingest.StrongReadConsistencyInstrumentation[map[int32]int64]
}

func newReadConsistencyRoundTripper(next http.RoundTripper, offsetsReaders map[string]*ingest.TopicOffsetsReader, limits Limits, logger log.Logger, metrics *ingest.StrongReadConsistencyInstrumentation[map[int32]int64]) http.RoundTripper {
	return &readConsistencyRoundTripper{
		next:           next,
		offsetsReaders: offsetsReaders,
		limits:         limits,
		logger:         logger,
		metrics:        metrics,
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

	errGroup, ctx := errgroup.WithContext(ctx)
	reqHeaderLock := &sync.Mutex{}

	for headerKey, offsetsReader := range r.offsetsReaders {
		headerKey := headerKey
		offsetsReader := offsetsReader

		errGroup.Go(func() error {
			offsets, err := r.metrics.Observe(false, func() (map[int32]int64, error) {
				return offsetsReader.WaitNextFetchLastProducedOffset(ctx)
			})
			if err != nil {
				return errors.Wrapf(err, "wait for last produced offsets of topic '%s'", offsetsReader.Topic())
			}

			reqHeaderLock.Lock()
			req.Header.Add(headerKey, string(querierapi.EncodeOffsets(offsets)))
			reqHeaderLock.Unlock()

			return nil
		})
	}

	if err = errGroup.Wait(); err != nil {
		return nil, err
	}

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
