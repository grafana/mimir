// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"

	apierror "github.com/grafana/mimir/pkg/api/error"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type readConsistencyRoundTripper struct {
	next http.RoundTripper

	offsetsReader *ingest.TopicOffsetsReader
	limits        Limits
	logger        log.Logger
}

func newReadConsistencyRoundTripper(next http.RoundTripper, offsetsReader *ingest.TopicOffsetsReader, limits Limits, logger log.Logger) http.RoundTripper {
	return &readConsistencyRoundTripper{
		next:          next,
		limits:        limits,
		logger:        logger,
		offsetsReader: offsetsReader,
	}
}

// TODO add a metric to track how long it takes
func (r *readConsistencyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	spanLog, ctx := spanlogger.NewWithLogger(ctx, r.logger, "readConsistencyRoundTripper.RoundTrip")
	defer spanLog.Finish()

	// Fetch the tenant ID(s).
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Detect the requested read consistency level.
	level, ok := querierapi.ReadConsistencyFromContext(req.Context())
	if !ok {
		level = validation.PreferredStringPerTenant(tenantIDs, r.limits.IngestStorageReadConsistency, []string{querierapi.ReadConsistencyStrong})
	}

	if level != querierapi.ReadConsistencyStrong {
		return r.next.RoundTrip(req)
	}

	// Fetch last produced offsets.
	offsets, err := r.offsetsReader.WaitNextFetchLastProducedOffset(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "wait for last produced offsets")
	}

	req.Header.Add(querierapi.ReadConsistencyOffsetsHeader, string(querierapi.EncodeOffsets(offsets)))

	return r.next.RoundTrip(req)
}
