// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	apierror "github.com/grafana/mimir/pkg/api/error"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type readConsistencyRoundTripper struct {
	next http.RoundTripper

	offsetsReader ReadConsistencyOffsetsReader

	limits  Limits
	logger  log.Logger
	metrics *ingest.StrongReadConsistencyMetrics
}

func newReadConsistencyRoundTripper(next http.RoundTripper, offsetsReader ReadConsistencyOffsetsReader, limits Limits, logger log.Logger, metrics *ingest.StrongReadConsistencyMetrics) http.RoundTripper {
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

	topicLabel := readConsistencyMetricsTopicLabel(r.offsetsReader.Topics())
	encoded, err := ingest.ObserveStrongReadConsistency(r.metrics, topicLabel, false, func() (querierapi.EncodedOffsets, error) {
		return r.offsetsReader.WaitNextEncodedOffsets(ctx)
	})
	if err != nil {
		return nil, errors.Wrapf(err, "wait for last produced offsets of topics %v", r.offsetsReader.Topics())
	}

	headerValue := string(encoded)
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

func newReadConsistencyMetrics(reg prometheus.Registerer, offsetsReader ReadConsistencyOffsetsReader) *ingest.StrongReadConsistencyMetrics {
	const component = "query-frontend"

	topics := []string{readConsistencyMetricsTopicLabel(offsetsReader.Topics())}
	return ingest.NewStrongReadConsistencyMetrics(reg, component, topics)
}

// readConsistencyMetricsTopicLabel builds the "topic" metric label for an offsets reader: the topic itself
// when it monitors a single topic (compartments disabled), or "mixed" when it monitors multiple
// read-compartment topics.
func readConsistencyMetricsTopicLabel(topics []string) string {
	if len(topics) == 1 {
		return topics[0]
	}
	return "mixed"
}

// ReadConsistencyOffsetsReader fetches the last produced offsets used to enforce strong read consistency
// and encodes them for the read consistency offsets header.
type ReadConsistencyOffsetsReader interface {
	// WaitNextEncodedOffsets returns the result of the next "last produced offsets" fetch, already encoded
	// for the read consistency offsets header.
	WaitNextEncodedOffsets(ctx context.Context) (querierapi.EncodedOffsets, error)

	// Topics returns the monitored topics, used for metrics and logging.
	Topics() []string
}

// singleClusterReadConsistencyOffsetsReader monitors the offsets of a single topic in a single Kafka
// cluster and encodes them with the v1 format. It is used when compartments are disabled.
type singleClusterReadConsistencyOffsetsReader struct {
	reader *ingest.SingleClusterTopicOffsetsReader
}

// NewSingleClusterReadConsistencyOffsetsReader returns a ReadConsistencyOffsetsReader backed by a single
// Kafka cluster's topic offsets reader. The reader is expected to monitor exactly one topic.
func NewSingleClusterReadConsistencyOffsetsReader(reader *ingest.SingleClusterTopicOffsetsReader) ReadConsistencyOffsetsReader {
	return singleClusterReadConsistencyOffsetsReader{reader: reader}
}

func (r singleClusterReadConsistencyOffsetsReader) WaitNextEncodedOffsets(ctx context.Context) (querierapi.EncodedOffsets, error) {
	offsets, err := r.reader.WaitNextFetchLastProducedOffset(ctx)
	if err != nil {
		return "", err
	}
	return querierapi.EncodeOffsetsV1(offsets), nil
}

func (r singleClusterReadConsistencyOffsetsReader) Topics() []string {
	return []string{r.reader.Topic()}
}

// multiClusterReadConsistencyOffsetsReader monitors the offsets of the read-compartment topics across all
// write-compartment Kafka clusters and encodes them with the v2 format. It is used when compartments are
// enabled.
type multiClusterReadConsistencyOffsetsReader struct {
	reader *ingest.MultiClusterOffsetsReader
}

// NewMultiClusterReadConsistencyOffsetsReader returns a ReadConsistencyOffsetsReader backed by a
// multi-cluster offsets reader.
func NewMultiClusterReadConsistencyOffsetsReader(reader *ingest.MultiClusterOffsetsReader) ReadConsistencyOffsetsReader {
	return multiClusterReadConsistencyOffsetsReader{reader: reader}
}

func (r multiClusterReadConsistencyOffsetsReader) WaitNextEncodedOffsets(ctx context.Context) (querierapi.EncodedOffsets, error) {
	offsetsByTopic, err := r.reader.WaitNextFetchLastProducedOffset(ctx)
	if err != nil {
		return "", err
	}

	// The reader returns offsets keyed by topic; map each topic back to its read compartment ID (topics are
	// returned in read-compartment order, so the index is the read compartment ID) for the v2 encoding.
	topics := r.reader.Topics()
	offsetsByReadCompartment := make(map[int]kmeta.PartitionsOffsets, len(topics))
	for readCompartmentID, topic := range topics {
		offsetsByReadCompartment[readCompartmentID] = offsetsByTopic[topic]
	}
	return querierapi.EncodeOffsetsV2(offsetsByReadCompartment), nil
}

func (r multiClusterReadConsistencyOffsetsReader) Topics() []string {
	return r.reader.Topics()
}
