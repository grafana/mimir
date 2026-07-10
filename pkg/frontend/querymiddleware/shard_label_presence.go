// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// shardLabelPresenceMiddleware shards a label presence cardinality request by
// __query_shard__ and merges the per-shard aggregates. Because sharding
// partitions series disjointly, the counts can be summed across shards without
// double counting.
type shardLabelPresenceMiddleware struct {
	shardBySeriesBase
}

func newShardLabelPresenceMiddleware(upstream http.RoundTripper, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardLabelPresenceMiddleware{shardBySeriesBase{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
	}}
}

func (s *shardLabelPresenceMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.New(r.Context(), s.logger, tracer, "shardLabelPresence.RoundTrip")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Decode the request up-front so we know the expected labels and limit to use
	// when merging the per-shard responses.
	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}
	parsed, err := cardinality.DecodeLabelPresenceRequestFromValues(reqValues)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	defaultShardCount := s.limits.QueryShardingTotalShards(tenantID)
	shardCount := setShardCountFromHeader(defaultShardCount, r, spanLog)

	if shardCount < 2 {
		spanLog.DebugLog("msg", "query sharding disabled for request")
		return s.upstream.RoundTrip(r)
	}

	maxShards := s.limits.QueryShardingMaxShardedQueries(tenantID)
	if cardinalityMaxShards := s.limits.CardinalityShardingMaxShardedQueries(tenantID); cardinalityMaxShards > 0 {
		maxShards = cardinalityMaxShards
	}
	if maxShards > 0 && shardCount > maxShards {
		return nil, apierror.New(
			apierror.TypeBadData,
			fmt.Sprintf("shard count %d exceeds allowed maximum (%d)", shardCount, maxShards),
		)
	}

	selector, err := parseSelector(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	spanLog.DebugLog("msg", "sharding label presence query", "shardCount", shardCount, "selector", selector.String())

	// buildShardedRequests inherits the label[] and limit params from r.
	reqs, err := buildShardedRequests(ctx, r, shardCount, selector)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	merger := newLabelPresenceMerger(parsed.Labels, parsed.Limit)
	if err := s.processShardedRequests(ctx, reqs, merger.merge); err != nil {
		if errors.Is(err, errShardCountTooLow) {
			return nil, apierror.New(apierror.TypeTooLargeEntry, fmt.Errorf("%w: try increasing the requested shard count", err).Error())
		}
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	merged := merger.result()

	body, err := json.Marshal(merged)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	resp := &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
	}
	resp.Header.Set("Content-Type", "application/json")
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return resp, nil
}

// labelPresenceMerger additively folds the per-shard aggregates into a single
// response. Because processShardedRequests invokes merge concurrently across
// shards, all mutations are guarded by mu. labelNames defines the label order
// in the merged response; limit caps the number of example series returned.
type labelPresenceMerger struct {
	mu            sync.Mutex
	merged        *cardinality.LabelPresenceResponse
	missingByName map[string]int
	limit         int
}

func newLabelPresenceMerger(labelNames []string, limit int) *labelPresenceMerger {
	merged := &cardinality.LabelPresenceResponse{
		Labels: make([]cardinality.LabelPresenceItem, len(labelNames)),
	}
	missingByName := make(map[string]int, len(labelNames))
	for i, name := range labelNames {
		merged.Labels[i].LabelName = name
		missingByName[name] = i
	}
	return &labelPresenceMerger{
		merged:        merged,
		missingByName: missingByName,
		limit:         limit,
	}
}

// merge decodes a single shard response and folds it into the accumulated
// result. It satisfies the handle callback signature of processShardedRequests.
func (m *labelPresenceMerger) merge(_ context.Context, res *http.Response) error {
	body, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil {
		return fmt.Errorf("error reading shard response: %w", err)
	}

	var shardResp cardinality.LabelPresenceResponse
	if err := json.Unmarshal(body, &shardResp); err != nil {
		return fmt.Errorf("error decoding shard response: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.merged.TotalSeries += shardResp.TotalSeries
	m.merged.CompliantSeries += shardResp.CompliantSeries
	for _, item := range shardResp.Labels {
		if idx, ok := m.missingByName[item.LabelName]; ok {
			m.merged.Labels[idx].MissingCount += item.MissingCount
		}
	}
	for _, ex := range shardResp.Examples {
		if len(m.merged.Examples) >= m.limit {
			break
		}
		m.merged.Examples = append(m.merged.Examples, ex)
	}

	return nil
}

func (m *labelPresenceMerger) result() *cardinality.LabelPresenceResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.merged
}
