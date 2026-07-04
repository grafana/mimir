// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

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
	upstream http.RoundTripper
	limits   Limits
	logger   log.Logger
}

func newShardLabelPresenceMiddleware(upstream http.RoundTripper, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardLabelPresenceMiddleware{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
	}
}

func (s *shardLabelPresenceMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.New(r.Context(), s.logger, tracer, "shardLabelPresence.RoundTrip")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Decode the request up-front so we can carry label[]/limit onto every shard
	// and reuse the parsed values when a shard count of <2 disables sharding.
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

	if maxShards := s.limits.QueryShardingMaxShardedQueries(tenantID); shardCount > maxShards {
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

	extraValues := url.Values{}
	for _, l := range parsed.Labels {
		extraValues.Add("label[]", l)
	}
	extraValues.Set("limit", strconv.Itoa(parsed.Limit))

	reqs, err := buildShardedRequests(ctx, r, shardCount, selector, extraValues)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	responses, err := doShardedRequests(ctx, reqs, s.upstream)
	if err != nil {
		if errors.Is(err, errShardCountTooLow) {
			return nil, apierror.New(apierror.TypeTooLargeEntry, fmt.Errorf("%w: try increasing the requested shard count", err).Error())
		}
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	merged, err := mergeLabelPresenceResponses(responses, parsed.Labels, parsed.Limit)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

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

// mergeLabelPresenceResponses additively folds the per-shard aggregates into a
// single response. labelNames defines the label order in the merged response;
// limit caps the number of example series returned.
func mergeLabelPresenceResponses(responses []*http.Response, labelNames []string, limit int) (*cardinality.LabelPresenceResponse, error) {
	merged := &cardinality.LabelPresenceResponse{
		Labels: make([]cardinality.LabelPresenceItem, len(labelNames)),
	}
	missingByName := make(map[string]int, len(labelNames))
	for i, name := range labelNames {
		merged.Labels[i].LabelName = name
		missingByName[name] = i
	}

	for _, res := range responses {
		if res == nil {
			continue
		}
		body, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("error reading shard response: %w", err)
		}

		var shardResp cardinality.LabelPresenceResponse
		if err := json.Unmarshal(body, &shardResp); err != nil {
			return nil, fmt.Errorf("error decoding shard response: %w", err)
		}

		merged.TotalSeries += shardResp.TotalSeries
		merged.CompliantSeries += shardResp.CompliantSeries
		for _, item := range shardResp.Labels {
			if idx, ok := missingByName[item.LabelName]; ok {
				merged.Labels[idx].MissingCount += item.MissingCount
			}
		}
		for _, ex := range shardResp.Examples {
			if len(merged.Examples) >= limit {
				break
			}
			merged.Examples = append(merged.Examples, ex)
		}
	}

	return merged, nil
}
