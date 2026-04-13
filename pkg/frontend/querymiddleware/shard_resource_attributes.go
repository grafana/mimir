// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type shardResourceAttributesMiddleware struct {
	upstream http.RoundTripper
	limits   Limits
	logger   log.Logger
}

func newShardResourceAttributesMiddleware(upstream http.RoundTripper, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardResourceAttributesMiddleware{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
	}
}

func (s *shardResourceAttributesMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.New(r.Context(), s.logger, tracer, "shardResourceAttributes.RoundTrip")
	defer spanLog.Finish()

	// Don't shard /api/v1/resources/series (reverse lookup uses resource.attr filters, not series matchers).
	if IsResourceAttributesSeriesQuery(r.URL.Path) {
		return s.upstream.RoundTrip(r)
	}

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	shardCount := s.limits.QueryShardingTotalShards(tenantID)
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

	spanLog.DebugLog("msg", "sharding resource attributes query", "shardCount", shardCount)

	// Parse original request params.
	reqValues, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Parse limit for re-application after merge.
	var limit int64
	if limitStr := reqValues.Get("limit"); limitStr != "" {
		var err error
		limit, err = strconv.ParseInt(limitStr, 10, 64)
		if err != nil {
			return nil, apierror.New(apierror.TypeBadData, fmt.Sprintf("invalid limit parameter %q: %s", limitStr, err))
		}
	}

	// If no match[] params, pass through so the querier returns the proper 400.
	if len(reqValues["match[]"]) == 0 {
		return s.upstream.RoundTrip(r)
	}

	// Build N sharded requests, each with the shard matcher AND'd into every match[] entry.
	reqs, err := buildShardedResourceAttributesRequests(ctx, r, shardCount, reqValues)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	resps, err := doShardedRequests(ctx, reqs, s.upstream)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	return mergeResourceAttributesResponses(resps, limit, spanLog)
}

func buildShardedResourceAttributesRequests(ctx context.Context, origReq *http.Request, shardCount int, origValues url.Values) ([]*http.Request, error) {
	origMatchStrs := origValues["match[]"]

	// Pre-parse all match[] entries so we can inject the shard matcher properly.
	pqlParser := promqlext.NewPromQLParser()
	parsedMatchers := make([][]*labels.Matcher, len(origMatchStrs))
	for i, m := range origMatchStrs {
		ms, err := pqlParser.ParseMetricSelector(m)
		if err != nil {
			return nil, fmt.Errorf("invalid match[] selector %q: %w", m, err)
		}
		parsedMatchers[i] = ms
	}

	reqs := make([]*http.Request, shardCount)
	for i := 0; i < shardCount; i++ {
		r, err := http.NewRequestWithContext(ctx, origReq.Method, origReq.URL.Path, http.NoBody)
		if err != nil {
			return nil, err
		}

		shardMatcher, err := labels.NewMatcher(
			labels.MatchEqual, sharding.ShardLabel,
			sharding.ShardSelector{ShardIndex: uint64(i), ShardCount: uint64(shardCount)}.LabelValue(),
		)
		if err != nil {
			return nil, err
		}

		// Deep-copy all original params, excluding match[] (rebuilt below)
		// and limit (applied only post-merge to avoid premature truncation).
		vals := make(url.Values, len(origValues))
		for k, v := range origValues {
			if k == "match[]" || k == "limit" {
				continue
			}
			vals[k] = append([]string(nil), v...)
		}

		// Inject the shard matcher AND'd into every match[] entry by appending
		// it to the parsed matchers and re-serializing as a valid selector.
		// Use a capacity-limited slice to avoid mutating the shared backing array.
		for _, ms := range parsedMatchers {
			all := append(ms[:len(ms):len(ms)], shardMatcher)
			vals.Add("match[]", matchersToSelector(all))
		}

		r.URL.RawQuery = vals.Encode()
		r.RequestURI = r.URL.String()

		if err := user.InjectOrgIDIntoHTTPRequest(ctx, r); err != nil {
			return nil, err
		}

		reqs[i] = r
	}
	return reqs, nil
}

// matchersToSelector serializes matchers into a valid PromQL metric selector string.
func matchersToSelector(ms []*labels.Matcher) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, m := range ms {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(m.String())
	}
	b.WriteByte('}')
	return b.String()
}

// resourceAttributesResponse is used for JSON unmarshalling of partial shard responses.
type resourceAttributesResponse struct {
	Status string `json:"status"`
	Data   *struct {
		Series []stdjson.RawMessage `json:"series"`
	} `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

// seriesWithSortKey holds a raw JSON series together with a canonical sort
// key derived from its labels. This ensures the sharded merge produces the
// same ordering as the non-sharded handler (which sorts via labelsMapToKey).
type seriesWithSortKey struct {
	key  string
	data stdjson.RawMessage
}

// extractLabelsSortKey parses a series JSON object and returns a canonical
// sort key matching the querier handler's labelsMapToKey (sorted label
// key\x00value\x01 pairs). On parse failure, falls back to the raw JSON
// bytes to preserve uniqueness for deduplication.
func extractLabelsSortKey(raw stdjson.RawMessage) string {
	var partial struct {
		Labels map[string]string `json:"labels"`
	}
	if err := stdjson.Unmarshal(raw, &partial); err != nil || len(partial.Labels) == 0 {
		// Use raw bytes as fallback key to avoid collapsing distinct
		// unparseable series into one during deduplication.
		return string(raw)
	}
	keys := make([]string, 0, len(partial.Labels))
	for k := range partial.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.WriteString(partial.Labels[k])
		buf.WriteByte(1)
	}
	return buf.String()
}

// mergeResourceAttributesResponses concatenates series from all shard
// responses, sorts for determinism, re-applies the client limit, and
// returns a single merged JSON response. Full in-memory buffering is
// acceptable because resource attributes responses are small (typically
// 10s–100s of series, each a few hundred bytes).
func mergeResourceAttributesResponses(resps []*http.Response, limit int64, logger log.Logger) (*http.Response, error) {
	// Drain and close all response bodies first to prevent leaks if
	// unmarshalling fails partway through the loop.
	bodies := make([][]byte, len(resps))
	for i, resp := range resps {
		if resp == nil {
			continue
		}
		var err error
		bodies[i], err = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			// Close remaining bodies before returning.
			for _, r := range resps[i+1:] {
				if r != nil && r.Body != nil {
					_ = r.Body.Close()
				}
			}
			return nil, fmt.Errorf("reading shard response: %w", err)
		}
	}

	var allSeries []seriesWithSortKey
	seen := map[string]int{} // label sort key → index into allSeries
	for _, body := range bodies {
		if body == nil {
			continue
		}

		var parsed resourceAttributesResponse
		if err := stdjson.Unmarshal(body, &parsed); err != nil {
			return nil, fmt.Errorf("unmarshalling shard response: %w", err)
		}
		if parsed.Status != "success" {
			return nil, fmt.Errorf("shard returned error: %s", parsed.Error)
		}
		if parsed.Data != nil {
			for _, s := range parsed.Data.Series {
				key := extractLabelsSortKey(s)
				if idx, dup := seen[key]; dup {
					// Merge versions from the duplicate into the existing entry.
					merged, err := mergeSeriesVersionsJSON(allSeries[idx].data, s)
					if err != nil {
						level.Warn(logger).Log("msg", "failed to merge duplicate series versions across shards, keeping existing", "err", err)
					} else {
						allSeries[idx].data = merged
					}
					continue
				}
				seen[key] = len(allSeries)
				allSeries = append(allSeries, seriesWithSortKey{
					key:  key,
					data: s,
				})
			}
		}
	}

	// Sort by canonical label key for deterministic output matching
	// the non-sharded handler's ordering.
	sort.Slice(allSeries, func(i, j int) bool {
		return allSeries[i].key < allSeries[j].key
	})

	// Re-apply limit after merge.
	if limit > 0 && int64(len(allSeries)) > limit {
		allSeries = allSeries[:limit]
	}

	// Build merged response.
	merged := struct {
		Status string `json:"status"`
		Data   struct {
			Series []stdjson.RawMessage `json:"series"`
		} `json:"data"`
	}{
		Status: "success",
	}
	rawSeries := make([]stdjson.RawMessage, len(allSeries))
	for i, s := range allSeries {
		rawSeries[i] = s.data
	}
	merged.Data.Series = rawSeries

	mergedBody, err := stdjson.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("marshalling merged response: %w", err)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(mergedBody)),
	}, nil
}

// versionDedupKey is used to deduplicate versions by time range and identifying
// attributes, matching the dedup logic in the handler's mergeResourceVersions.
type versionDedupKey struct {
	minTime     int64
	maxTime     int64
	identifying string
}

// parsedVersion is the subset of a version JSON object needed for deduplication.
type parsedVersion struct {
	Identifying map[string]string `json:"identifying"`
	MinTimeMs   int64             `json:"minTimeMs"`
	MaxTimeMs   int64             `json:"maxTimeMs"`
}

func (v *parsedVersion) dedupKey() versionDedupKey {
	// Build a canonical key for identifying attrs matching the handler's labelsMapToKey.
	keys := make([]string, 0, len(v.Identifying))
	for k := range v.Identifying {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	for _, k := range keys {
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.WriteString(v.Identifying[k])
		buf.WriteByte(1)
	}
	return versionDedupKey{v.MinTimeMs, v.MaxTimeMs, buf.String()}
}

// mergeSeriesVersionsJSON merges the "versions" arrays from two JSON series
// objects, deduplicating by (minTimeMs, maxTimeMs, identifying).
func mergeSeriesVersionsJSON(existing, incoming stdjson.RawMessage) (stdjson.RawMessage, error) {
	var existingObj struct {
		Labels   map[string]string    `json:"labels"`
		Versions []stdjson.RawMessage `json:"versions"`
	}
	var incomingObj struct {
		Versions []stdjson.RawMessage `json:"versions"`
	}
	if err := stdjson.Unmarshal(existing, &existingObj); err != nil {
		return nil, err
	}
	if err := stdjson.Unmarshal(incoming, &incomingObj); err != nil {
		return nil, err
	}
	if len(incomingObj.Versions) == 0 {
		return existing, nil
	}

	// Build dedup set from existing versions.
	seen := make(map[versionDedupKey]struct{}, len(existingObj.Versions))
	for _, raw := range existingObj.Versions {
		var v parsedVersion
		if err := stdjson.Unmarshal(raw, &v); err != nil {
			continue // skip from dedup set; the version is kept in the output but won't be deduplicated
		}
		seen[v.dedupKey()] = struct{}{}
	}

	// Append only incoming versions not already present.
	for _, raw := range incomingObj.Versions {
		var v parsedVersion
		if err := stdjson.Unmarshal(raw, &v); err != nil {
			// Keep unparseable versions to avoid data loss.
			existingObj.Versions = append(existingObj.Versions, raw)
			continue
		}
		if _, dup := seen[v.dedupKey()]; dup {
			continue
		}
		seen[v.dedupKey()] = struct{}{}
		existingObj.Versions = append(existingObj.Versions, raw)
	}

	// Sort merged versions by minTimeMs for deterministic output.
	sort.Slice(existingObj.Versions, func(i, j int) bool {
		var vi, vj parsedVersion
		_ = stdjson.Unmarshal(existingObj.Versions[i], &vi)
		_ = stdjson.Unmarshal(existingObj.Versions[j], &vj)
		return vi.MinTimeMs < vj.MinTimeMs
	})

	return stdjson.Marshal(existingObj)
}
