// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/sync/errgroup"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

// ResourceAttributesResponseData contains the response data format.
type ResourceAttributesResponseData struct {
	Series []*SeriesResourceAttributesData `json:"series"`
}

// SeriesResourceAttributesData contains resource attributes for a single series.
type SeriesResourceAttributesData struct {
	Labels   map[string]string      `json:"labels"`
	Versions []*ResourceVersionData `json:"versions"`
}

// ResourceVersionData contains versioned resource attribute data.
type ResourceVersionData struct {
	Identifying map[string]string `json:"identifying,omitempty"`
	Descriptive map[string]string `json:"descriptive,omitempty"`
	Entities    []*EntityData     `json:"entities,omitempty"`
	MinTimeMs   int64             `json:"minTimeMs"`
	MaxTimeMs   int64             `json:"maxTimeMs"`
}

// EntityData contains entity information.
type EntityData struct {
	Type        string            `json:"type"`
	ID          map[string]string `json:"id,omitempty"`
	Description map[string]string `json:"description,omitempty"`
}

// ResourceAttributesResponse matches the Prometheus API response format.
type ResourceAttributesResponse struct {
	Status string                          `json:"status"`
	Data   *ResourceAttributesResponseData `json:"data,omitempty"`
	Error  string                          `json:"error,omitempty"`
}

// ResourceAttributesBlocksQueryable is an interface for querying resource attributes from block storage.
type ResourceAttributesBlocksQueryable interface {
	ResourceAttributes(ctx context.Context, minT, maxT int64, matchers []*labels.Matcher, limit int64, resourceAttrFilters []*storepb.ResourceAttrFilter) ([]*storepb.ResourceAttributesSeriesData, error)
}

// ResourceAttributesHandlerConfig holds configuration for resource attributes handler.
type ResourceAttributesHandlerConfig struct {
	QueryStoreAfter                 time.Duration
	QueryIngestersWithin            func(userID string) time.Duration
	MaxQueryLookback                func(userID string) time.Duration
	MaxLabelsQueryLength            func(userID string) time.Duration
	MaxResourceAttributesQueryLimit func(userID string) int
}

// NewResourceAttributesHandler creates a http.Handler for the /api/v1/resources endpoint.
// This endpoint is for querying OTel resource attributes persisted per time series.
// It queries both ingesters (via distributor) and store-gateways (via blocksQueryable) and merges results.
// If blocksQueryable is nil, only ingesters are queried.
func NewResourceAttributesHandler(d Distributor, blocksQueryable ResourceAttributesBlocksQueryable, cfg ResourceAttributesHandlerConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Validate tenant
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Parse request parameters
		if err := r.ParseForm(); err != nil {
			http.Error(w, "error parsing request form: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Parse time range
		startMs, endMs, err := parseResourceTimeRange(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Parse matchers — multiple match[] params have OR (union) semantics,
		// matching the Prometheus /api/v1/series convention.
		matcherSets := r.Form["match[]"]
		if len(matcherSets) == 0 {
			http.Error(w, "at least one matcher is required (use {__name__=~\".+\"} for all series)", http.StatusBadRequest)
			return
		}

		pqlParser := promqlext.NewPromQLParser()
		var parsedMatcherSets [][]*labels.Matcher
		for _, matcherSet := range matcherSets {
			matchers, err := pqlParser.ParseMetricSelector(matcherSet)
			if err != nil {
				http.Error(w, "error parsing matcher: "+err.Error(), http.StatusBadRequest)
				return
			}
			parsedMatcherSets = append(parsedMatcherSets, matchers)
		}

		// Parse limit (optional)
		var limit int64
		if limitStr := r.FormValue("limit"); limitStr != "" {
			limit, err = strconv.ParseInt(limitStr, 10, 64)
			if err != nil {
				http.Error(w, "invalid limit parameter: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		// Clamp limit to the configured per-tenant max.
		if cfg.MaxResourceAttributesQueryLimit != nil {
			if maxLimit := int64(cfg.MaxResourceAttributesQueryLimit(tenantID)); maxLimit > 0 {
				if limit <= 0 || limit > maxLimit {
					limit = maxLimit
				}
			}
		}

		now := time.Now()
		nowMs := now.UnixMilli()
		var allSeries []*SeriesResourceAttributesData

		// Default endMs to now if not specified
		if endMs == 0 {
			endMs = nowMs
		}

		// Default startMs to 24h ago if not specified, to prevent epoch-to-now scans.
		if startMs == 0 {
			startMs = nowMs - (24 * time.Hour).Milliseconds()
		}

		// Apply max query lookback to avoid scanning all blocks since epoch.
		if cfg.MaxQueryLookback != nil {
			if maxLookback := cfg.MaxQueryLookback(tenantID); maxLookback > 0 {
				earliest := nowMs - maxLookback.Milliseconds()
				if startMs < earliest {
					startMs = earliest
				}
			}
		}

		// Clamp time range to MaxLabelsQueryLength if configured.
		if cfg.MaxLabelsQueryLength != nil {
			if maxLen := cfg.MaxLabelsQueryLength(tenantID); maxLen > 0 {
				if endMs-startMs > maxLen.Milliseconds() {
					startMs = endMs - maxLen.Milliseconds()
				}
			}
		}

		// Query ingesters and store-gateways in parallel, issuing separate queries
		// per matcher set (OR semantics) and merging results. Cap concurrency to
		// avoid excessive fan-out when many match[] params are provided.
		const maxResourceAttrConcurrency = 10
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(maxResourceAttrConcurrency)

		var queryIngestersWithin time.Duration
		if cfg.QueryIngestersWithin != nil {
			queryIngestersWithin = cfg.QueryIngestersWithin(tenantID)
		}
		shouldQueryIngesters := ShouldQueryIngesters(queryIngestersWithin, now, endMs)
		shouldQueryStore := blocksQueryable != nil && ShouldQueryBlockStore(cfg.QueryStoreAfter, now, startMs)

		var resultsMu sync.Mutex
		var ingesterConverted, storeConverted []*SeriesResourceAttributesData

		for _, matchers := range parsedMatcherSets {
			matchers := matchers // capture loop var
			if shouldQueryIngesters {
				g.Go(func() error {
					results, err := d.ResourceAttributes(gCtx, startMs, endMs, matchers, limit, nil)
					if err != nil {
						return err
					}
					converted := convertIngesterResults(results)
					resultsMu.Lock()
					ingesterConverted = append(ingesterConverted, converted...)
					resultsMu.Unlock()
					return nil
				})
			}

			if shouldQueryStore {
				g.Go(func() error {
					results, err := blocksQueryable.ResourceAttributes(gCtx, startMs, endMs, matchers, limit, nil)
					if err != nil {
						return err
					}
					converted := convertStoreResults(results)
					resultsMu.Lock()
					storeConverted = append(storeConverted, converted...)
					resultsMu.Unlock()
					return nil
				})
			}
		}

		// Wait for all queries to complete
		if err := g.Wait(); err != nil {
			http.Error(w, "error querying resource attributes: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Merge results, deduplicating by series labels
		allSeries = mergeResourceAttributesSeries(ingesterConverted, storeConverted)

		// Apply limit if specified
		if limit > 0 && int64(len(allSeries)) > limit {
			allSeries = allSeries[:limit]
		}

		util.WriteJSONResponse(w, ResourceAttributesResponse{
			Status: statusSuccess,
			Data: &ResourceAttributesResponseData{
				Series: allSeries,
			},
		})
	})
}

// parseResourceTimeRange parses the start and end time parameters from the request.
func parseResourceTimeRange(r *http.Request) (int64, int64, error) {
	var startMs, endMs int64

	if startStr := r.FormValue("start"); startStr != "" {
		t, err := util.ParseTime(startStr)
		if err != nil {
			return 0, 0, errors.Wrap(err, "error parsing start time")
		}
		startMs = t
	}

	if endStr := r.FormValue("end"); endStr != "" {
		t, err := util.ParseTime(endStr)
		if err != nil {
			return 0, 0, errors.Wrap(err, "error parsing end time")
		}
		endMs = t
	}

	return startMs, endMs, nil
}

// convertIngesterResults converts ingester client results to the HTTP response format.
func convertIngesterResults(results []*ingester_client.SeriesResourceAttributes) []*SeriesResourceAttributesData {
	if results == nil {
		return nil
	}

	series := make([]*SeriesResourceAttributesData, 0, len(results))

	for _, item := range results {
		lbls := make(map[string]string)
		for _, l := range item.Labels {
			lbls[l.Name] = l.Value
		}

		versions := make([]*ResourceVersionData, 0, len(item.Versions))
		for _, v := range item.Versions {
			version := &ResourceVersionData{
				MinTimeMs:   v.MinTimeMs,
				MaxTimeMs:   v.MaxTimeMs,
				Identifying: v.Identifying,
				Descriptive: v.Descriptive,
			}

			if len(v.Entities) > 0 {
				entities := make([]*EntityData, 0, len(v.Entities))
				for _, e := range v.Entities {
					entities = append(entities, &EntityData{
						Type:        e.Type,
						ID:          e.Id,
						Description: e.Description,
					})
				}
				version.Entities = entities
			}

			versions = append(versions, version)
		}

		series = append(series, &SeriesResourceAttributesData{
			Labels:   lbls,
			Versions: versions,
		})
	}

	return series
}

// convertStoreResults converts store-gateway results to the HTTP response format.
func convertStoreResults(results []*storepb.ResourceAttributesSeriesData) []*SeriesResourceAttributesData {
	if results == nil {
		return nil
	}

	series := make([]*SeriesResourceAttributesData, 0, len(results))

	for _, item := range results {
		versions := make([]*ResourceVersionData, 0, len(item.Versions))
		for _, v := range item.Versions {
			version := &ResourceVersionData{
				MinTimeMs:   v.MinTimeMs,
				MaxTimeMs:   v.MaxTimeMs,
				Identifying: v.Identifying,
				Descriptive: v.Descriptive,
			}

			if len(v.Entities) > 0 {
				entities := make([]*EntityData, 0, len(v.Entities))
				for _, e := range v.Entities {
					entities = append(entities, &EntityData{
						Type:        e.Type,
						ID:          e.Id,
						Description: e.Description,
					})
				}
				version.Entities = entities
			}

			versions = append(versions, version)
		}

		series = append(series, &SeriesResourceAttributesData{
			Labels:   item.Labels,
			Versions: versions,
		})
	}

	return series
}

// mergeResourceAttributesSeries merges results from ingesters and store-gateways.
// Series with the same labels are merged by combining their versions and deduplicating.
func mergeResourceAttributesSeries(ingesterSeries, storeSeries []*SeriesResourceAttributesData) []*SeriesResourceAttributesData {
	// Create a map for efficient lookup by label fingerprint
	seriesMap := make(map[string]*SeriesResourceAttributesData)

	// Add all ingester series, merging duplicates that arise from
	// multiple matcher sets (OR semantics) matching the same series.
	for _, s := range ingesterSeries {
		key := labelsMapToKey(s.Labels)
		if existing, ok := seriesMap[key]; ok {
			existing.Versions = mergeResourceVersions(existing.Versions, s.Versions)
		} else {
			seriesMap[key] = s
		}
	}

	// Merge store series with existing ingester series
	for _, s := range storeSeries {
		key := labelsMapToKey(s.Labels)
		if existing, ok := seriesMap[key]; ok {
			existing.Versions = mergeResourceVersions(existing.Versions, s.Versions)
		} else {
			seriesMap[key] = s
		}
	}

	// Convert map back to slice, pre-computing sort keys to avoid
	// recomputing labelsMapToKey O(N log N) times during sort.
	type keyed struct {
		key  string
		data *SeriesResourceAttributesData
	}
	keyedResult := make([]keyed, 0, len(seriesMap))
	for key, s := range seriesMap {
		keyedResult = append(keyedResult, keyed{key: key, data: s})
	}

	sort.Slice(keyedResult, func(i, j int) bool {
		return keyedResult[i].key < keyedResult[j].key
	})

	result := make([]*SeriesResourceAttributesData, len(keyedResult))
	for i, k := range keyedResult {
		result[i] = k.data
	}
	return result
}

// labelsMapToKey creates a string key from a labels map for deduplication.
// Uses null-byte separators to prevent collisions from values containing delimiters.
func labelsMapToKey(lbls map[string]string) string {
	keys := make([]string, 0, len(lbls))
	for k := range lbls {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	for _, k := range keys {
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.WriteString(lbls[k])
		buf.WriteByte(1)
	}
	return buf.String()
}

// mergeResourceVersions merges and deduplicates resource versions from two sources.
// Deduplication uses time range AND identifying attributes to avoid incorrectly
// collapsing versions that share the same time range but differ in content.
func mergeResourceVersions(a, b []*ResourceVersionData) []*ResourceVersionData {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	type versionKey struct {
		minTime     int64
		maxTime     int64
		identifying string
	}

	seen := make(map[versionKey]bool, len(a)+len(b))
	result := make([]*ResourceVersionData, 0, len(a)+len(b))

	for _, v := range a {
		key := versionKey{v.MinTimeMs, v.MaxTimeMs, labelsMapToKey(v.Identifying)}
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}

	for _, v := range b {
		key := versionKey{v.MinTimeMs, v.MaxTimeMs, labelsMapToKey(v.Identifying)}
		if !seen[key] {
			seen[key] = true
			result = append(result, v)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MinTimeMs < result[j].MinTimeMs
	})

	return result
}

// NewResourceAttributesSeriesHandler creates a http.Handler for the /api/v1/resources/series endpoint.
// This is the reverse lookup endpoint: given resource attribute key:value filters, find matching series.
// The response format is the same as the forward lookup endpoint.
func NewResourceAttributesSeriesHandler(d Distributor, blocksQueryable ResourceAttributesBlocksQueryable, cfg ResourceAttributesHandlerConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := r.ParseForm(); err != nil {
			http.Error(w, "error parsing request form: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Parse time range
		startMs, endMs, err := parseResourceTimeRange(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Parse resource attribute filters: resource.attr=key:value
		filterParams := r.Form["resource.attr"]
		if len(filterParams) == 0 {
			http.Error(w, "at least one resource.attr parameter is required (format: resource.attr=key:value)", http.StatusBadRequest)
			return
		}

		ingesterFilters, storeFilters, err := parseResourceAttrFilters(filterParams)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Parse limit (optional)
		var limit int64
		if limitStr := r.FormValue("limit"); limitStr != "" {
			limit, err = strconv.ParseInt(limitStr, 10, 64)
			if err != nil {
				http.Error(w, "invalid limit parameter: "+err.Error(), http.StatusBadRequest)
				return
			}
		}

		// Clamp limit to the configured per-tenant max.
		if cfg.MaxResourceAttributesQueryLimit != nil {
			if maxLimit := int64(cfg.MaxResourceAttributesQueryLimit(tenantID)); maxLimit > 0 {
				if limit <= 0 || limit > maxLimit {
					limit = maxLimit
				}
			}
		}

		now := time.Now()
		nowMs := now.UnixMilli()
		var allSeries []*SeriesResourceAttributesData

		if endMs == 0 {
			endMs = nowMs
		}

		// Default startMs to 24h ago if not specified, to prevent epoch-to-now scans.
		if startMs == 0 {
			startMs = nowMs - (24 * time.Hour).Milliseconds()
		}

		// Apply max query lookback to avoid scanning all blocks since epoch.
		if cfg.MaxQueryLookback != nil {
			if maxLookback := cfg.MaxQueryLookback(tenantID); maxLookback > 0 {
				earliest := nowMs - maxLookback.Milliseconds()
				if startMs < earliest {
					startMs = earliest
				}
			}
		}

		// Clamp time range to MaxLabelsQueryLength if configured.
		if cfg.MaxLabelsQueryLength != nil {
			if maxLen := cfg.MaxLabelsQueryLength(tenantID); maxLen > 0 {
				if endMs-startMs > maxLen.Milliseconds() {
					startMs = endMs - maxLen.Milliseconds()
				}
			}
		}

		g, gCtx := errgroup.WithContext(ctx)

		// Query ingesters via distributor with filters (no matchers needed).
		// Pass limit=0 to individual sources; limit is applied post-merge.
		var ingesterResults []*ingester_client.SeriesResourceAttributes
		var queryIngestersWithin time.Duration
		if cfg.QueryIngestersWithin != nil {
			queryIngestersWithin = cfg.QueryIngestersWithin(tenantID)
		}
		shouldQueryIngesters := ShouldQueryIngesters(queryIngestersWithin, now, endMs)
		if shouldQueryIngesters {
			g.Go(func() error {
				var err error
				ingesterResults, err = d.ResourceAttributes(gCtx, startMs, endMs, nil, limit, ingesterFilters)
				return err
			})
		}

		// Query store-gateways with filters
		var storeResults []*storepb.ResourceAttributesSeriesData
		if blocksQueryable != nil && ShouldQueryBlockStore(cfg.QueryStoreAfter, now, startMs) {
			g.Go(func() error {
				var err error
				storeResults, err = blocksQueryable.ResourceAttributes(gCtx, startMs, endMs, nil, limit, storeFilters)
				return err
			})
		}

		if err := g.Wait(); err != nil {
			http.Error(w, "error querying resource attributes: "+err.Error(), http.StatusInternalServerError)
			return
		}

		ingesterConverted := convertIngesterResults(ingesterResults)
		storeConverted := convertStoreResults(storeResults)
		allSeries = mergeResourceAttributesSeries(ingesterConverted, storeConverted)

		if limit > 0 && int64(len(allSeries)) > limit {
			allSeries = allSeries[:limit]
		}

		util.WriteJSONResponse(w, ResourceAttributesResponse{
			Status: statusSuccess,
			Data: &ResourceAttributesResponseData{
				Series: allSeries,
			},
		})
	})
}

// parseResourceAttrFilters parses "key:value" filter strings into both ingester and store-gateway filter types.
func parseResourceAttrFilters(params []string) ([]*ingester_client.ResourceAttrFilter, []*storepb.ResourceAttrFilter, error) {
	ingesterFilters := make([]*ingester_client.ResourceAttrFilter, 0, len(params))
	storeFilters := make([]*storepb.ResourceAttrFilter, 0, len(params))

	for _, param := range params {
		// Find the first colon separator
		idx := -1
		for i, c := range param {
			if c == ':' {
				idx = i
				break
			}
		}
		if idx <= 0 {
			return nil, nil, fmt.Errorf("invalid resource.attr format %q: expected key:value", param)
		}

		key := param[:idx]
		value := param[idx+1:]

		ingesterFilters = append(ingesterFilters, &ingester_client.ResourceAttrFilter{
			Key:   key,
			Value: value,
		})
		storeFilters = append(storeFilters, &storepb.ResourceAttrFilter{
			Key:   key,
			Value: value,
		})
	}

	return ingesterFilters, storeFilters, nil
}
