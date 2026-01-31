// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/sync/errgroup"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
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
	ResourceAttributes(ctx context.Context, minT, maxT int64, matchers []*labels.Matcher, limit int64) ([]*storepb.ResourceAttributesSeriesData, error)
}

// ResourceAttributesHandlerConfig holds configuration for resource attributes handler.
type ResourceAttributesHandlerConfig struct {
	QueryStoreAfter      time.Duration
	QueryIngestersWithin func(userID string) time.Duration
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
			util.WriteJSONResponse(w, ResourceAttributesResponse{
				Status: statusError,
				Error:  "error parsing request form: " + err.Error(),
			})
			return
		}

		// Parse time range
		startMs, endMs, err := parseResourceTimeRange(r)
		if err != nil {
			util.WriteJSONResponse(w, ResourceAttributesResponse{
				Status: statusError,
				Error:  err.Error(),
			})
			return
		}

		// Parse matchers
		matcherSets := r.Form["match[]"]
		if len(matcherSets) == 0 {
			util.WriteJSONResponse(w, ResourceAttributesResponse{
				Status: statusError,
				Error:  "at least one matcher is required (use {__name__=~\".+\"} for all series)",
			})
			return
		}

		var allMatchers []*labels.Matcher
		for _, matcherSet := range matcherSets {
			matchers, err := parser.ParseMetricSelector(matcherSet)
			if err != nil {
				util.WriteJSONResponse(w, ResourceAttributesResponse{
					Status: statusError,
					Error:  "error parsing matcher: " + err.Error(),
				})
				return
			}
			allMatchers = append(allMatchers, matchers...)
		}

		// Parse limit (optional)
		var limit int64
		if limitStr := r.FormValue("limit"); limitStr != "" {
			limit, err = strconv.ParseInt(limitStr, 10, 64)
			if err != nil {
				util.WriteJSONResponse(w, ResourceAttributesResponse{
					Status: statusError,
					Error:  "invalid limit parameter: " + err.Error(),
				})
				return
			}
		}

		now := time.Now()
		nowMs := now.UnixMilli()
		var allSeries []*SeriesResourceAttributesData

		// Default endMs to now if not specified
		if endMs == 0 {
			endMs = nowMs
		}

		// Query both ingesters and store-gateways in parallel
		g, gCtx := errgroup.WithContext(ctx)

		// Query ingesters via distributor
		var ingesterResults []*ingester_client.SeriesResourceAttributes
		var queryIngestersWithin time.Duration
		if cfg.QueryIngestersWithin != nil {
			queryIngestersWithin = cfg.QueryIngestersWithin(tenantID)
		}
		shouldQueryIngesters := ShouldQueryIngesters(queryIngestersWithin, now, endMs)
		if shouldQueryIngesters {
			g.Go(func() error {
				var err error
				ingesterResults, err = d.ResourceAttributes(gCtx, startMs, endMs, allMatchers, limit)
				return err
			})
		}

		// Query store-gateways via blocks queryable
		var storeResults []*storepb.ResourceAttributesSeriesData
		if blocksQueryable != nil && ShouldQueryBlockStore(cfg.QueryStoreAfter, now, startMs) {
			g.Go(func() error {
				var err error
				storeResults, err = blocksQueryable.ResourceAttributes(gCtx, startMs, endMs, allMatchers, limit)
				return err
			})
		}

		// Wait for both queries to complete
		if err := g.Wait(); err != nil {
			util.WriteJSONResponse(w, ResourceAttributesResponse{
				Status: statusError,
				Error:  "error querying resource attributes: " + err.Error(),
			})
			return
		}

		// Convert and merge results from both sources
		ingesterConverted := convertIngesterResults(ingesterResults)
		storeConverted := convertStoreResults(storeResults)

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

	// Add all ingester series to the map
	for _, s := range ingesterSeries {
		key := labelsMapToKey(s.Labels)
		seriesMap[key] = s
	}

	// Merge store series with existing ingester series
	for _, s := range storeSeries {
		key := labelsMapToKey(s.Labels)
		if existing, ok := seriesMap[key]; ok {
			// Merge versions from both sources
			existing.Versions = mergeResourceVersions(existing.Versions, s.Versions)
		} else {
			seriesMap[key] = s
		}
	}

	// Convert map back to slice
	result := make([]*SeriesResourceAttributesData, 0, len(seriesMap))
	for _, s := range seriesMap {
		result = append(result, s)
	}

	// Sort by labels for consistent ordering
	sort.Slice(result, func(i, j int) bool {
		return labelsMapToKey(result[i].Labels) < labelsMapToKey(result[j].Labels)
	})

	return result
}

// labelsMapToKey creates a string key from a labels map for deduplication.
func labelsMapToKey(lbls map[string]string) string {
	// Get sorted keys
	keys := make([]string, 0, len(lbls))
	for k := range lbls {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build key string
	var key string
	for _, k := range keys {
		key += k + "=" + lbls[k] + ","
	}
	return key
}

// mergeResourceVersions merges and deduplicates resource versions from two sources.
func mergeResourceVersions(a, b []*ResourceVersionData) []*ResourceVersionData {
	// Simple merge: append and sort by time
	// In the future, we could deduplicate overlapping time ranges
	all := append(a, b...)

	// Sort by MinTimeMs
	sort.Slice(all, func(i, j int) bool {
		return all[i].MinTimeMs < all[j].MinTimeMs
	})

	// Deduplicate versions with same time range
	if len(all) == 0 {
		return all
	}

	result := []*ResourceVersionData{all[0]}
	for i := 1; i < len(all); i++ {
		last := result[len(result)-1]
		// Skip if same time range (keep the first one, which is from ingesters)
		if all[i].MinTimeMs == last.MinTimeMs && all[i].MaxTimeMs == last.MaxTimeMs {
			continue
		}
		result = append(result, all[i])
	}

	return result
}
