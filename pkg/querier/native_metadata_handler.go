// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/distributor"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

// The native metadata prototype's lookup endpoints, which answer from the ingesters' native metadata: their series'
// metric metadata and OTel resources, as versions over time. They're experimental, and never cached.

const (
	// A lookup returns at most this many series without a limit, and at most nativeLookupMaxLimit with one.
	nativeLookupDefaultLimit = 1_000
	nativeLookupMaxLimit     = 10_000
)

// NativeMetadataDistributor looks up the ingesters' native metadata.
type NativeMetadataDistributor interface {
	NativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit int) (distributor.NativeMetricMetadataResult, error)
	NativeResources(ctx context.Context, matcherSets [][]*labels.Matcher, start, end int64, limit int, groupByTarget bool) (distributor.NativeResourcesResult, error)
	NativeSeriesResources(ctx context.Context, matcherSets [][]*labels.Matcher, start, end int64) (distributor.NativeResourcesResult, func(), error)
}

type nativeLookupResponse[R any] struct {
	Results []R `json:"results"`
	// Whether more series matched than the limit let through.
	Limited bool `json:"limited"`
	// When the ingesters' native metadata starts: changes before it aren't known.
	CoverageStartMs int64 `json:"coverage_start_ms"`
}

type nativeResourcesResult struct {
	Labels    map[string]string        `json:"labels"`
	Versions  []nativeResourcesVersion `json:"versions"`
	Coalesced bool                     `json:"coalesced"`
}

type nativeResourcesVersion struct {
	ResourceAttributes nativeResourceAttributes `json:"resource_attributes"`
	MinTimeMs          int64                    `json:"min_time_ms"`
	MaxTimeMs          int64                    `json:"max_time_ms"`
}

type nativeResourceAttributes struct {
	Identifying map[string]string `json:"identifying"`
	Descriptive map[string]string `json:"descriptive"`
}

type nativeMetricMetadataResult struct {
	Labels    map[string]string             `json:"labels"`
	Versions  []nativeMetricMetadataVersion `json:"versions"`
	Truncated bool                          `json:"truncated"`
	Coalesced bool                          `json:"coalesced"`
}

type nativeMetricMetadataVersion struct {
	Type      string `json:"type"`
	Unit      string `json:"unit"`
	Help      string `json:"help"`
	MinTimeMs int64  `json:"min_time_ms"`
	MaxTimeMs int64  `json:"max_time_ms"`
}

// NativeResourcesHandler serves GET /api/v1/resources, in #17740's shape: the OTel resources of the series matching
// any of match[], as versions over [start, end], at most limit series, and with latest, only each series' last version.
func NativeResourcesHandler(d NativeMetadataDistributor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params, ok := parseNativeLookupParams(w, r, true)
		if !ok {
			return
		}
		res, err := d.NativeResources(r.Context(), params.matcherSets, params.start, params.end, params.limit, false)
		if err != nil {
			writeNativeLookupError(w, err)
			return
		}
		out := nativeLookupResponse[nativeResourcesResult]{Results: make([]nativeResourcesResult, 0, len(res.Series)), Limited: res.Limited, CoverageStartMs: res.CoverageStart}
		for _, s := range res.Series {
			versions := s.Versions
			if params.latest && len(versions) > 0 {
				versions = versions[len(versions)-1:]
			}
			result := nativeResourcesResult{Labels: labelsMap(s.Labels), Versions: make([]nativeResourcesVersion, 0, len(versions)), Coalesced: s.Coalesced}
			for _, v := range versions {
				result.Versions = append(result.Versions, nativeResourcesVersion{
					ResourceAttributes: nativeResourceAttributes{Identifying: v.Identifying, Descriptive: v.Descriptive},
					MinTimeMs:          v.MinTimeMs,
					MaxTimeMs:          v.MaxTimeMs,
				})
			}
			out.Results = append(out.Results, result)
		}
		writeNativeLookupResponse(w, out)
	})
}

// NativeMetricMetadataHandler serves GET /api/v1/native_metric_metadata: the series' own metric metadata, its type,
// unit and help, of the series matching any of match[], as versions over time, at most limit series, and with latest,
// only each series' last version.
func NativeMetricMetadataHandler(d NativeMetadataDistributor) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		params, ok := parseNativeLookupParams(w, r, false)
		if !ok {
			return
		}
		res, err := d.NativeMetricMetadata(r.Context(), params.matcherSets, params.limit)
		if err != nil {
			writeNativeLookupError(w, err)
			return
		}
		out := nativeLookupResponse[nativeMetricMetadataResult]{Results: make([]nativeMetricMetadataResult, 0, len(res.Series)), Limited: res.Limited, CoverageStartMs: res.CoverageStart}
		for _, s := range res.Series {
			versions := s.Versions
			if params.latest && len(versions) > 0 {
				versions = versions[len(versions)-1:]
			}
			result := nativeMetricMetadataResult{Labels: labelsMap(s.Labels), Versions: make([]nativeMetricMetadataVersion, 0, len(versions)), Truncated: s.Truncated, Coalesced: s.Coalesced}
			for _, v := range versions {
				result.Versions = append(result.Versions, nativeMetricMetadataVersion{
					Type:      string(mimirpb.MetricMetadataMetricTypeToMetricType(v.Type)),
					Unit:      v.Unit,
					Help:      v.Help,
					MinTimeMs: v.MinTimeMs,
					MaxTimeMs: v.MaxTimeMs,
				})
			}
			out.Results = append(out.Results, result)
		}
		writeNativeLookupResponse(w, out)
	})
}

type nativeLookupParams struct {
	matcherSets [][]*labels.Matcher
	start, end  int64
	limit       int
	latest      bool
}

// parseNativeLookupParams parses the request's parameters, writing a bad request if they aren't valid. Every match[]
// needs a matcher that doesn't match the empty string, as /api/v1/series does, so that a lookup can't go through every
// series of a tenant.
func parseNativeLookupParams(w http.ResponseWriter, r *http.Request, timeRange bool) (nativeLookupParams, bool) {
	badRequest := func(err error) (nativeLookupParams, bool) {
		writeNativeLookupJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "errorType": "bad_data", "error": err.Error()})
		return nativeLookupParams{}, false
	}
	if _, err := tenant.TenantID(r.Context()); err != nil {
		return badRequest(err)
	}
	if err := r.ParseForm(); err != nil {
		return badRequest(err)
	}
	params := nativeLookupParams{start: math.MinInt64, end: math.MaxInt64, limit: nativeLookupDefaultLimit}
	var err error
	if params.matcherSets, err = parseSearchMatchers(r.Form["match[]"]); err != nil {
		return badRequest(err)
	}
	if len(params.matcherSets) == 0 {
		return badRequest(fmt.Errorf("no match[] parameter provided"))
	}
	for _, matchers := range params.matcherSets {
		if !slices.ContainsFunc(matchers, func(m *labels.Matcher) bool { return !m.Matches("") }) {
			return badRequest(fmt.Errorf("match[] must contain at least one non-empty matcher"))
		}
	}
	if timeRange {
		if s := r.FormValue("start"); s != "" {
			if params.start, err = util.ParseTime(s); err != nil {
				return badRequest(fmt.Errorf("invalid start: %w", err))
			}
		}
		if s := r.FormValue("end"); s != "" {
			if params.end, err = util.ParseTime(s); err != nil {
				return badRequest(fmt.Errorf("invalid end: %w", err))
			}
		}
	}
	if s := r.FormValue("limit"); s != "" {
		limit, err := strconv.Atoi(s)
		if err != nil || limit <= 0 || limit > nativeLookupMaxLimit {
			return badRequest(fmt.Errorf("limit must be a positive number up to %d", nativeLookupMaxLimit))
		}
		params.limit = limit
	}
	if s := r.FormValue("latest"); s != "" {
		if params.latest, err = strconv.ParseBool(s); err != nil {
			return badRequest(fmt.Errorf("invalid latest: %w", err))
		}
	}
	return params, true
}

func writeNativeLookupError(w http.ResponseWriter, err error) {
	switch {
	case ingester_client.IsNativeMetadataDisabled(err):
		writeNativeLookupJSON(w, http.StatusBadRequest, map[string]string{"status": "error", "errorType": "bad_data", "error": "native metadata is disabled for the tenant"})
	case ingester_client.IsNativeLookupTooManySeries(err):
		writeNativeLookupJSON(w, http.StatusUnprocessableEntity, map[string]string{"status": "error", "errorType": "execution", "error": err.Error()})
	default:
		respondFromError(err, w)
	}
}

func writeNativeLookupResponse(w http.ResponseWriter, data any) {
	writeNativeLookupJSON(w, http.StatusOK, map[string]any{"status": "success", "data": data})
}

func writeNativeLookupJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func labelsMap(lbls []mimirpb.LabelAdapter) map[string]string {
	out := make(map[string]string, len(lbls))
	for _, l := range lbls {
		out[l.Name] = l.Value
	}
	return out
}
