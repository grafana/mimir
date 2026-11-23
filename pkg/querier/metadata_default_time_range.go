// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/validation"
)

// formMaxMemory is the net/http default that r.FormValue uses to parse a multipart body.
const formMaxMemory = 32 << 20

type metadataDefaultTimeRangeWarningKey struct{}

// MetadataDefaultTimeRange returns the default time range for series, label names, label values and search requests
// that do not specify start or end. A request for many tenants uses the smallest non-zero value of the tenants.
// It returns 0 when no default applies, or when the context has no tenant.
func MetadataDefaultTimeRange(ctx context.Context, limits *validation.Overrides) time.Duration {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		// The handler reports the missing tenant.
		return 0
	}
	return validation.SmallestPositiveNonZeroDurationPerTenant(tenantIDs, limits.MetadataDefaultTimeRange)
}

// applyMetadataDefaultTimeRange replaces a missing start or end with the rules of the Prometheus
// metadata-default-time-range feature, so that Mimir and Prometheus return the same range.
func applyMetadataDefaultTimeRange(startMs, endMs int64, hasStart, hasEnd bool, nowMs int64, d time.Duration) (int64, int64) {
	if !hasEnd {
		endMs = nowMs
	}
	if !hasStart {
		startMs = endMs - d.Milliseconds()
	}
	return startMs, endMs
}

// newMetadataDefaultTimeRangeWarning returns the warning that Prometheus returns when it applies a default range.
func newMetadataDefaultTimeRangeWarning(startMs, endMs int64) error {
	return fmt.Errorf("start or end not given, applied a default: start=%s, end=%s",
		time.UnixMilli(startMs).UTC().Format(time.RFC3339Nano), time.UnixMilli(endMs).UTC().Format(time.RFC3339Nano))
}

// NewMetadataDefaultTimeRangeMiddleware sets the per-tenant default start and end on series, label names and label
// values requests that do not specify them. Prometheus has the same feature, but it uses one value for all tenants.
// The middleware puts the warning in the request context. NewMetadataDefaultTimeRangeQueryable adds it to the response.
func NewMetadataDefaultTimeRangeMiddleware(limits *validation.Overrides) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d := MetadataDefaultTimeRange(r.Context(), limits)
			if d <= 0 {
				next.ServeHTTP(w, r)
				return
			}

			// Prometheus cannot parse the form again after this, because the body is consumed. Thus the error must
			// be reported here.
			if err := r.ParseForm(); err != nil {
				writeMetadataDefaultTimeRangeError(w, fmt.Errorf("error parsing form values: %w", err))
				return
			}
			// Prometheus reads start and end with r.FormValue, which also reads a multipart body and ignores errors.
			_ = r.ParseMultipartForm(formMaxMemory)

			startParam, endParam := r.Form.Get("start"), r.Form.Get("end")
			hasStart, hasEnd := startParam != "", endParam != ""
			if hasStart && hasEnd {
				next.ServeHTTP(w, r)
				return
			}

			var startMs, endMs int64
			var err error
			if hasStart {
				if startMs, err = util.ParseTime(startParam); err != nil {
					// Prometheus reports the invalid value.
					next.ServeHTTP(w, r)
					return
				}
			}
			if hasEnd {
				if endMs, err = util.ParseTime(endParam); err != nil {
					next.ServeHTTP(w, r)
					return
				}
			}

			startMs, endMs = applyMetadataDefaultTimeRange(startMs, endMs, hasStart, hasEnd, time.Now().UnixMilli(), d)
			if !hasStart {
				r.Form.Set("start", time.UnixMilli(startMs).UTC().Format(time.RFC3339Nano))
			}
			if !hasEnd {
				r.Form.Set("end", time.UnixMilli(endMs).UTC().Format(time.RFC3339Nano))
			}

			ctx := context.WithValue(r.Context(), metadataDefaultTimeRangeWarningKey{}, newMetadataDefaultTimeRangeWarning(startMs, endMs))
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
}

func writeMetadataDefaultTimeRangeError(w http.ResponseWriter, err error) {
	apiErr := apierror.New(apierror.TypeBadData, err.Error())
	// EncodeJSON marshals a struct of strings, which cannot fail.
	body, _ := apiErr.EncodeJSON()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(apiErr.StatusCode())
	_, _ = w.Write(body)
}

func metadataDefaultTimeRangeWarningFromContext(ctx context.Context) error {
	warning, _ := ctx.Value(metadataDefaultTimeRangeWarningKey{}).(error)
	return warning
}

// NewMetadataDefaultTimeRangeQueryable returns a queryable that adds the warning from
// NewMetadataDefaultTimeRangeMiddleware to label names, label values and series results.
func NewMetadataDefaultTimeRangeQueryable(q storage.SampleAndChunkQueryable) storage.SampleAndChunkQueryable {
	return metadataDefaultTimeRangeQueryable{SampleAndChunkQueryable: q}
}

type metadataDefaultTimeRangeQueryable struct {
	storage.SampleAndChunkQueryable
}

func (q metadataDefaultTimeRangeQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	querier, err := q.SampleAndChunkQueryable.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return metadataDefaultTimeRangeQuerier{Querier: querier}, nil
}

type metadataDefaultTimeRangeQuerier struct {
	storage.Querier
}

func (q metadataDefaultTimeRangeQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	values, warnings, err := q.Querier.LabelValues(ctx, name, hints, matchers...)
	if warning := metadataDefaultTimeRangeWarningFromContext(ctx); warning != nil {
		warnings.Add(warning)
	}
	return values, warnings, err
}

func (q metadataDefaultTimeRangeQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	names, warnings, err := q.Querier.LabelNames(ctx, hints, matchers...)
	if warning := metadataDefaultTimeRangeWarningFromContext(ctx); warning != nil {
		warnings.Add(warning)
	}
	return names, warnings, err
}

func (q metadataDefaultTimeRangeQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	set := q.Querier.Select(ctx, sortSeries, hints, matchers...)
	// The series endpoint sets Func to "series". Other callers, such as PromQL, do not get the warning.
	if hints == nil || hints.Func != "series" {
		return set
	}
	warning := metadataDefaultTimeRangeWarningFromContext(ctx)
	if warning == nil {
		return set
	}
	var warnings annotations.Annotations
	warnings.Add(warning)
	return series.NewSeriesSetWithWarnings(set, warnings)
}
