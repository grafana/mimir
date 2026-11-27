// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	//lint:ignore faillint Allow importing the propagation package, since it's an isolated package (doesn't come with many other dependencies).
	"github.com/grafana/mimir/pkg/util/propagation"
)

// NativeTargetInfoHeader picks, per request, how info() finds target_info in the native metadata prototype: "native",
// from the ingesters' native resources, which is what tenants with native resources get without it, or "join", from
// the stored target_info series.
const NativeTargetInfoHeader = "X-Mimir-Native-Target-Info"

type nativeTargetInfoContextKey int

const (
	nativeTargetInfoModeKey = nativeTargetInfoContextKey(iota)
	nativeInfoOwnModeKey
)

// ContextWithNativeTargetInfo returns a context whose queries find target_info natively, or by the join.
func ContextWithNativeTargetInfo(ctx context.Context, native bool) context.Context {
	return context.WithValue(ctx, nativeTargetInfoModeKey, native)
}

// NativeTargetInfoFromContext returns whether the context's queries find target_info natively, and whether a request
// chose that at all: otherwise the tenant's native resources decide.
func NativeTargetInfoFromContext(ctx context.Context) (native, set bool) {
	native, set = ctx.Value(nativeTargetInfoModeKey).(bool)
	return native, set
}

// NativeTargetInfoModeExtractor carries a valid NativeTargetInfoHeader from a request to its context, for the
// query-frontend, and ignores any other value, as ConsistencyExtractor does with its headers: the queriers, which the
// query-frontend passes the header on to, reject an invalid one.
type NativeTargetInfoModeExtractor struct{}

func (e *NativeTargetInfoModeExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	switch value := carrier.Get(NativeTargetInfoHeader); value {
	case "native", "join":
		return ContextWithNativeTargetInfo(ctx, value == "native"), nil
	default:
		return ctx, nil
	}
}

// ContextWithNativeInfoOwnMode returns a context whose info() selects of info series read the stored series only:
// info() enriches the inner series it can from their own native resources, which it looks up itself, and the others
// by the join. info() decides once per query, and passes it to its selects through their context.
func ContextWithNativeInfoOwnMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, nativeInfoOwnModeKey, true)
}

// NativeInfoOwnModeFromContext returns whether info() decided on own mode for the context's selects.
func NativeInfoOwnModeFromContext(ctx context.Context) bool {
	own, _ := ctx.Value(nativeInfoOwnModeKey).(bool)
	return own
}

// NativeInfoResources is what a querier of info() function's info series offers info()'s own mode: each inner series'
// own native resource, by a lookup by series with the inner selector's matchers.
type NativeInfoResources interface {
	// NativeInfoOwnMode returns whether the query's info() takes the inner series' own resources: the tenant's switch
	// is on, and the request reads target_info natively.
	NativeInfoOwnMode(ctx context.Context) bool

	// NativeSeriesResources looks up the resources of the series matchers select over hints' range. ok is false
	// when the lookup can't answer, native resources being disabled or the lookup over a cap, in which case the
	// inner series take the join, and the fallback has been counted.
	NativeSeriesResources(ctx context.Context, hints *storage.SelectHints, matchers ...*labels.Matcher) (resources NativeSeriesResources, ok bool, err error)

	// ObserveNativeInfoInnerSeries counts count inner series that took their own resources, with source
	// NativeInfoSourceOwn, or the join, with the reason.
	ObserveNativeInfoInnerSeries(source string, count int)
}

// NativeSeriesResources is a lookup by series' answer.
type NativeSeriesResources struct {
	Series []NativeSeriesResource
	// When the lookup covers the series' changes from: before it, the stored target_info is in force.
	CoverageStart int64
	// Release gives back the memory the answers were counted for, once they're no longer needed. It's safe to call
	// more than once.
	Release func()
}

// NativeSeriesResource is a series' versions of its resource, oldest first.
type NativeSeriesResource struct {
	Labels   labels.Labels
	Versions []NativeSeriesResourceVersion
}

// NativeSeriesResourceVersion is a version of a series' resource, from MinTime until MaxTime, the next version's start
// minus one, or the series' newest sample for its last.
type NativeSeriesResourceVersion struct {
	MinTime, MaxTime int64
	// The labels of the target_info series the OTLP conversion writes for the version's resource, or none when it
	// writes none: for an unknown resource, or one of only identifying attributes.
	Labels labels.Labels
}

// The sources of NativeInfoResources.ObserveNativeInfoInnerSeries.
const (
	NativeInfoSourceOwn = "own"
	// The join's reasons: the inner expression doesn't map one selector's series one to one; the data-label selector
	// doesn't select target_info; more than one stored series maps to the inner series; the lookup didn't return the
	// series; or the lookup couldn't answer.
	NativeInfoSourceShape         = "shape"
	NativeInfoSourceNotTargetInfo = "not_target_info"
	NativeInfoSourceMerged        = "merged"
	NativeInfoSourceNotReturned   = "not_returned"
	NativeInfoSourceUnavailable   = "unavailable"
)
