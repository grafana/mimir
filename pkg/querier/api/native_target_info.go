// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"

	//lint:ignore faillint Allow importing the propagation package, since it's an isolated package (doesn't come with many other dependencies).
	"github.com/grafana/mimir/pkg/util/propagation"
)

// NativeTargetInfoHeader picks, per request, how info() finds target_info in the native metadata prototype: "native",
// from the ingesters' native resources, which is what tenants with native resources get without it, or "join", from
// the stored target_info series.
const NativeTargetInfoHeader = "X-Mimir-Native-Target-Info"

type nativeTargetInfoContextKey int

const nativeTargetInfoModeKey = nativeTargetInfoContextKey(0)

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
