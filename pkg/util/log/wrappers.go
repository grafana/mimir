// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/log/wrappers.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package log

import (
	"context"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/tracing"
)

// levelWrapper implements leveledLogger and log.Logger.
type levelWrapper struct {
	lvl        logLevel
	log.Logger // Calls to Log() will fall through to here.
}

func wrap(original, logger log.Logger) log.Logger {
	lvl := debugLevel // default if unknown
	if x, ok := original.(leveledLogger); ok {
		lvl = x.level()
	}
	return levelWrapper{lvl: lvl, Logger: logger}
}

func (w levelWrapper) level() logLevel {
	return w.lvl
}

// WithUserID returns a Logger that has information about the current user in
// its details.
func WithUserID(userID string, l log.Logger) log.Logger {
	// See note in WithContext.
	return wrap(l, log.With(l, "user", userID))
}

// WithUserIDs returns a Logger that has information about the current user or
// users (separated by "|") in its details.
func WithUserIDs(userIDs []string, l log.Logger) log.Logger {
	return wrap(l, log.With(l, "user", tenant.JoinTenantIDs(userIDs)))
}

// WithTraceID returns a Logger that has information about the traceID in
// its details.
func WithTraceID(traceID string, l log.Logger) log.Logger {
	// See note in WithContext.
	return wrap(l, log.With(l, "traceID", traceID))
}

// WithContext returns a Logger that has information about the current user or users
// and trace in its details.
//
// e.g.
//
//	log = util.WithContext(ctx, log)
//	# level=error user=user-1|user-2 traceID=123abc msg="Could not chunk chunks" err="an error"
//	level.Error(log).Log("msg", "Could not chunk chunks", "err", err)
func WithContext(ctx context.Context, l log.Logger) log.Logger {
	// Weaveworks uses "orgs" and "orgID" to represent Cortex users,
	// even though the code-base generally uses `userID` to refer to the same thing.
	userIDs, err := tenant.TenantIDs(ctx)
	if err == nil {
		l = WithUserIDs(userIDs, l)
	}

	traceID, ok := tracing.ExtractSampledTraceID(ctx)
	if !ok {
		return l
	}

	return WithTraceID(traceID, l)
}

// WithSourceIPs returns a Logger that has information about the source IPs in
// its details.
func WithSourceIPs(sourceIPs string, l log.Logger) log.Logger {
	return wrap(l, log.With(l, "sourceIPs", sourceIPs))
}
