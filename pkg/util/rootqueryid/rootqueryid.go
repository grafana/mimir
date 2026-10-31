// SPDX-License-Identifier: AGPL-3.0-only

// Package rootqueryid carries the identifier of the user query that a request belongs to.
//
// The query-frontend splits and shards one user query into many sub-requests. It sends each
// sub-request to the query-scheduler, and a querier evaluates each one separately.
//
// The query-frontend's HTTP transport handler allocates the ID once per user query. Every
// sub-request then carries it. Log lines and trace spans in the read path report it, so you can
// attribute them to the query the user sent.
//
// An empty ID means unknown.
package rootqueryid

import (
	"context"

	"github.com/google/uuid"
)

// FieldName is the name to be used as a log field and as a trace span attribute
const FieldName = "root_query_id"

type contextKey int

var ctxKey = contextKey(0)

// New returns a new root query ID.
//
// The ID is a random UUID rather than a counter value. The query-frontend reports the ID to the
// caller in the query stats response header, so a counter would tell one tenant how many queries
// the query-frontend served for all other tenants.
func New() string {
	return uuid.NewString()
}

// ContextWithID returns a context carrying the given root query ID.
func ContextWithID(ctx context.Context, rootQueryID string) context.Context {
	return context.WithValue(ctx, ctxKey, rootQueryID)
}

// IDFromContext returns the root query ID held in the context, or an empty string if there is
// none.
func IDFromContext(ctx context.Context) string {
	rootQueryID, ok := ctx.Value(ctxKey).(string)
	if !ok {
		return ""
	}
	return rootQueryID
}

// AppendLogFields appends the root query ID to fields. It returns fields unchanged when the ID is
// empty, because an empty ID means unknown. An unknown root must not read as a real query.
func AppendLogFields(fields []any, rootQueryID string) []any {
	if rootQueryID == "" {
		return fields
	}
	return append(fields, FieldName, rootQueryID)
}
