// SPDX-License-Identifier: AGPL-3.0-only

// Package parentqueryid carries the identifier of the user query that a request belongs to.
//
// The query-frontend splits and shards one user query into many sub-requests. It sends each
// sub-request to the query-scheduler, and a querier evaluates each one separately.
//
// The query-frontend's HTTP transport handler allocates the ID once per user query. Every
// sub-request then carries it. Log lines and trace spans in the read path report it, so you can
// attribute them to the query the user sent.
//
// An empty ID means unknown.
package parentqueryid

import (
	"context"

	"github.com/google/uuid"
)

// FieldName is the name to be used as a log field and as a trace span attribute
const FieldName = "parent_query_id"

type contextKey int

var ctxKey = contextKey(0)

// New returns a new parent query ID.
//
// The ID is a random UUID rather than a counter value. The query-frontend reports the ID to the
// caller in the query stats response header, so a counter would tell one tenant how many queries
// the query-frontend served for all other tenants.
func New() string {
	return uuid.NewString()
}

// ContextWithID returns a context carrying the given parent query ID.
func ContextWithID(ctx context.Context, parentQueryID string) context.Context {
	return context.WithValue(ctx, ctxKey, parentQueryID)
}

// IDFromContext returns the parent query ID held in the context, or an empty string if there is
// none.
func IDFromContext(ctx context.Context) string {
	parentQueryID, ok := ctx.Value(ctxKey).(string)
	if !ok {
		return ""
	}
	return parentQueryID
}

// AppendLogFields appends the parent query ID to fields. It returns fields unchanged when the ID is
// empty, because an empty ID means unknown. An unknown parent must not read as a real query.
func AppendLogFields(fields []any, parentQueryID string) []any {
	if parentQueryID == "" {
		return fields
	}
	return append(fields, FieldName, parentQueryID)
}
