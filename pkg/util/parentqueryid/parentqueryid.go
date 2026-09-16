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
// The ID is unique within one query-frontend process only. Zero means unknown.
package parentqueryid

import "context"

// FieldName is the name to be used as a log field and as a trace span attribute
const FieldName = "parent_query_id"

type contextKey int

var ctxKey = contextKey(0)

// ContextWithID returns a context carrying the given parent query ID.
func ContextWithID(ctx context.Context, parentQueryID uint64) context.Context {
	return context.WithValue(ctx, ctxKey, parentQueryID)
}

// IDFromContext returns the parent query ID held in the context, or zero if there is none.
func IDFromContext(ctx context.Context) uint64 {
	parentQueryID, ok := ctx.Value(ctxKey).(uint64)
	if !ok {
		return 0
	}
	return parentQueryID
}

// AppendLogFields appends the parent query ID to fields. It returns fields unchanged when the ID is
// zero, because zero means unknown. An unknown parent must not read as query 0.
func AppendLogFields(fields []any, parentQueryID uint64) []any {
	if parentQueryID == 0 {
		return fields
	}
	return append(fields, FieldName, parentQueryID)
}
