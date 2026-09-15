// SPDX-License-Identifier: AGPL-3.0-only

// Package parentquery carries the identifier of the user query that a request belongs to.
//
// A single user query becomes many sub-requests: the query-frontend splits and shards it, and each
// resulting sub-request is enqueued to the query-scheduler and evaluated by a querier separately.
// The parent query ID is allocated once per user query by the query-frontend's HTTP transport
// handler and travels with every sub-request, so that log lines and trace spans emitted anywhere in
// the read path can be attributed back to the query the user issued.
//
// The ID is unique within a single query-frontend process only. Zero means unknown.
package parentquery

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

// AppendLogFields appends the parent query ID to fields, or returns fields unchanged when the ID is
// zero. Zero means unknown, so it is left out rather than attributing the request to query 0.
func AppendLogFields(fields []any, parentQueryID uint64) []any {
	if parentQueryID == 0 {
		return fields
	}
	return append(fields, FieldName, parentQueryID)
}
