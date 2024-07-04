// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"fmt"
	"slices"
)

const (
	ReadConsistencyHeader = "X-Read-Consistency"

	// ReadConsistencyStrong means that a query sent by the same client will always observe the writes
	// that have completed before issuing the query.
	ReadConsistencyStrong = "strong"

	// ReadConsistencyEventual is the default consistency level for all queries.
	// This level means that a query sent by a client may not observe some of the writes that the same client has recently made.
	ReadConsistencyEventual = "eventual"
)

var ReadConsistencies = []string{ReadConsistencyStrong, ReadConsistencyEventual}

func IsValidReadConsistency(lvl string) error {
	if slices.Contains(ReadConsistencies, lvl) {
		return nil
	}
	return fmt.Errorf("consistency level must be one of %v", ReadConsistencies)
}

// ContextWithReadConsistency returns a new context with the given consistency level.
// The consistency level can be retrieved with ReadConsistencyFromContext.
func ContextWithReadConsistency(parent context.Context, level string) context.Context {
	return ContextWithAHeaderOption(parent, ReadConsistencyHeader, level)
}

// ReadConsistencyFromContext returns the consistency level from the context if set via ContextWithReadConsistency.
// The second return value is true if the consistency level was found in the context and is valid.
func ReadConsistencyFromContext(ctx context.Context) (string, bool) {
	return HeaderOptionFromContext(ctx, ReadConsistencyHeader)
}
