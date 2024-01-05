// SPDX-License-Identifier: AGPL-3.0-only

package api

const (
	// ReadConsistencyStrong means that a query sent by the same client will always observe the writes
	// that have completed before issuing the query.
	ReadConsistencyStrong = "strong"

	// ReadConsistencyEventual is the default consistency level for all queries.
	// This level means that a query sent by a client may not observe some of the writes that the same client has recently made.
	ReadConsistencyEventual = "eventual"
)

var ReadConsistencies = []string{ReadConsistencyStrong, ReadConsistencyEventual}
