// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import "context"

// QueryPostProcessor is invoked after a query has executed successfully.
//
// It can be used to observe the outcome of a query, for example to populate a cache from the query
// stats. Post-processors are not invoked if the query fails, and must not modify the query result.
type QueryPostProcessor interface {
	// PostProcess is called once, after the query has executed successfully. Implementations should
	// read whatever they need (for example the query stats) from ctx.
	PostProcess(ctx context.Context) error
}
