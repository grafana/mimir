// SPDX-License-Identifier: AGPL-3.0-only

package index

import "context"

// Reader reads the Parquet index for an individual Parquet block.
// The interface is independent of the underlying storage medium and access patterns.
type Reader interface {
	// Close releases the resources of the Reader.
	Close() error

	// IndexVersion returns version of index.
	IndexVersion(context.Context) (string, error)
}
