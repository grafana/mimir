// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/bucketindex/markers.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package bucketindex

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/oklog/ulid"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

const (
	MarkersPathname = "markers"
)

func markFilepath(blockID ulid.ULID, markFilename string) string {
	return fmt.Sprintf("%s/%s-%s", MarkersPathname, blockID.String(), markFilename)
}

func isMarkFilename(name string, markFilename string) (ulid.ULID, bool) {
	parts := strings.SplitN(name, "-", 2)
	if len(parts) != 2 {
		return ulid.ULID{}, false
	}

	// Ensure the 2nd part matches the mark filename.
	if parts[1] != markFilename {
		return ulid.ULID{}, false
	}

	// Ensure the 1st part is a valid block ID.
	id, err := ulid.Parse(filepath.Base(parts[0]))
	return id, err == nil
}

// BlockDeletionMarkFilepath returns the path, relative to the tenant's bucket location,
// of a block deletion mark in the bucket markers location.
func BlockDeletionMarkFilepath(blockID ulid.ULID) string {
	return markFilepath(blockID, metadata.DeletionMarkFilename)
}

// IsBlockDeletionMarkFilename returns whether the input filename matches the expected pattern
// of block deletion markers stored in the markers location.
func IsBlockDeletionMarkFilename(name string) (ulid.ULID, bool) {
	return isMarkFilename(name, metadata.DeletionMarkFilename)
}

// NoCompactMarkFilepath returns the path, relative to the tenant's bucket location,
// of a no-compact block mark in the bucket markers location.
func NoCompactMarkFilepath(blockID ulid.ULID) string {
	return markFilepath(blockID, metadata.NoCompactMarkFilename)
}

// IsNoCompactMarkFilename returns true if input filename matches the expected
// pattern of block marker stored in the markers location.
func IsNoCompactMarkFilename(name string) (ulid.ULID, bool) {
	return isMarkFilename(name, metadata.NoCompactMarkFilename)
}
