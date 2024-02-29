// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import "github.com/oklog/ulid"

type Segment struct {
	// PartitionID is the partition ID.
	PartitionID int32

	// OffsetID is a sequential ID, starting from 0. The offset ID is unique per partition
	// and the metadata store guarantees no gaps between offset IDs (e.g. if offset ID 2 exists, then
	// offset ID 1 must exist too).
	OffsetID int64

	// ObjectID is the unique identifier of the segment object in the object storage.
	ObjectID ulid.ULID
}
