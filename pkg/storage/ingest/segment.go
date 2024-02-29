// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import "github.com/oklog/ulid"

type Segment struct {
	// PartitionID is the partition ID.
	PartitionID int32

	// CommitID is a sequential commit ID, starting from 0. The commit ID is unique per partition
	// and the metadata store guarantees no gaps between commit IDs (e.g. if commit ID 2 exists, then
	// commit ID 1 must exist too).
	CommitID int64

	// ObjectID is the unique identifier of the segment object in the object storage.
	ObjectID ulid.ULID
}
