// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"path"
	"strconv"
	"time"

	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

// SegmentStorage is a client to the segments storage.
type SegmentStorage struct {
	bucket   objstore.Bucket
	metadata *MetadataStore
	random   *rand.Rand
}

func NewSegmentStorage(bucket objstore.Bucket, metadata *MetadataStore) *SegmentStorage {
	return &SegmentStorage{
		bucket:   bucket,
		metadata: metadata,
		random:   rand.New(rand.NewSource(time.Now().UnixMilli())),
	}
}

// CommitSegment uploads and commits a segment to the storage.
func (s *SegmentStorage) CommitSegment(ctx context.Context, segment *ingestpb.Segment) (SegmentRef, error) {
	// Marshal the segment.
	segmentData, err := segment.Marshal()
	if err != nil {
		return SegmentRef{}, errors.Wrap(err, "failed to marshal segment")
	}

	// Upload the segment to the object storage.
	objectID, err := ulid.New(uint64(time.Now().UnixMilli()), s.random)
	if err != nil {
		return SegmentRef{}, errors.Wrap(err, "failed to generate segment object ID")
	}
	objectPath := getSegmentObjectPath(segment.PartitionId, objectID)

	if err := s.bucket.Upload(ctx, objectPath, bytes.NewReader(segmentData)); err != nil {
		return SegmentRef{}, errors.Wrapf(err, "failed to upload segment object %s", objectPath)
	}

	// Commit it to the metadata store after it has been successfully uploaded.
	return s.metadata.AddSegment(ctx, segment.PartitionId, objectID)
}

// FetchSegment reads a segment from the storage.
func (s *SegmentStorage) FetchSegment(ctx context.Context, ref SegmentRef) (_ *ingestpb.Segment, returnErr error) {
	objectPath := getSegmentObjectPath(ref.PartitionID, ref.ObjectID)

	reader, err := s.bucket.Get(ctx, objectPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read segment object %s", objectPath)
	}

	// Ensure we close the reader once done.
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && returnErr == nil {
			returnErr = closeErr
		}
	}()

	// Read all the segment object content and then unmarshal it.
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read segment object %s", objectPath)
	}

	segment := &ingestpb.Segment{}
	if err := segment.Unmarshal(data); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal segment object %s", objectPath)
	}

	return segment, nil
}

// getSegmentObjectPath returns the path of the segment object in the object storage.
func getSegmentObjectPath(partitionID int32, objectID ulid.ULID) string {
	return path.Join(strconv.Itoa(int(partitionID)), objectID.String())
}
