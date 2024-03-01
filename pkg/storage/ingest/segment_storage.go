// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"path"
	"strconv"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

// SegmentStorage is a low-level client to write and read segments to/from the storage.
// Use SegmentReader if you need an higher level client to read segments.
type SegmentStorage struct {
	bucket   objstore.Bucket
	metadata *MetadataStore
}

func NewSegmentStorage(bucket objstore.Bucket, metadata *MetadataStore) *SegmentStorage {
	return &SegmentStorage{
		bucket:   bucket,
		metadata: metadata,
	}
}

// CommitSegment uploads and commits a segment to the storage.
func (s *SegmentStorage) CommitSegment(ctx context.Context, partitionID int32, segmentData *ingestpb.Segment, now time.Time) (SegmentRef, error) {
	// Marshal the segment.
	rawData, err := segmentData.Marshal()
	if err != nil {
		return SegmentRef{}, errors.Wrap(err, "failed to marshal segment")
	}

	// Upload the segment to the object storage.
	objectID, err := ulid.New(uint64(now.UnixMilli()), rand.Reader)
	if err != nil {
		return SegmentRef{}, errors.Wrap(err, "failed to generate segment object ID")
	}
	objectPath := getSegmentObjectPath(partitionID, objectID)

	if err := s.bucket.Upload(ctx, objectPath, bytes.NewReader(rawData)); err != nil {
		return SegmentRef{}, errors.Wrapf(err, "failed to upload segment object %s", objectPath)
	}

	// Commit it to the metadata store after it has been successfully uploaded.
	return s.metadata.CommitSegment(ctx, partitionID, objectID, now)
}

// FetchSegment reads a segment from the storage.
func (s *SegmentStorage) FetchSegment(ctx context.Context, ref SegmentRef) (_ *Segment, returnErr error) {
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
	segmentData, err := readSegmentObject(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read segment object %s", objectPath)
	}

	return &Segment{
		Ref:  ref,
		Data: segmentData,
	}, nil
}

// DeleteSegment deletes a segment from storage.
func (s *SegmentStorage) DeleteSegment(ctx context.Context, ref SegmentRef) error {
	// First delete it from the metadata store.
	if err := s.metadata.DeleteSegment(ctx, ref); err != nil {
		return errors.Wrap(err, "failed to delete segment from metadata store")
	}

	// Then we can delete it from object storage.
	objectPath := getSegmentObjectPath(ref.PartitionID, ref.ObjectID)

	if err := s.bucket.Delete(ctx, objectPath); err != nil {
		return errors.Wrap(err, "failed to delete segment from object storage")
	}

	return nil
}

// FetchSegmentWithRetries is like FetchSegment but retries few times on failure.
func (s *SegmentStorage) FetchSegmentWithRetries(ctx context.Context, ref SegmentRef) (segment *Segment, returnErr error) {
	try := backoff.New(ctx, backoff.Config{
		MinBackoff: 100 * time.Millisecond,
		MaxBackoff: 500 * time.Millisecond,
		MaxRetries: 3,
	})

	for try.Ongoing() {
		segment, returnErr = s.FetchSegment(ctx, ref)
		if returnErr == nil {
			return
		}

		try.Wait()
	}

	// If no error has been recorded yet, we fallback to the backoff error.
	if returnErr == nil {
		returnErr = try.Err()
	}

	return
}

// getSegmentObjectPath returns the path of the segment object in the object storage.
func getSegmentObjectPath(partitionID int32, objectID ulid.ULID) string {
	return path.Join(strconv.Itoa(int(partitionID)), objectID.String())
}

func readSegmentObject(reader io.Reader) (*ingestpb.Segment, error) {
	rawData, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	segmentData := &ingestpb.Segment{}
	if err := segmentData.Unmarshal(rawData); err != nil {
		return nil, err
	}

	return segmentData, nil
}
