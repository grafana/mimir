package streams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

type SegmentsScanner struct {
	services.Service

	bucket objstore.BucketReader

	tracker  *SegmentsTracker
	consumer *SegmentConsumer
}

func NewSegmentsScanner(bucket objstore.BucketReader, l log.Logger) *SegmentsScanner {
	scanner := &SegmentsScanner{
		bucket:  bucket,
		tracker: NewSegmentsTracker(),
		consumer: NewSegmentConsumer(
			0, // TODO dimitarvdimitrov infer from ring
			bucket,
			&PartitionConsumer{l: l},
		),
	}
	scanner.Service = services.NewTimerService(time.Second, nil, scanner.scan, nil)
	return scanner
}

func (s *SegmentsScanner) scan(ctx context.Context) error {
	now := time.Now()
	minT, maxT := now.Add(-time.Minute), now.Add(time.Minute)

	segments, err := s.listSegments(ctx, minT, maxT)
	if err != nil {
		return err
	}
	segments = s.filterOutKnownSegments(segments)
	segments = sortSegments(segments)
	consumed, err := s.consumeSegments(ctx, segments)
	s.tracker.trackConsumed(consumed)
	if err != nil {
		return fmt.Errorf("consuming segments: %w", err)
	}
	s.tracker.untrackSegmentsSince(minT)
	return nil
}

func (s *SegmentsScanner) listSegments(ctx context.Context, minT time.Time, maxT time.Time) ([]SegmentInfo, error) {
	ranges := splitRanges(minT, maxT, time.Second)
	segmentKeys, err := s.listObjectsInRanges(ctx, ranges)
	if err != nil {
		return nil, fmt.Errorf("scanning bucket: %w", err)
	}
	segments, err := keysToSegments(segmentKeys)
	if err != nil {
		return nil, fmt.Errorf("discovering segments: %w", err)
	}
	return segments, nil
}

// sortSegments sorts segments by their upload time and then by their ULID.
func sortSegments(segments []SegmentInfo) []SegmentInfo {
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ObjectKey < segments[j].ObjectKey
	})
	return segments
}

func splitRanges(start time.Time, end time.Time, interval time.Duration) []string {
	start = start.Truncate(interval)
	end = end.Truncate(interval)

	prefixes := make([]string, 0, end.Sub(start)/interval)
	for now := start; !now.After(end); now.Add(interval) {
		prefixes = append(prefixes, strconv.FormatInt(now.Unix(), 10))
	}
	return prefixes
}

func (s *SegmentsScanner) listObjectsInRanges(ctx context.Context, prefixes []string) ([]string, error) {
	g, ctx := errgroup.WithContext(ctx)
	namesChan := make(chan string)
	for _, prefix := range prefixes {
		prefix := prefix
		g.Go(func() error {
			return s.bucket.Iter(ctx, prefix, func(s string) error {
				namesChan <- s
				return nil
			})
		})
	}

	var err error
	go func() {
		err = g.Wait()
		close(namesChan)
	}()
	var allNames []string
	for name := range namesChan {
		allNames = append(allNames, name)
	}
	if err != nil {
		return nil, fmt.Errorf("listing chunk objects: %w", err)
	}
	return allNames, nil
}

type SegmentInfo struct {
	UploadTime time.Time
	ObjectKey  string
}

func keysToSegments(keys []string) ([]SegmentInfo, error) {
	infos := make([]SegmentInfo, 0, len(keys))
	for _, key := range keys {
		tStr := path.Base(path.Base(key))
		unixSeconds, err := strconv.ParseInt(tStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing segment name %s: %w", key, err)
		}
		infos = append(infos, SegmentInfo{
			UploadTime: time.Unix(unixSeconds, 0),
			ObjectKey:  key,
		})
	}
	return infos, nil
}

func (s *SegmentsScanner) filterOutKnownSegments(segmentKeys []SegmentInfo) []SegmentInfo {
	for i := 0; i < len(segmentKeys); i++ {
		if s.tracker.isConsumed(segmentKeys[i]) {
			segmentKeys[i] = segmentKeys[len(segmentKeys)-1]
			segmentKeys = segmentKeys[:len(segmentKeys)-1]
			i--
		}
	}
	return segmentKeys
}

func (s *SegmentsScanner) consumeSegments(ctx context.Context, segments []SegmentInfo) (consumed []SegmentInfo, _ error) {
	// TODO fetch TOC concurrently, fetch partitions in order
	for segmentIdx, segment := range segments {
		toc, err := s.segmentTOC(ctx, segment)
		if err != nil {
			return segments[:segmentIdx], fmt.Errorf("reading segment TOC %s: %w", segment, err)
		}

		err = s.consumer.consumeSegment(ctx, segment, toc)
		if err != nil {
			return segments[:segmentIdx], fmt.Errorf("consuming segment %s: %w", segment, err)
		}
	}
	return segments, nil
}

func (s *SegmentsScanner) segmentTOC(ctx context.Context, segment SegmentInfo) (ChunkBufferTOC, error) {
	const maxPartitionsPerSegment = 1000
	tocReader, err := s.bucket.GetRange(ctx, segment.ObjectKey, 0, int64(ChunkBufferTOCSize(maxPartitionsPerSegment)))
	if err != nil {
		return ChunkBufferTOC{}, err
	}
	defer tocReader.Close()

	tocBytes, err := io.ReadAll(tocReader)
	if err != nil {
		return ChunkBufferTOC{}, fmt.Errorf("reading TOC of %s: %w", segment.ObjectKey, err)
	}
	toc, err := NewChunkBufferTOCFromBytes(tocBytes)
	if errors.Is(err, encoding.ErrInvalidSize) {
		// TODO implement decoding from an io.Reader instead of from a byte slice, then just open a reader for the whole object.
		return ChunkBufferTOC{}, fmt.Errorf("TOC was possibly underfetched: TOC partitions=%d, maxPartitionsPerSegment=%d: %w", toc.partitionsLength, maxPartitionsPerSegment, err)
	}
	if err != nil {
		return ChunkBufferTOC{}, fmt.Errorf("decoding TOC: %w", err)
	}
	return toc, nil
}

type SegmentsTracker struct {
	segments map[string]SegmentInfo
}

func NewSegmentsTracker() *SegmentsTracker {
	return &SegmentsTracker{segments: make(map[string]SegmentInfo)}
}

func (t SegmentsTracker) isConsumed(s SegmentInfo) bool {
	_, isConsumed := t.segments[s.ObjectKey]
	return isConsumed
}

func (t SegmentsTracker) trackConsumed(consumed []SegmentInfo) {
	for _, s := range consumed {
		t.segments[s.ObjectKey] = s
	}
}

func (t SegmentsTracker) untrackSegmentsSince(ts time.Time) {
	for key, s := range t.segments {
		// TODO if this is expensive, then start recording segments into a map of maps. Then untracking will be just deleting the oldest map
		if s.UploadTime.Before(ts) {
			delete(t.segments, key)
		}
	}
}

type SegmentConsumer struct {
	targetPartition uint32

	bucket   objstore.BucketReader
	consumer *PartitionConsumer
}

func NewSegmentConsumer(targetPartition uint32, bucket objstore.BucketReader, consumer *PartitionConsumer) *SegmentConsumer {
	return &SegmentConsumer{targetPartition: targetPartition, bucket: bucket, consumer: consumer}
}

func (c *SegmentConsumer) consumeSegment(ctx context.Context, segment SegmentInfo, toc ChunkBufferTOC) error {
	for _, partition := range toc.partitions {
		if partition.partitionID != c.targetPartition {
			continue
		}
		partitionReader, err := c.bucket.GetRange(ctx, segment.ObjectKey, int64(partition.offset), int64(partition.length))
		if err != nil {
			return fmt.Errorf("reading partition %d from %s: %w", partition.partitionID, segment.ObjectKey, err)
		}
		err = c.consumer.consume(ctx, segment, partitionReader)
		if err != nil {
			return fmt.Errorf("consuming partition %d from %s: %w", partition.partitionID, segment.ObjectKey, err)
		}
	}
	return nil
}

type PartitionConsumer struct {
	l log.Logger
}

func (c *PartitionConsumer) consume(ctx context.Context, segment SegmentInfo, reader io.ReadCloser) error {
	// TODO parse partition and feed into ingester.Push
	level.Info(c.l).Log("msg", "consumed partition", "segment", segment.ObjectKey, "segment_uploaded", segment.UploadTime)
	return reader.Close()
}
