// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
)

type offsetTime struct {
	offset int64
	time   time.Time
}

// offsetScanner computes an initial set of <offset, time> pairs for each
// partition, used to seed the scheduler at startup.
type offsetScanner struct {
	// stores are indexed by clusterID.
	stores  []offsetStore
	topic   string
	endTime time.Time
	// probeTimes are the timestamps to query Kafka offsets at, in descending
	// order so the scan stops once it reaches the partition's resume offset.
	probeTimes      []time.Time
	recordTimeDelta prometheus.Observer
}

func newOffsetScanner(stores []offsetStore, topic string, endTime time.Time, jobSize, maxScanAge time.Duration, recordTimeDelta prometheus.Observer) *offsetScanner {
	minScanTime := endTime.Add(-maxScanAge)
	return &offsetScanner{
		stores:          stores,
		topic:           topic,
		endTime:         endTime,
		probeTimes:      scanProbeTimes(endTime, jobSize, minScanTime),
		recordTimeDelta: recordTimeDelta,
	}
}

// probeInitialPartitionOffsets probes every cluster of a partition and returns their offsets,
// indexed by clusterID. These offsets can be used to seed the scheduler's per-cluster end
// offset tracking at startup. clusterOffsets is indexed by clusterID; a nil entry means the
// partition has no offsets on that cluster and is skipped, leaving a nil slot in the result.
func (s *offsetScanner) probeInitialPartitionOffsets(ctx context.Context, clusterOffsets []*partitionOffsets, logger log.Logger) ([][]*offsetTime, error) {
	perCluster := make([][]*offsetTime, len(clusterOffsets))
	for clusterID, po := range clusterOffsets {
		if po == nil {
			continue
		}
		offsets, err := s.probeSingleClusterOffsets(ctx, clusterID, *po, logger)
		if err != nil {
			return nil, err
		}
		perCluster[clusterID] = offsets
	}
	return perCluster, nil
}

// probeSingleClusterOffsets computes an initial set of offsets that exist between this
// partition's resume and end offsets on a single cluster. These offsets can be used to
// seed the scheduler's per-cluster end offset tracking at startup.
func (s *offsetScanner) probeSingleClusterOffsets(ctx context.Context, clusterID int, po partitionOffsets, logger log.Logger) ([]*offsetTime, error) {
	store := s.stores[clusterID]

	if po.resume >= po.end || po.start >= po.end {
		// No new data to consume. Return the single end offset so it is initially registered.
		return []*offsetTime{{offset: po.end, time: s.endTime}}, nil
	}

	reachedResume := false
	sentinels := []*offsetTime{}

	// The general idea is that we know the resume offset, but we don't know
	// that offset's timestamp. We have an API (offsetAfterTime) to get offsets
	// given a timestamp, so we iteratively call that API for each probe time
	// until we reach the resume offset.
	for _, pb := range s.probeTimes {
		offset, t, isEndOffset, err := store.offsetAfterTime(ctx, s.topic, po.partition, pb)
		if err != nil {
			return nil, err
		}
		level.Debug(logger).Log("msg", "found next boundary offset", "ts", pb,
			"topic", s.topic, "partition", po.partition, "offset", offset, "isEndOffset", isEndOffset)

		// Don't want to probe for offsets before the resume offset.
		offset = max(offset, po.resume)

		if len(sentinels) == 0 || offset != sentinels[len(sentinels)-1].offset {
			// The end offset has no real record timestamp, so register its
			// offset at the current time.
			if isEndOffset {
				t = s.endTime
			} else {
				s.recordTimeDelta.Observe(t.Sub(pb).Seconds())
			}
			sentinels = append(sentinels, &offsetTime{offset: offset, time: t})
		}

		if offset == po.resume {
			// We've reached the resume offset, so we're done.
			reachedResume = true
			break
		}
	}

	if !reachedResume {
		lastOffset := int64(-1)
		if len(sentinels) > 0 {
			lastOffset = sentinels[len(sentinels)-1].offset
		}
		level.Warn(logger).Log("msg", "probe offsets: probe did not reach commit offset due to limited scan age", "partition", po.partition, "lastOffset", lastOffset, "resumeOffset", po.resume)
	}

	// Return them in increasing order of offset.
	slices.SortFunc(sentinels, func(a, b *offsetTime) int {
		return cmp.Compare(a.offset, b.offset)
	})

	return sentinels, nil
}

// scanProbeTimes returns the times at which the scanner probes for offsets, in
// decreasing order.
// TODO: anchor the probe times to job boundaries instead, so offsets returned are more aligned with the boundaries.
// TODO: append a final probe at minScanTime, so the bottom of the scan window is covered.
func scanProbeTimes(endTime time.Time, jobSize time.Duration, minScanTime time.Time) []time.Time {
	scanStep := jobSize / 4
	var probeTimes []time.Time
	for pb := endTime; minScanTime.Before(pb); pb = pb.Add(-scanStep) {
		probeTimes = append(probeTimes, pb)
	}
	return probeTimes
}

type offsetStore interface {
	offsetAfterTime(context.Context, string, int32, time.Time) (int64, time.Time, bool, error)
}

type offsetFinder struct {
	offsets     map[time.Time]kadm.ListedOffsets
	adminClient *kadm.Client
}

func newOffsetFinder(adminClient *kadm.Client) *offsetFinder {
	return &offsetFinder{
		offsets:     make(map[time.Time]kadm.ListedOffsets),
		adminClient: adminClient,
	}
}

// offsetAfterTime is a cached version of adminClient.ListOffsetsAfterMilli that
// makes use of the fact that we want to ask about the same times for all partitions.
// The returned bool reports whether the offset is the partition's end offset
// (no record at or after t); when true, the returned time is not meaningful.
func (o *offsetFinder) offsetAfterTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, time.Time, bool, error) {
	offs, ok := o.offsets[t]
	if !ok {
		var err error
		offs, err = o.adminClient.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
		if err != nil {
			return 0, time.Time{}, false, err
		}
		if offs.Error() != nil {
			return 0, time.Time{}, false, offs.Error()
		}

		o.offsets[t] = offs
	}

	po, ok := offs.Lookup(topic, partition)
	if !ok {
		return 0, time.Time{}, false, fmt.Errorf("failed to get offset for partition %d at time %s: not present", partition, t)
	}
	if po.Err != nil {
		return 0, time.Time{}, false, fmt.Errorf("failed to get offset for partition %d at time %s: %w", partition, t, po.Err)
	}

	if po.Timestamp == -1 {
		// When there's no record at or after the requested time, kadm returns the
		// end offset with a -1 timestamp. The timestamp is meaningless here, so
		// report it as the end offset.
		return po.Offset, time.Time{}, true, nil
	}
	if po.Timestamp < -1 {
		// ListOffsetsAfterMilli should only ever return a real record timestamp or
		// the -1 sentinel, so anything below -1 means a malformed response.
		panic(fmt.Sprintf("unexpected timestamp %d for partition %d at time %s: timestamps below -1 should never be returned", po.Timestamp, partition, t))
	}

	return po.Offset, time.UnixMilli(po.Timestamp), false, nil
}

var _ offsetStore = (*offsetFinder)(nil)
