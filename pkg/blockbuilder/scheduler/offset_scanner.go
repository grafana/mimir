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
	offs            offsetStore
	endTime         time.Time
	probeTimes      []time.Time
	recordTimeDelta prometheus.Observer
}

func newOffsetScanner(offs offsetStore, endTime time.Time, jobSize, maxScanAge time.Duration, recordTimeDelta prometheus.Observer) *offsetScanner {
	minScanTime := endTime.Add(-maxScanAge)
	return &offsetScanner{
		offs:            offs,
		endTime:         endTime,
		probeTimes:      scanProbeTimes(endTime, jobSize, minScanTime),
		recordTimeDelta: recordTimeDelta,
	}
}

// probeInitialOffsets computes an initial set of <offset, time> pairs that exist between this
// partition's resume and end offsets. These pairs can be used to seed a bunch of
// end offset observations to start the scheduler.
func (s *offsetScanner) probeInitialOffsets(ctx context.Context, off partitionOffsets, logger log.Logger) ([]*offsetTime, error) {
	if off.resume >= off.end || off.start >= off.end {
		// No new data to consume. Return the single end offset so it is initially registered.
		return []*offsetTime{{offset: off.end, time: s.endTime}}, nil
	}

	reachedResume := false
	sentinels := []*offsetTime{}

	// The general idea is that we know the resume offset, but we don't know
	// that offset's timestamp. We have an API (offsetAfterTime) to get offsets
	// given a timestamp, so we iteratively call that API for each probe time
	// until we reach the resume offset.
	for _, pb := range s.probeTimes {
		offset, t, isEndOffset, err := s.offs.offsetAfterTime(ctx, off.topic, off.partition, pb)
		if err != nil {
			return nil, err
		}
		level.Debug(logger).Log("msg", "found next boundary offset", "ts", pb,
			"topic", off.topic, "partition", off.partition, "offset", offset, "isEndOffset", isEndOffset)

		// Don't want to probe for offsets before the resume offset.
		offset = max(offset, off.resume)

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

		if offset == off.resume {
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
		level.Warn(logger).Log("msg", "probe offsets: probe did not reach commit offset due to limited scan age", "partition", off.partition, "lastOffset", lastOffset, "resumeOffset", off.resume)
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
