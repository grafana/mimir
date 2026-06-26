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

	if len(s.probeTimes) == 0 {
		// No scan window, so there's nothing to probe.
		return []*offsetTime{}, nil
	}

	reachedResume := false
	sentinels := []*offsetTime{}

	// The general idea is that we know the resume offset, but we don't know
	// that offset's timestamp. We have an API (offsetAfterTime) to get offsets
	// given a timestamp, so we iteratively call that API for each probe time
	// until we reach the resume offset.
	for _, pb := range s.probeTimes {
		offset, t, err := s.offs.offsetAfterTime(ctx, off.topic, off.partition, pb)
		if err != nil {
			return nil, err
		}
		level.Debug(logger).Log("msg", "found next boundary offset", "ts", pb,
			"topic", off.topic, "partition", off.partition, "offset", offset)

		// Don't want to probe for offsets before the resume offset.
		offset = max(offset, off.resume)

		// Only append sentinel if it's the first time we've seen it.
		if len(sentinels) == 0 || offset != sentinels[len(sentinels)-1].offset {
			sentinels = append(sentinels, &offsetTime{offset: offset, time: t})

			// The high watermark is returned with ts == 0, that's not a real timestamp to observe.
			if !t.IsZero() {
				s.recordTimeDelta.Observe(t.Sub(pb).Seconds())
			}
		}

		if offset == off.resume {
			// We've reached the resume offset, so we're done.
			reachedResume = true
			break
		}
	}

	// We have at least one sentinel if we reach this point.
	if !reachedResume {
		lowestOffset := sentinels[len(sentinels)-1].offset
		level.Warn(logger).Log("msg", "probe offsets: probe did not reach resume offset due to limited scan age",
			"partition", off.partition, "lowestOffset", lowestOffset, "resumeOffset", off.resume)
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
	offsetAfterTime(context.Context, string, int32, time.Time) (int64, time.Time, error)
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
func (o *offsetFinder) offsetAfterTime(ctx context.Context, topic string, partition int32, t time.Time) (int64, time.Time, error) {
	offs, ok := o.offsets[t]
	if !ok {
		var err error
		offs, err = o.adminClient.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
		if err != nil {
			return 0, time.Time{}, err
		}
		if offs.Error() != nil {
			return 0, time.Time{}, offs.Error()
		}

		o.offsets[t] = offs
	}

	po, ok := offs.Lookup(topic, partition)
	if !ok {
		return 0, time.Time{}, fmt.Errorf("failed to get offset for partition %d at time %s: not present", partition, t)
	}
	if po.Err != nil {
		return 0, time.Time{}, fmt.Errorf("failed to get offset for partition %d at time %s: %w", partition, t, po.Err)
	}

	return po.Offset, time.UnixMilli(po.Timestamp), nil
}

var _ offsetStore = (*offsetFinder)(nil)
