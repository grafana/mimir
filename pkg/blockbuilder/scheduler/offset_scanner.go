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
	"github.com/twmb/franz-go/pkg/kadm"
)

type offsetTime struct {
	offset int64
	time   time.Time
}

// probeInitialOffsets computes an initial set of <offset, time> pairs that
// exist between this partition's commit and end offsets. These pairs can be
// used to seed a bunch of end offset observations to start the scheduler.
func probeInitialOffsets(ctx context.Context, offs offsetStore, topic string, partition int32, start, commit, end int64,
	endTime time.Time, jobSize time.Duration, minScanTime time.Time, logger log.Logger) ([]*offsetTime, error) {

	if commit >= end || start >= end {
		// No new data to consume. Return the single end offset so it is initially registered.
		return []*offsetTime{{offset: end, time: endTime}}, nil
	}

	// Pick a more high-resolution interval to scan for the sentinel offsets.
	scanStep := jobSize / 4
	reachedCommit := false
	sentinels := []*offsetTime{}

	// The general idea is that we know the commit offset, but we don't know
	// that offset's timestamp. We have an API (offsetAfterTime) to get offsets
	// given a timestamp, so we iteratively call that API until we reach either
	// the commit offset or the min scan time.
	for pb := endTime; minScanTime.Before(pb); pb = pb.Add(-scanStep) {
		off, t, err := offs.offsetAfterTime(ctx, topic, partition, pb)
		if err != nil {
			return nil, err
		}
		level.Debug(logger).Log("msg", "found next boundary offset", "ts", pb,
			"topic", topic, "partition", partition, "offset", off)

		// Don't want to probe for offsets before the commit.
		off = max(off, commit)

		if len(sentinels) == 0 || off != sentinels[len(sentinels)-1].offset {
			sentinels = append(sentinels, &offsetTime{offset: off, time: t})
		}

		if off == commit {
			// We've reached the commit offset, so we're done.
			reachedCommit = true
			break
		}
	}

	if !reachedCommit {
		lastOffset := int64(-1)
		if len(sentinels) > 0 {
			lastOffset = sentinels[len(sentinels)-1].offset
		}
		level.Warn(logger).Log("msg", "probe offsets: probe did not reach commit offset due to limited scan age", "partition", partition, "lastOffset", lastOffset, "commitOffset", commit)
	}

	// Return them in increasing order of offset.
	slices.SortFunc(sentinels, func(a, b *offsetTime) int {
		return cmp.Compare(a.offset, b.offset)
	})

	return sentinels, nil
}

type offsetStore interface {
	offsetAfterTime(context.Context, string, int32, time.Time) (int64, time.Time, error)
}

type offsetFinder struct {
	offsets     map[time.Time]kadm.ListedOffsets
	adminClient *kadm.Client
	logger      log.Logger
}

func newOffsetFinder(adminClient *kadm.Client, logger log.Logger) *offsetFinder {
	return &offsetFinder{
		offsets:     make(map[time.Time]kadm.ListedOffsets),
		adminClient: adminClient,
		logger:      logger,
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
