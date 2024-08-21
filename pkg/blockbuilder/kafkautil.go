// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// getGroupLag is the inlined version of `kadm.Client.Lag` but updated to work with when the group
// doesn't have live participants.
// Similar to `kadm.CalculateGroupLagWithStartOffsets`, it takes into account that the group may not have any commits.
func getGroupLag(ctx context.Context, admClient *kadm.Client, topic, group string, fallbackOffset int64) (kadm.GroupLag, error) {
	offsets, err := admClient.FetchOffsets(ctx, group)
	if err != nil {
		if !errors.Is(err, kerr.GroupIDNotFound) {
			return nil, fmt.Errorf("fetch offsets: %w", err)
		}
	}
	if err := offsets.Error(); err != nil {
		return nil, fmt.Errorf("fetch offsets got error in response: %w", err)
	}

	startOffsets, err := admClient.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	endOffsets, err := admClient.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}

	resolveFallbackOffsets := sync.OnceValues(func() (kadm.ListedOffsets, error) {
		if fallbackOffset == kafkaOffsetStart {
			return startOffsets, nil
		}
		if fallbackOffset > 0 {
			return admClient.ListOffsetsAfterMilli(ctx, fallbackOffset, topic)
		}
		// This should not happen because fallbackOffset already went through the validation by this point.
		return nil, fmt.Errorf("cannot resolve fallback offset for value %v", fallbackOffset)
	})
	// If the group-partition in offsets doesn't have a commit, fall back depending on where fallbackOffset points at.
	for topic, pt := range startOffsets.Offsets() {
		for part := range pt {
			if _, ok := offsets.Lookup(topic, part); ok {
				continue
			}
			fallbackOffsets, err := resolveFallbackOffsets()
			if err != nil {
				return nil, fmt.Errorf("resolve fallback offsets: %w", err)
			}
			o, ok := fallbackOffsets.Lookup(topic, part)
			if !ok {
				return nil, fmt.Errorf("partition %d not found in fallback offsets for topic %s", part, topic)
			}
			offsets.Add(kadm.OffsetResponse{Offset: kadm.Offset{
				Topic:       o.Topic,
				Partition:   o.Partition,
				At:          o.Offset,
				LeaderEpoch: o.LeaderEpoch,
			}})
		}
	}

	return calculateGroupLag(offsets, startOffsets, endOffsets), nil
}

var errListMissing = errors.New("missing from list offsets")

// Inlined from https://github.com/grafana/mimir/blob/04df05d32320cd19281591a57c19cceba63ceabf/vendor/github.com/twmb/franz-go/pkg/kadm/groups.go#L1711
func calculateGroupLag(commit kadm.OffsetResponses, startOffsets, endOffsets kadm.ListedOffsets) kadm.GroupLag {
	l := make(map[string]map[int32]kadm.GroupMemberLag)
	for t, ps := range commit {
		lt := l[t]
		if lt == nil {
			lt = make(map[int32]kadm.GroupMemberLag)
			l[t] = lt
		}
		tstart := startOffsets[t]
		tend := endOffsets[t]
		for p, pcommit := range ps {
			var (
				pend = kadm.ListedOffset{
					Topic:     t,
					Partition: p,
					Err:       errListMissing,
				}
				pstart = pend
				perr   error
			)

			// In order of priority, perr (the error on the Lag
			// calculation) is non-nil if:
			//
			//  * The topic is missing from end ListOffsets
			//  * The partition is missing from end ListOffsets
			//  * OffsetFetch has an error on the partition
			//  * ListOffsets has an error on the partition
			//
			// If we have no error, then we can calculate lag.
			// We *do* allow an error on start ListedOffsets;
			// if there are no start offsets or the start offset
			// has an error, it is not used for lag calculation.
			perr = errListMissing
			if tend != nil {
				if pendActual, ok := tend[p]; ok {
					pend = pendActual
					perr = nil
				}
			}
			if perr == nil {
				if perr = pcommit.Err; perr == nil {
					perr = pend.Err
				}
			}
			if tstart != nil {
				if pstartActual, ok := tstart[p]; ok {
					pstart = pstartActual
				}
			}

			lag := int64(-1)
			if perr == nil {
				lag = pend.Offset
				if pstart.Err == nil {
					lag = pend.Offset - pstart.Offset
				}
				if pcommit.At >= 0 {
					lag = pend.Offset - pcommit.At
				}
				if lag < 0 {
					lag = 0
				}
			}

			lt[p] = kadm.GroupMemberLag{
				Topic:     t,
				Partition: p,
				Commit:    pcommit.Offset,
				Start:     pstart,
				End:       pend,
				Lag:       lag,
				Err:       perr,
			}
		}
	}

	// Now we look at all topics that we calculated lag for, and check out
	// the partitions we listed. If those partitions are missing from the
	// lag calculations above, the partitions were not committed to and we
	// count that as entirely lagging.
	for t, lt := range l {
		tstart := startOffsets[t]
		tend := endOffsets[t]
		for p, pend := range tend {
			if _, ok := lt[p]; ok {
				continue
			}
			pcommit := kadm.Offset{
				Topic:       t,
				Partition:   p,
				At:          -1,
				LeaderEpoch: -1,
			}
			perr := pend.Err
			lag := int64(-1)
			if perr == nil {
				lag = pend.Offset
			}
			pstart := kadm.ListedOffset{
				Topic:     t,
				Partition: p,
				Err:       errListMissing,
			}
			if tstart != nil {
				if pstartActual, ok := tstart[p]; ok {
					pstart = pstartActual
					if pstart.Err == nil {
						lag = pend.Offset - pstart.Offset
						if lag < 0 {
							lag = 0
						}
					}
				}
			}
			lt[p] = kadm.GroupMemberLag{
				Topic:     t,
				Partition: p,
				Commit:    pcommit,
				Start:     pstart,
				End:       pend,
				Lag:       lag,
				Err:       perr,
			}
		}
	}

	return l
}
