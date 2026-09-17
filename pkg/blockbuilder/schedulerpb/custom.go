// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Ranges returns the offset ranges to consume keyed by cluster ID. In non-compartment mode,
// cluster ID is set to zero.
func (m JobSpec) Ranges() map[int32]OffsetRange {
	if len(m.OffsetRanges) > 0 {
		return m.OffsetRanges
	}
	return map[int32]OffsetRange{0: {StartOffset: m.StartOffset, EndOffset: m.EndOffset}}
}

// RangesString returns a compact, log-friendly rendering of the offset ranges. In non-compartment
// mode it is "[start,end)"; in compartment mode it is "clusterID:[start,end)" per cluster, sorted
// by cluster ID (e.g. "0:[100,200) 1:[500,700)").
func (m JobSpec) RangesString() string {
	if len(m.OffsetRanges) == 0 {
		return fmt.Sprintf("[%d,%d)", m.StartOffset, m.EndOffset)
	}
	clusters := make([]int32, 0, len(m.OffsetRanges))
	for clusterID := range m.OffsetRanges {
		clusters = append(clusters, clusterID)
	}
	slices.Sort(clusters)
	var b strings.Builder
	for i, clusterID := range clusters {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d:[%d,%d)", clusterID, m.OffsetRanges[clusterID].StartOffset, m.OffsetRanges[clusterID].EndOffset)
	}
	return b.String()
}

func (m JobSpec) Validate(compartmentsEnabled bool, numCompartments int) error {
	if compartmentsEnabled {
		if m.EndOffset != 0 || m.StartOffset != 0 {
			return errors.New("start/end_offsets should not be set when compartments is enabled")
		}

		if len(m.OffsetRanges) < 1 {
			return errors.New("offset_ranges should not be empty when compartments is enabled")
		}

		for id := range m.OffsetRanges {
			if int(id) >= numCompartments || id < 0 {
				return fmt.Errorf("invalid compartment id, expecting id between [0,%d) got: %d", numCompartments, id)
			}
		}
	} else {
		if len(m.OffsetRanges) > 0 {
			return errors.New("offset_ranges should not be set when compartments is disabled")
		}
	}
	return nil
}
