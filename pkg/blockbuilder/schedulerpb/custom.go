// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import (
	"errors"
	"fmt"
)

// Ranges returns the offset ranges to consume keyed by cluster ID. In non-compartment mode,
// cluster ID is set to zero.
func (m JobSpec) Ranges() map[int32]OffsetRange {
	if len(m.OffsetRanges) > 0 {
		return m.OffsetRanges
	}
	return map[int32]OffsetRange{0: {StartOffset: m.StartOffset, EndOffset: m.EndOffset}}
}

func (m JobSpec) Validate(compartmentsEnabled bool, numCompartments int) error {
	if compartmentsEnabled {
		if m.EndOffset != 0 || m.StartOffset != 0 {
			return errors.New("start/end_offsets should not be set when compartments is enabled")
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
