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
	if m.EndOffset > m.StartOffset {
		return map[int32]OffsetRange{0: {StartOffset: m.StartOffset, EndOffset: m.EndOffset}}
	}
	return nil
}

func (m JobSpec) Validate(numCompartments int) error {
	if len(m.Ranges()) == 0 {
		return errors.New("job spec carries no offset ranges")
	}
	if m.IsCompartmentEncoding() {
		for id := range m.OffsetRanges {
			if int(id) >= numCompartments || id < 0 {
				return fmt.Errorf("invalid compartment id, expecting id between [0,%d) got: %d", numCompartments, id)
			}
		}
	}
	if len(m.OffsetRanges) > 0 && (m.EndOffset != 0 || m.StartOffset != 0) {
		return errors.New("offset ranges and start/end offsets should not be set at the same time")
	}
	return nil
}

func (m JobSpec) IsCompartmentEncoding() bool {
	return len(m.OffsetRanges) > 0
}

// NewNonCompartmentJobSpec builds a non-compartment JobSpec carrying the single range
// in the top-level fields.
func NewNonCompartmentJobSpec(topic string, partition int32, startOffset, endOffset int64) JobSpec {
	return JobSpec{
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
}
