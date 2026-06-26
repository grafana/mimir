// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

// NewSingleWCJobSpec builds a non-compartment JobSpec carrying the single range
// in the top-level fields.
func NewSingleWCJobSpec(topic string, partition int32, startOffset, endOffset int64) JobSpec {
	return JobSpec{
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
		EndOffset:   endOffset,
	}
}

// Ranges returns the offset ranges to consume, unifying both JobSpec encodings:
// OffsetRanges in compartment mode, or a synthesized WC-0 range from the
// top-level offsets otherwise. In compartment mode a spec carries ranges only
// for the WCs that had new data in the bucket, not necessarily every WC.
// Returns nil when the spec carries no work.
func (m JobSpec) Ranges() []WCOffsetRange {
	if len(m.OffsetRanges) > 0 {
		return m.OffsetRanges
	}
	if m.EndOffset > m.StartOffset {
		return []WCOffsetRange{{WcId: 0, StartOffset: m.StartOffset, EndOffset: m.EndOffset}}
	}
	return nil
}
