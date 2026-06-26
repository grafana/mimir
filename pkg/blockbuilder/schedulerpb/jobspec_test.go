// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import "testing"

func TestNewSingleWCJobSpec(t *testing.T) {
	spec := NewSingleWCJobSpec("topic-a", 3, 10, 20)
	if spec.Topic != "topic-a" || spec.Partition != 3 {
		t.Fatalf("unexpected job identity: topic=%q partition=%d", spec.Topic, spec.Partition)
	}
	if len(spec.OffsetRanges) != 0 {
		t.Fatalf("non-compartment spec should carry no OffsetRanges, got %d", len(spec.OffsetRanges))
	}
	if spec.StartOffset != 10 || spec.EndOffset != 20 {
		t.Fatalf("unexpected top-level range: start=%d end=%d", spec.StartOffset, spec.EndOffset)
	}
	ranges := spec.Ranges()
	if len(ranges) != 1 {
		t.Fatalf("len(ranges) = %d, want 1", len(ranges))
	}
	if wc := ranges[0]; wc.WcId != 0 || wc.StartOffset != 10 || wc.EndOffset != 20 {
		t.Fatalf("unexpected synthesized range: %+v", wc)
	}
}
