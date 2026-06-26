// SPDX-License-Identifier: AGPL-3.0-only

package schedulerpb

import "testing"

func TestNewNonCompartmentJobSpec(t *testing.T) {
	spec := NewNonCompartmentJobSpec("topic-a", 3, 10, 20)
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
	if rng := ranges[0]; rng.StartOffset != 10 || rng.EndOffset != 20 {
		t.Fatalf("unexpected synthesized range: %+v", rng)
	}
	if spec.IsCompartmentEncoding() {
		t.Fatalf("non-compartment spec should not report IsCompartmentEncoding")
	}
}

func TestJobSpecIsCompartmentEncoding(t *testing.T) {
	compartment := JobSpec{Topic: "t", Partition: 0, OffsetRanges: map[int32]OffsetRange{0: {StartOffset: 0, EndOffset: 10}}}
	if !compartment.IsCompartmentEncoding() {
		t.Fatalf("spec carrying OffsetRanges should report IsCompartmentEncoding")
	}
	if NewNonCompartmentJobSpec("t", 0, 0, 10).IsCompartmentEncoding() {
		t.Fatalf("non-compartment spec should not report IsCompartmentEncoding")
	}
}
