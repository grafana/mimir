// SPDX-License-Identifier: AGPL-3.0-only

package stats

// This file implements the wiresmith customtype contract
// (SizeWiresmith/MarshalWiresmith/UnmarshalWiresmith/EqualWiresmith/CompareWiresmith)
// for SafeStats, referenced from frontendv2pb/frontend.proto via
// (wiresmith.options.customtype). The methods delegate to the wiresmith-generated
// Stats methods so the embedded atomic accessors keep working unchanged.

// SizeWiresmith implements the wiresmith customtype contract.
func (s *SafeStats) SizeWiresmith() int {
	return s.Size()
}

// MarshalWiresmith implements the wiresmith customtype contract: buf is
// exactly SizeWiresmith() bytes, so the reverse writer fills it completely.
func (s *SafeStats) MarshalWiresmith(buf []byte) (int, error) {
	return s.MarshalToSizedBuffer(buf)
}

// UnmarshalWiresmith implements the wiresmith customtype contract.
func (s *SafeStats) UnmarshalWiresmith(buf []byte) error {
	return s.Unmarshal(buf)
}

// EqualWiresmith implements the wiresmith customtype contract.
func (s *SafeStats) EqualWiresmith(other any) bool {
	o, ok := other.(SafeStats)
	if !ok {
		return false
	}
	return s.Equal(o.Stats)
}

// CompareWiresmith implements the wiresmith customtype contract.
func (s *SafeStats) CompareWiresmith(other any) int {
	o, ok := other.(SafeStats)
	if !ok {
		return -1
	}
	return s.Compare(o.Stats)
}
