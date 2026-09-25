// SPDX-License-Identifier: AGPL-3.0-only

package scallop

// This file defines the versioned wire contract for replaying one planning round.

// ReplaySnapshotVersion is the current offline replay envelope version.
const ReplaySnapshotVersion = 1

// ReplaySnapshotEnvelope contains every input required to reproduce one Plan call.
type ReplaySnapshotEnvelope struct {
	Version  int      `json:"version"`
	Snapshot Snapshot `json:"snapshot"`
	Policy   Policy   `json:"policy"`
}

// NewReplaySnapshotEnvelope wraps one complete planner input in the current wire version.
func NewReplaySnapshotEnvelope(snapshot Snapshot, policy Policy) ReplaySnapshotEnvelope {
	return ReplaySnapshotEnvelope{
		Version:  ReplaySnapshotVersion,
		Snapshot: snapshot,
		Policy:   policy,
	}
}
