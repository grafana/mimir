// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/blockbuilder/schedulerpb"
)

func TestOrderObservationsForImport(t *testing.T) {
	// clusterRangeArg pairs a cluster ID with its offset range for building JobSpecs.
	type clusterRangeArg struct {
		cluster     int32
		offsetRange schedulerpb.OffsetRange
	}
	offsetRange := func(cluster int32, start, end int64) clusterRangeArg {
		return clusterRangeArg{cluster: cluster, offsetRange: schedulerpb.OffsetRange{StartOffset: start, EndOffset: end}}
	}
	obs := func(id string, ranges ...clusterRangeArg) *observation {
		m := make(map[int32]schedulerpb.OffsetRange, len(ranges))
		for _, r := range ranges {
			m[r.cluster] = r.offsetRange
		}
		return &observation{key: jobKey{id: id}, spec: schedulerpb.JobSpec{Topic: "t", Partition: 0, OffsetRanges: m}}
	}
	// legacyObs builds a non-compartment spec, with StartOffset/EndOffset set and no
	// OffsetRanges, so ordering goes through the JobSpec.Ranges() fallback used in production.
	legacyObs := func(id string, start, end int64) *observation {
		return &observation{key: jobKey{id: id}, spec: schedulerpb.JobSpec{Topic: "t", Partition: 0, StartOffset: start, EndOffset: end}}
	}
	ids := func(observations []*observation) []string {
		out := make([]string, len(observations))
		for i, o := range observations {
			out[i] = o.key.id
		}
		return out
	}

	type testCase struct {
		name         string
		observations []*observation
		numClusters  int
		want         []string
		unordered    bool
		wantErr      error
	}

	tests := [...]testCase{
		{
			name: "single cluster sorts by start offset",
			observations: []*observation{
				obs("c", offsetRange(0, 20, 30)),
				obs("a", offsetRange(0, 0, 10)),
				obs("b", offsetRange(0, 10, 20)),
			},
			numClusters: 1,
			want:        []string{"a", "b", "c"},
		},
		{
			// J2 only continues cluster 1; J1 spans both with a high cluster 0 start, so a
			// single representative offset would mis-order them. J1 must come first.
			name: "multi-cluster sparse, predecessor before successor",
			observations: []*observation{
				obs("J2", offsetRange(1, 50, 120)),
				obs("J1", offsetRange(0, 1000, 1100), offsetRange(1, 0, 50)),
			},
			numClusters: 2,
			want:        []string{"J1", "J2"},
		},
		{
			name: "disjoint cluster sets all emitted",
			observations: []*observation{
				obs("b", offsetRange(1, 0, 5)),
				obs("a", offsetRange(0, 0, 10)),
			},
			numClusters: 2,
			want:        []string{"a", "b"},
		},
		{
			// Cluster 0 ties X and Y; cluster 1 strictly orders Y before X. The tie
			// must not count as a precedence constraint, else the valid order [Y,X]
			// is misreported as a cycle.
			name: "tie in one cluster resolved by strict order in another",
			observations: []*observation{
				obs("X", offsetRange(0, 10, 20), offsetRange(1, 20, 30)),
				obs("Y", offsetRange(0, 10, 20), offsetRange(1, 10, 20)),
			},
			numClusters: 2,
			want:        []string{"Y", "X"},
		},
		{
			// Y ties with X in cluster 0 but is never at the front of any other
			// cluster; it must still be considered for emission while X waits on Z.
			name: "tied job behind a blocked front is still emittable",
			observations: []*observation{
				obs("X", offsetRange(0, 10, 20), offsetRange(1, 20, 30)),
				obs("Y", offsetRange(0, 10, 20)),
				obs("Z", offsetRange(1, 10, 20)),
			},
			numClusters: 2,
			want:        []string{"Y", "Z", "X"},
		},
		{
			// Cluster 0 orders J1 before J2; cluster 1 orders J2 before J1.
			name: "offset cycle returns error",
			observations: []*observation{
				obs("J1", offsetRange(0, 0, 10), offsetRange(1, 20, 30)),
				obs("J2", offsetRange(0, 20, 30), offsetRange(1, 0, 10)),
			},
			numClusters: 2,
			wantErr:     errObservationOffsetCycle,
		},
		{
			// cluster 0: J1<J3<J4<J5, cluster 1: J1<J2<J3<J5. Those constraints pin a
			// single valid order across several merge passes, regardless of input order.
			name: "multi-cluster forced chain across sparse specs",
			observations: []*observation{
				obs("J4", offsetRange(0, 20, 30)),
				obs("J1", offsetRange(0, 0, 10), offsetRange(1, 0, 10)),
				obs("J5", offsetRange(0, 30, 40), offsetRange(1, 30, 40)),
				obs("J2", offsetRange(1, 10, 20)),
				obs("J3", offsetRange(0, 10, 20), offsetRange(1, 20, 30)),
			},
			numClusters: 2,
			want:        []string{"J1", "J2", "J3", "J4", "J5"},
		},
		{
			// Three clusters (A=0, B=1, C=2), each link forced through a different shared
			// cluster: A<AB in cluster A, AB<BC in cluster B, BC<C in cluster C.
			name: "forced chain across three clusters via distinct shared clusters",
			observations: []*observation{
				obs("C", offsetRange(2, 10, 20)),
				obs("AB", offsetRange(0, 10, 20), offsetRange(1, 0, 10)),
				obs("A", offsetRange(0, 0, 10)),
				obs("BC", offsetRange(1, 10, 20), offsetRange(2, 0, 10)),
			},
			numClusters: 3,
			want:        []string{"A", "AB", "BC", "C"},
		},
		{
			// Clusters 0-2 hold the A/B/C chain; clusters 3-4 hold a separate D/E chain
			// sharing no cluster with the first group. The merge walks clusters in order
			// each pass, so the order is deterministic: C trails D and E because it only
			// becomes cluster 2's front after BC, on a later pass.
			name: "disjoint cluster groups all emitted",
			observations: []*observation{
				obs("BC", offsetRange(1, 10, 20), offsetRange(2, 0, 10)),
				obs("E", offsetRange(4, 10, 20)),
				obs("A", offsetRange(0, 0, 10)),
				obs("D", offsetRange(3, 0, 10), offsetRange(4, 0, 10)),
				obs("C", offsetRange(2, 10, 20)),
				obs("AB", offsetRange(0, 10, 20), offsetRange(1, 0, 10)),
			},
			numClusters: 5,
			want:        []string{"A", "AB", "BC", "D", "E", "C"},
		},
		{
			// Identical specs with distinct names must all be emitted; their relative
			// order is unspecified. Exercises the identity-based swap in emitCandidate
			// with look-alikes.
			name: "duplicate specs with different names all emitted",
			observations: []*observation{
				obs("dupA", offsetRange(0, 0, 10), offsetRange(1, 0, 10)),
				obs("dupB", offsetRange(0, 0, 10), offsetRange(1, 0, 10)),
			},
			numClusters: 2,
			want:        []string{"dupA", "dupB"},
			unordered:   true,
		},
		{
			// Non-compartment specs (StartOffset/EndOffset, no OffsetRanges) sort via the
			// JobSpec.Ranges() fallback, matching the live single-cluster recovery path.
			name: "legacy specs sort by start offset via ranges fallback",
			observations: []*observation{
				legacyObs("c", 20, 30),
				legacyObs("a", 0, 10),
				legacyObs("b", 10, 20),
			},
			numClusters: 1,
			want:        []string{"a", "b", "c"},
		},
		{
			// Compartments configured, but every job only touched cluster 0, so the
			// other cluster's list is empty and the result is a plain offset sort.
			name: "multiple clusters configured, all jobs in cluster 0",
			observations: []*observation{
				obs("c", offsetRange(0, 20, 30)),
				obs("a", offsetRange(0, 0, 10)),
				obs("b", offsetRange(0, 10, 20)),
			},
			numClusters: 2,
			want:        []string{"a", "b", "c"},
		},
		{
			name:         "no observations",
			observations: nil,
			numClusters:  2,
			want:         []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := orderObservationsForImport(tt.observations, tt.numClusters)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.unordered {
				require.ElementsMatch(t, tt.want, ids(got))
			} else {
				require.Equal(t, tt.want, ids(got))
			}
		})
	}
}
