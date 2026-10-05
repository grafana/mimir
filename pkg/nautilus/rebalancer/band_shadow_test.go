// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
)

func TestBuildBandPageCutsHotBand(t *testing.T) {
	// Range covers bands 0 and 1. Band 1 holds the mass.
	parent := assignment.HashRange{Lo: 0, Hi: (2 * bandWidth) - 1}
	asg := &assignment.Assignment{Entries: []assignment.Entry{{
		TenantID:    "tenant",
		Range:       parent,
		PartitionID: 1,
	}}}
	resp := &usagetrackerpb.GetTenantBandsResponse{
		Partition:  0,
		Partitions: 64,
		Tenants: []*usagetrackerpb.TenantBands{{
			UserID:         "tenant",
			TotalSeries:    100,
			LocalitySeries: 100,
			Bands:          []uint32{1, 0},
			Counts:         []uint64{80, 20},
		}},
	}
	stats := map[partitionRangeKey]rangeStatsView{
		{tenantID: "tenant", partitionID: 1, hr: parent}: {Series: 6400, SampleRate: 50},
	}
	rates := map[int32]float64{1: 50, 2: 10, 3: 40}
	page := buildBandPage(time.Unix(10, 0), resp, asg, stats, rates, 128, time.Millisecond)
	require.Len(t, page.Tenants, 1)
	tenant := page.Tenants[0]
	require.Equal(t, uint64(6400), tenant.EstimatedSeries)
	require.NotNil(t, tenant.Proposal)
	require.Equal(t, "move", tenant.Proposal.Kind)
	require.Equal(t, int32(1), tenant.Proposal.FromPartition)
	require.Equal(t, int32(2), tenant.Proposal.ToPartition)
	require.Equal(t, uint32(bandWidth), tenant.Proposal.ChildLo)
	require.Equal(t, uint32(2*bandWidth-1), tenant.Proposal.ChildHi)
	require.Contains(t, tenant.Proposal.Text, "not applied")
	require.Equal(t, int64(6400), tenant.Proposal.ReadcacheSeries)
	require.Contains(t, tenant.Ranges[0].HotCaption, "where it sits")
}

func TestBalancePassBooksAcrossPartitions(t *testing.T) {
	// Three equal one-band ranges on p1. Three partitions, so each piece
	// equals the mean and fits on an empty one. p2 has the lower sample
	// rate; a coldest-once picker would send every cut there.
	resp := &usagetrackerpb.GetTenantBandsResponse{Partition: 0, Partitions: 1}
	asg := &assignment.Assignment{}
	for i, id := range []string{"a", "b", "c"} {
		lo := uint32(i) * bandWidth
		resp.Tenants = append(resp.Tenants, &usagetrackerpb.TenantBands{
			UserID: id, TotalSeries: 20, LocalitySeries: 20,
			Bands: []uint32{uint32(i)}, Counts: []uint64{20},
		})
		asg.Entries = append(asg.Entries, assignment.Entry{
			TenantID: id, PartitionID: 1,
			Range: assignment.HashRange{Lo: lo, Hi: lo + bandWidth - 1},
		})
	}
	page := buildBandPage(time.Unix(10, 0), resp, asg, nil, map[int32]float64{1: 100, 2: 1, 3: 50}, 0, 0)
	require.Equal(t, uint64(20), page.Balance.Mean)
	require.Equal(t, 2, page.Balance.Booked)

	byUser := map[string]*bandProposal{}
	for i := range page.Tenants {
		byUser[page.Tenants[i].UserID] = page.Tenants[i].Proposal
	}
	require.Equal(t, "move", byUser["a"].Kind)
	require.Equal(t, int32(2), byUser["a"].ToPartition)
	require.Equal(t, "move", byUser["b"].Kind)
	require.Equal(t, int32(3), byUser["b"].ToPartition)
	require.Equal(t, "shed", byUser["c"].Kind)
	require.Contains(t, byUser["c"].Text, "already brought it")

	after := map[int32]uint64{}
	for _, p := range page.Balance.Partitions {
		after[p.Partition] = p.After
	}
	require.Equal(t, uint64(20), after[1])
	require.Equal(t, uint64(20), after[2])
	require.Equal(t, uint64(20), after[3])
}

func TestBuildBandPageFlatMassDoesNotCut(t *testing.T) {
	parent := assignment.HashRange{Lo: 0, Hi: (2 * bandWidth) - 1}
	asg := &assignment.Assignment{Entries: []assignment.Entry{{
		TenantID:    "tenant",
		Range:       parent,
		PartitionID: 1,
	}}}
	resp := &usagetrackerpb.GetTenantBandsResponse{
		Partition:  0,
		Partitions: 4,
		Tenants: []*usagetrackerpb.TenantBands{{
			UserID:         "tenant",
			TotalSeries:    100,
			LocalitySeries: 40,
			Bands:          []uint32{0, 1},
			Counts:         []uint64{20, 20},
		}},
	}
	page := buildBandPage(time.Unix(10, 0), resp, asg, nil, map[int32]float64{1: 10, 2: 1}, 0, 0)
	require.Equal(t, "flat", page.Tenants[0].Proposal.Kind)
	require.Contains(t, page.Tenants[0].Proposal.Text, "not applied")
	// 60 series on this shard had no locality hash. Scaled by 4 partitions.
	require.Equal(t, uint64(240), page.Tenants[0].Unhashed)
}

func TestServeBandsHTMLSaysNotApplied(t *testing.T) {
	r := &Rebalancer{admin: adminState{}}
	r.admin.setBandPage(bandPage{
		At:         time.Unix(10, 0).UTC(),
		Partition:  0,
		Partitions: 64,
		Tenants: []bandTenantView{{
			UserID: "tenant",
			Proposal: &bandProposal{
				Text: "would move 0x00010000-0x0001ffff from p1 to p2 (not applied)",
			},
			Ranges: []bandRangeView{{
				Label:    "range",
				HasCut:   true,
				CutLeft:  10,
				CutWidth: 20,
				Marks:    []bandMark{{Left: 10, Width: 20, Hot: true, Title: "hot"}},
			}},
		}},
	})
	rec := httptest.NewRecorder()
	r.serveBandsHTML(rec, nil)
	require.Contains(t, rec.Body.String(), "not applied")
	require.Contains(t, rec.Body.String(), "Shadow only")
}
