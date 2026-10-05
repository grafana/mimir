// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// twoMovePage is a snapshot with one tenant and two booked moves out of
// p1: a one-band piece to p2 (the larger) and a one-band piece to p3.
func twoMovePage() bandPage {
	return bandPage{
		At:         time.Unix(10, 0).UTC(),
		Partition:  0,
		Partitions: 64,
		Tenants: []bandTenantView{{
			UserID:          "tenant",
			EstimatedSeries: 6400,
			Proposal: &bandProposal{
				Kind:     "move",
				Text:     "would move 00010000-0001ffff (80 series) from p1 to p2 (not applied)",
				ParentLo: 0, ParentHi: 2*bandWidth - 1,
				ChildLo: bandWidth, ChildHi: 2*bandWidth - 1,
				HotLo: bandWidth, HotHi: 2*bandWidth - 1,
			},
			Ranges: []bandRangeView{
				{
					Lo: 0, Hi: 2*bandWidth - 1, Partition: 1, HasCut: true, Width: "2 bands",
					Decision: "would move 00010000-0001ffff (80 series) from p1 to p2; booked (not applied)",
					Marks: []bandMark{
						{Lo: 0, Hi: bandWidth - 1, Estimated: 20},
						{Lo: bandWidth, Hi: 2*bandWidth - 1, Estimated: 80, Hot: true},
					},
				},
				{
					Lo: 2 * bandWidth, Hi: 3*bandWidth - 1, Partition: 1, HasCut: true, Width: "1 band",
					Decision: "would move 00020000-0002ffff (30 series) from p1 to p3; booked (not applied)",
					Marks:    []bandMark{{Lo: 2 * bandWidth, Hi: 3*bandWidth - 1, Estimated: 30, Hot: true}},
				},
				{Lo: 3 * bandWidth, Hi: 0xffffffff, Partition: 4, Width: "65533 bands", Decision: "no retained band overlaps this range"},
			},
		}},
		Balance: bandBalance{
			Mean:   50,
			Booked: 2,
			Partitions: []bandPartitionFill{
				{Partition: 1, Before: 130, After: 20, Over: true},
				{Partition: 2, Before: 0, After: 80},
				{Partition: 3, Before: 10, After: 40},
			},
			Moves: []bandMove{
				// Booked in pass order: the smaller one first, to prove the
				// page renumbers largest first.
				{Tenant: "tenant", From: 1, To: 3, ParentLo: 2 * bandWidth, ParentHi: 3*bandWidth - 1, Lo: 2 * bandWidth, Hi: 3*bandWidth - 1, HotLo: 2 * bandWidth, HotHi: 3*bandWidth - 1, Series: 30},
				{Tenant: "tenant", From: 1, To: 2, ParentLo: 0, ParentHi: 2*bandWidth - 1, Lo: bandWidth, Hi: 2*bandWidth - 1, HotLo: bandWidth, HotHi: 2*bandWidth - 1, Series: 80},
			},
		},
	}
}

func TestServeUsageTrackerHTML(t *testing.T) {
	r := &Rebalancer{admin: adminState{}}
	r.admin.setBandPage(twoMovePage())

	index := httptest.NewRecorder()
	r.serveUsageTrackerHTML(index, httptest.NewRequest("GET", "/nautilus/rebalancer/usage-tracker", nil))
	body := index.Body.String()
	require.Contains(t, body, "Signals from Usage Tracker")
	require.Contains(t, body, "nothing on this page is applied")
	require.NotContains(t, body, "<svg")
	require.Contains(t, body, `href="/nautilus/rebalancer/usage-tracker?user=tenant"`)
	// The moves count is the pass's bookings, not the tenants with a move.
	require.Contains(t, body, "2 moves booked across 3 partitions")
	// Partitions in id order, each with its moves.
	p1 := strings.Index(body, "<span class=\"part-id\">p1</span>")
	p2 := strings.Index(body, "<span class=\"part-id\">p2</span>")
	p3 := strings.Index(body, "<span class=\"part-id\">p3</span>")
	require.True(t, p1 > 0 && p1 < p2 && p2 < p3, "partitions by id")
	require.Contains(t, body, "Out of p1 (110 series)")
	require.Contains(t, body, "Into p2 (80 series)")
	require.Contains(t, body, "Into p3 (30 series)")
	require.Contains(t, body, "2 out · 0 in")
	// Links from the pass land on the tenant's move numbering.
	require.Contains(t, body, `usage-tracker?user=tenant&amp;move=1">tenant</a></td><td class="mono">00010000–0001ffff`)
	require.Contains(t, body, `usage-tracker?user=tenant&amp;move=2">tenant</a></td><td class="mono">00020000–0002ffff`)
	// Sections are collapsed by default.
	require.NotContains(t, body, `<details class="section" open>`)

	one := httptest.NewRecorder()
	r.serveUsageTrackerHTML(one, httptest.NewRequest("GET", "/nautilus/rebalancer/usage-tracker?user=tenant", nil))
	body = one.Body.String()
	require.Contains(t, body, "<svg")
	require.Contains(t, body, "Move 1 of 2")
	require.Contains(t, body, "Zoomed to move 1")
	require.Contains(t, body, "2 booked for this tenant, 110 series")
	require.Contains(t, body, `fill="#e15759"`)
	require.Equal(t, 2, strings.Count(body, `rect class="cut"`), "selected move outlined in both drawings")
	require.Equal(t, 2, strings.Count(body, `rect class="other"`), "other move dashed in both drawings")

	two := httptest.NewRecorder()
	r.serveUsageTrackerHTML(two, httptest.NewRequest("GET", "/nautilus/rebalancer/usage-tracker?user=tenant&move=2", nil))
	require.Contains(t, two.Body.String(), "Move 2 of 2")
	require.Contains(t, two.Body.String(), "Zoomed to move 2")

	missing := httptest.NewRecorder()
	r.serveUsageTrackerHTML(missing, httptest.NewRequest("GET", "/nautilus/rebalancer/usage-tracker?user=missing", nil))
	require.Equal(t, 404, missing.Code)
}

func TestServeUsageTrackerHTMLEscapesTenantOnce(t *testing.T) {
	// html/template percent-encodes query values itself; a second pass
	// would turn a/b into a%252Fb and the link would 404.
	page := twoMovePage()
	page.Tenants[0].UserID = "a/b c"
	for i := range page.Balance.Moves {
		page.Balance.Moves[i].Tenant = "a/b c"
	}
	r := &Rebalancer{admin: adminState{}}
	r.admin.setBandPage(page)
	index := httptest.NewRecorder()
	r.serveUsageTrackerHTML(index, httptest.NewRequest("GET", "/nautilus/rebalancer/usage-tracker", nil))
	require.Contains(t, index.Body.String(), `href="/nautilus/rebalancer/usage-tracker?user=a%2fb%20c"`)
	require.NotContains(t, index.Body.String(), "%252f")

	one := httptest.NewRecorder()
	r.serveUsageTrackerHTML(one, httptest.NewRequest("GET", "/nautilus/rebalancer/usage-tracker?user=a%2Fb+c", nil))
	require.Equal(t, 200, one.Code)
	require.Contains(t, one.Body.String(), "Move 1 of 2")
}

func TestTenantDrawingSeparatesRanges(t *testing.T) {
	// Three ranges, two partitions whose numbers share a residue mod the
	// palette size, two of them adjacent on the same partition.
	tenant := &bandTenantView{Ranges: []bandRangeView{
		{Lo: 0, Hi: bandWidth - 1, Partition: 144},
		{Lo: bandWidth, Hi: 2*bandWidth - 1, Partition: 144},
		{Lo: 2 * bandWidth, Hi: 3*bandWidth - 1, Partition: 294},
	}}
	colors := tenantColors(tenant)
	require.NotEqual(t, colors[144], colors[294], "partitions get distinct colors per tenant")
	for _, c := range colors {
		require.NotEqual(t, hotFill, c, "red is the hot band")
	}

	d := buildTenantDrawing(tenant, bandBalance{}, 0)
	var fills []string
	var separators int
	for _, r := range d.Strip {
		if r.Title == "" {
			separators++
			continue
		}
		fills = append(fills, r.Fill)
	}
	require.Len(t, fills, 3)
	require.NotEqual(t, fills[0], fills[1], "adjacent ranges on one partition alternate shade")
	require.Equal(t, tint(fills[0]), fills[1])
	// At full scale each one-band range is a fraction of a unit wide, so
	// no boundary line is drawn: a line would be wider than the ranges.
	require.Equal(t, 0, separators)
	require.Equal(t, "#9db5ce", tint("#4e79a7"))
	require.Nil(t, d.Selected)
	require.Empty(t, d.Outlines)
}

func TestTenantDrawingSelectedRangeOnTop(t *testing.T) {
	page := twoMovePage()
	tenant := &page.Tenants[0]
	d := buildTenantDrawing(tenant, page.Balance, 1)
	require.NotNil(t, d.Selected)
	require.Equal(t, 1, d.Selected.Index)
	require.Equal(t, int32(2), d.Selected.To, "largest move is selected by default")
	require.Equal(t, "110", d.MoveSeries)

	// The parent of the selected move is drawn last on the strip, so the
	// color under the outline is its own even when neighbours overdraw it.
	last := d.Strip[len(d.Strip)-1]
	require.Contains(t, last.Title, "00000000–0001ffff on p1")
	require.Equal(t, d.Strip[0].Fill, last.Fill)

	// Wide neighbours at zoom get a boundary line; the window holds the
	// parent and the second range on p1, so p1 and p4 meet at one boundary
	// and the two p1 ranges at another.
	require.NotNil(t, d.Zoom)
	var zoomSeparators int
	for _, r := range d.Zoom.Strip {
		if r.Title == "" {
			zoomSeparators++
		}
	}
	require.Equal(t, 2, zoomSeparators)

	// Rows: selected first, then the other moved range, then the rest.
	require.True(t, d.Rows[0].Selected)
	require.True(t, d.Rows[1].Moved)
	require.False(t, d.Rows[2].Moved)
}

func TestPassViewOrdersByPartition(t *testing.T) {
	page := twoMovePage()
	v := buildPassView(page.Balance)
	require.Equal(t, "50", v.Mean)
	require.Equal(t, "140", v.Total)
	require.Equal(t, "110", v.MovingSeries)
	require.Equal(t, 1, v.OverCount)
	require.Len(t, v.Rows, 3)
	require.Equal(t, int32(1), v.Rows[0].Partition)
	require.Equal(t, "−110", v.Rows[0].Delta)
	require.Len(t, v.Rows[0].Out, 2)
	require.Equal(t, "80", v.Rows[0].Out[0].Series, "moves out are largest first")
	require.Equal(t, 1, v.Rows[0].Out[0].TenantIndex)
	require.Equal(t, 2, v.Rows[0].Out[1].TenantIndex)
	require.Equal(t, "+80", v.Rows[1].Delta)
	require.Len(t, v.Rows[1].In, 1)
	require.Equal(t, "+30", v.Rows[2].Delta)
}

func TestCommas(t *testing.T) {
	require.Equal(t, "0", commas(0))
	require.Equal(t, "999", commas(999))
	require.Equal(t, "1,000", commas(1000))
	require.Equal(t, "182,197", commas(182197))
	require.Equal(t, "1,000,000", commas(1000000))
}
