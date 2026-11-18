// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log/level"

	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// ReadBounds is the read field and path label of a bounds answer: every block from its postings
// offsets table, and the heads from the label values cardinality RPC.
const ReadBounds = "bounds"

type boundsGroup struct {
	Value string `json:"value"`
	Lower int64  `json:"lower"`
	Upper int64  `json:"upper"`
}

type boundsRangeResponse struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	// MainSet is set when the range's in-order blocks of exactly the range are distinct shards, whose
	// counts add up to lower.
	MainSet     bool `json:"main_set"`
	MainBlocks  int  `json:"main_blocks"`
	OtherBlocks int  `json:"other_blocks"`
	// LowerBound is set when a block or a main shard of the range was not counted: upper may then be short too.
	LowerBound    bool           `json:"lower_bound"`
	MissingBlocks []string       `json:"missing_blocks,omitempty"`
	MissingShards []missingShard `json:"missing_shards,omitempty"`
	GroupCount    int            `json:"group_count"`
	LowerTotal    int64          `json:"lower_total"`
	UpperTotal    int64          `json:"upper_total"`
	HasMore       bool           `json:"has_more"`
	Groups        []boundsGroup  `json:"groups"`
}

type boundsHeadGroup struct {
	Value    string `json:"value"`
	InMemory int64  `json:"in_memory"`
	Active   int64  `json:"active"`
}

type boundsHeadResponse struct {
	// Approximate is set when the ingesters do not read from ingest storage: both figures are then
	// estimated from the zones' replicas.
	Approximate   bool              `json:"approximate"`
	GroupCount    int               `json:"group_count"`
	InMemoryTotal int64             `json:"in_memory_total"`
	ActiveTotal   int64             `json:"active_total"`
	HasMore       bool              `json:"has_more"`
	Groups        []boundsHeadGroup `json:"groups"`
}

type boundsResponse struct {
	Mode    string                `json:"mode"`
	Window  estimateWindow        `json:"window"`
	GroupBy string                `json:"group_by"`
	Ranges  []boundsRangeResponse `json:"ranges"`
	// NotCovered holds the parts of the window no block range answers.
	NotCovered []estimateWindow    `json:"not_covered,omitempty"`
	Head       *boundsHeadResponse `json:"head,omitempty"`
}

// serveBounds answers a bounds request. Each row set is sorted by upper value, then by value, groups
// with an upper value below min_count are left out, and limit cuts each row set.
func (e *CardinalityEstimator) serveBounds(ctx context.Context, w http.ResponseWriter, r *http.Request, tenantID string, req CardinalityEstimateRequest, opts estimateRequestOptions, start time.Time, timedOut func(error) error) {
	q := e.blocks
	var (
		res BoundsResult
		err error
	)
	if e.clampToLookback(spanlogger.FromContext(ctx, q.logger), tenantID, &req) {
		res, err = e.Bounds(ctx, tenantID, &req)
	}
	if limit := req.MaxGroups; err == nil && limit > 0 {
		for _, rng := range res.Ranges {
			if int64(len(rng.Upper)) > limit {
				err = errOverMaxGroups("the blocks of one range", limit, req.GroupBy)
			}
		}
		if res.Head != nil && int64(len(res.Head.InMemory)) > limit {
			err = errOverMaxGroups("the ingesters' heads", limit, req.GroupBy)
		}
	}
	err = timedOut(err)
	q.observeBounds(r.Context(), req, res, time.Since(start), err)
	if err != nil {
		writeEstimateError(w, err)
		return
	}
	out := boundsResponse{Mode: ReadBounds, Window: estimateWindow{Start: req.MinT, End: req.MaxT}, GroupBy: req.GroupBy, Ranges: []boundsRangeResponse{}, NotCovered: res.NotCovered}
	for _, rng := range res.Ranges {
		ro := boundsRangeResponse{
			Start: rng.MinT, End: rng.MaxT, MainSet: rng.MainSet, MainBlocks: rng.MainBlocks, OtherBlocks: rng.OtherBlocks,
			LowerBound: rng.LowerBound, MissingBlocks: convertULIDsToString(rng.MissingBlocks), MissingShards: rng.MissingShards,
			GroupCount: len(rng.Upper), Groups: []boundsGroup{},
		}
		for v, upper := range rng.Upper {
			lower := rng.Lower[v]
			ro.LowerTotal += lower
			ro.UpperTotal += upper
			if upper >= opts.minCount {
				ro.Groups = append(ro.Groups, boundsGroup{Value: v, Lower: lower, Upper: upper})
			}
		}
		slices.SortFunc(ro.Groups, func(a, b boundsGroup) int { return compareByUpper(a.Upper, b.Upper, a.Value, b.Value) })
		if opts.limit > 0 && opts.limit < len(ro.Groups) {
			ro.Groups, ro.HasMore = ro.Groups[:opts.limit], true
		}
		out.Ranges = append(out.Ranges, ro)
	}
	if h := res.Head; h != nil {
		ho := &boundsHeadResponse{Approximate: h.Approximate, GroupCount: len(h.InMemory), Groups: []boundsHeadGroup{}}
		for v, n := range h.InMemory {
			ho.InMemoryTotal += n
			if n >= opts.minCount {
				ho.Groups = append(ho.Groups, boundsHeadGroup{Value: v, InMemory: n, Active: h.Active[v]})
			}
		}
		for _, n := range h.Active {
			ho.ActiveTotal += n
		}
		slices.SortFunc(ho.Groups, func(a, b boundsHeadGroup) int { return compareByUpper(a.InMemory, b.InMemory, a.Value, b.Value) })
		if opts.limit > 0 && opts.limit < len(ho.Groups) {
			ho.Groups, ho.HasMore = ho.Groups[:opts.limit], true
		}
		out.Head = ho
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(&out); err != nil {
		level.Warn(util_log.WithContext(r.Context(), q.logger)).Log("msg", "cardinality estimate: writing the bounds answer failed", "err", err)
	}
}

// compareByUpper orders by count, largest first, then by value.
func compareByUpper(a, b int64, va, vb string) int {
	if a != b {
		if a > b {
			return -1
		}
		return 1
	}
	return strings.Compare(va, vb)
}
