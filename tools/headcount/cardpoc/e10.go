// SPDX-License-Identifier: AGPL-3.0-only

package cardpoc

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/tools/headcount/model"
)

// Budget caps the work one label breakdown may do. Zero means no limit.
type Budget struct {
	MaxSeries int
}

// Breakdown is one metric's series count per value of one label, over one
// block range.
type Breakdown struct {
	Metric, Label string
	MinT, MaxT    int64
	Counts        map[string]int
	// SeriesTouched is how many series the breakdown read labels for.
	SeriesTouched int
	// PostingsBytes is the size of the metric's postings lists read: 4
	// bytes per entry plus the 4-byte entry count, per block.
	PostingsBytes int64
	// LowerBound is set when the budget tripped. The breakdown then covers
	// only the series read before it tripped, so every count is at or
	// below the true count, and a value may be missing entirely.
	LowerBound bool
	Elapsed    time.Duration
}

// Total returns the sum of every value's count.
func (b Breakdown) Total() int {
	n := 0
	for _, c := range b.Counts {
		n += c
	}
	return n
}

// LabelBreakdown counts metric's series per value of label over the blocks
// whose range is exactly r, reading the metric's postings and each series'
// labels from the full index, and stops as soon as budget is reached. The
// blocks must share no series (see NameCountsForRange), so each series is
// counted once and a partial breakdown never overcounts.
func LabelBreakdown(bucketDir string, r BlockRange, metric, label string, budget Budget) (Breakdown, error) {
	metas, err := readMetas(bucketDir)
	if err != nil {
		return Breakdown{}, err
	}
	var chosen []*block.Meta
	for _, m := range metas {
		if m.MinTime == r.MinT && m.MaxTime == r.MaxT {
			chosen = append(chosen, m)
		}
	}
	if len(chosen) == 0 {
		return Breakdown{}, fmt.Errorf("no block covers exactly [%d, %d)", r.MinT, r.MaxT)
	}
	if err := checkDisjointShards(chosen); err != nil {
		return Breakdown{}, err
	}
	// Block-ID order, so a budgeted run always reads the same series.
	sort.Slice(chosen, func(i, j int) bool { return chosen[i].ULID.Compare(chosen[j].ULID) < 0 })
	var dirs []string
	for _, m := range chosen {
		dirs = append(dirs, filepath.Join(bucketDir, "anonymous", m.ULID.String()))
	}

	bd := Breakdown{Metric: metric, Label: label, MinT: r.MinT, MaxT: r.MaxT, Counts: map[string]int{}}
	start := time.Now()
	for _, dir := range dirs {
		tripped, err := addBlockBreakdown(dir, &bd, budget)
		if err != nil {
			return Breakdown{}, fmt.Errorf("block %s: %w", filepath.Base(dir), err)
		}
		if tripped {
			bd.LowerBound = true
			break
		}
	}
	bd.Elapsed = time.Since(start)
	return bd, nil
}

// addBlockBreakdown adds one block's series to bd and reports whether the
// budget tripped before the block was finished.
func addBlockBreakdown(dir string, bd *Breakdown, budget Budget) (bool, error) {
	r, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return false, err
	}
	defer r.Close()

	ctx := context.Background()
	p, err := r.Postings(ctx, "__name__", bd.Metric)
	if err != nil {
		return false, err
	}
	bd.PostingsBytes += 4 // the list's entry count.

	var b labels.ScratchBuilder
	for p.Next() {
		if budget.MaxSeries > 0 && bd.SeriesTouched >= budget.MaxSeries {
			return true, nil
		}
		bd.PostingsBytes += 4
		if err := r.Series(p.At(), &b, nil); err != nil {
			return false, err
		}
		bd.Counts[b.Labels().Get(bd.Label)]++
		bd.SeriesTouched++
	}
	return false, p.Err()
}

// E10Result is RunE10's outcome: one breakdown without a budget and one
// with a budget below the metric's series count, both against the
// population's truth for the same range.
type E10Result struct {
	Budget   Budget
	Exact    Breakdown
	Budgeted Breakdown
	Truth    map[string]int
}

// Pass reports whether the unbudgeted breakdown equals the truth, and the
// budgeted one tripped at exactly the budget with every count at or below
// the truth.
func (r E10Result) Pass() bool {
	if r.Exact.LowerBound || !equalCounts(r.Exact.Counts, r.Truth) {
		return false
	}
	if !r.Budgeted.LowerBound || r.Budgeted.SeriesTouched != r.Budget.MaxSeries || r.Budgeted.Total() != r.Budget.MaxSeries {
		return false
	}
	for value, n := range r.Budgeted.Counts {
		if n > r.Truth[value] {
			return false
		}
	}
	return true
}

// RunE10 breaks metric down by label over range r, once without a budget
// and once with budget, and checks both against the population's truth.
// budget.MaxSeries must be positive and below the metric's series count
// in r, or the budgeted run cannot trip.
func RunE10(bucketDir string, pop *model.Model, r BlockRange, metric, label string, budget Budget) (E10Result, error) {
	matcher := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", metric)}
	truth := pop.TruthBy(matcher, label, r.MinT, r.MaxT)
	total := 0
	for _, n := range truth {
		total += n
	}
	if budget.MaxSeries <= 0 || budget.MaxSeries >= total {
		return E10Result{}, fmt.Errorf("budget %d must be positive and below the metric's %d series", budget.MaxSeries, total)
	}

	exact, err := LabelBreakdown(bucketDir, r, metric, label, Budget{})
	if err != nil {
		return E10Result{}, err
	}
	budgeted, err := LabelBreakdown(bucketDir, r, metric, label, budget)
	if err != nil {
		return E10Result{}, err
	}
	return E10Result{Budget: budget, Exact: exact, Budgeted: budgeted, Truth: truth}, nil
}

func equalCounts(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
