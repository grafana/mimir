// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"slices"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/multiaggregation"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

var disallowedOperations = map[core.BinaryOperation]struct{}{
	core.BINARY_LOR: {},
}

// NarrowSelectorsOptimizationPass examines a QueryPlan to determine if there are any
// labels that can be used to reduce the amount of data fetched on one side of binary
// expression and propagates those labels as a Hint on the binary expression.
type NarrowSelectorsOptimizationPass struct {
	attempts prometheus.Counter
	modified prometheus.Counter
	logger   log.Logger
}

func NewNarrowSelectorsOptimizationPass(reg prometheus.Registerer, logger log.Logger) *NarrowSelectorsOptimizationPass {
	return &NarrowSelectorsOptimizationPass{
		attempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_narrow_selectors_attempted_total",
			Help: "Total number of queries that the optimization pass has attempted to add hints to narrow selectors for.",
		}),
		modified: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_narrow_selectors_modified_total",
			Help: "Total number of queries where the optimization pass has added hints to narrow selectors for. Incremented whenever any binary expression in the query receives either on-matching include hints or exclude hints.",
		}),
		logger: logger,
	}
}

func (n *NarrowSelectorsOptimizationPass) Name() string {
	return "Narrow selectors"
}

func (n *NarrowSelectorsOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// If this query plan doesn't contain any selectors for us to apply hints for, if the
	// query has been rewritten to be sharded or spun off, don't attempt to generate any
	// query hints since there are no selectors that we understand and can add matchers to.
	res := optimize.InspectSelectors(plan.Root)
	if !res.HasSelectors || res.IsRewrittenByMiddleware {
		return plan, nil
	}

	n.attempts.Inc()
	addedHint := false

	_ = optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, _ []planning.Node) (bool, error) {
		e, ok := node.(*core.BinaryExpression)
		if !ok {
			return true, nil
		}

		// Only set hints for operations that are compatible with adding extra selectors to
		// the right side of the expression. For example, "logical or" includes series from
		// the right side only when they _don't_ have matching label sets on the left side.
		if _, disallowed := disallowedOperations[e.Op]; disallowed {
			return true, nil
		}

		if e.VectorMatching == nil {
			return true, nil
		}

		// Prometheus validates every right-side match group when any fill modifier is active.
		if e.VectorMatching.FillValues.LhsSet || e.VectorMatching.FillValues.RhsSet {
			return true, nil
		}

		// Labels created by label_replace or label_join anywhere within this binary
		// expression's subtree. We must not generate matchers for these labels because they
		// don't exist on the raw series fetched from storage.
		created := createdLabels(e)

		// Labels that keep their value up to the output of the side that receives the matchers.
		kept := labelsKeptBy(narrowedSide(e))

		// Note: "on ()" with an empty label list is intentionally not handled by either
		// branch. It matches all series regardless of labels, so no narrowing hint is useful.
		if e.VectorMatching.On {
			addedHint = n.hintsForOn(ctx, e, created, kept) || addedHint
		} else {
			addedHint = n.hintsForIgnoring(ctx, e, created, kept) || addedHint
		}

		return true, nil
	}))

	if addedHint {
		n.modified.Inc()
	}

	return plan, nil
}

// hintsForOn handles "on (labels)" matching: it uses the matching labels as Include
// hints, without any labels synthesised by label_replace/label_join and without the metric
// name, because many operations remove the metric name before the binary operation, so a
// matcher for it would often select no series. It also removes the labels that the narrowed
// side doesn't keep. Returns true if a hint was set.
func (n *NarrowSelectorsOptimizationPass) hintsForOn(ctx context.Context, e *core.BinaryExpression, created map[string]struct{}, kept keptLabels) bool {
	if len(e.VectorMatching.MatchingLabels) == 0 {
		return false
	}

	include := kept.filter(filterLabelsForDefaultMatching(e.VectorMatching.MatchingLabels, created))
	if len(include) == 0 {
		return false
	}

	if e.Hints == nil {
		e.Hints = &core.BinaryExpressionHints{}
	}
	e.Hints.Include = include

	sl := spanlogger.FromContext(ctx, n.logger)
	sl.DebugLog("msg", "setting on-matching query hint on binary expression", "labels", include)
	return true
}

// hintsForIgnoring creates hints for default and ignoring matching.
func (n *NarrowSelectorsOptimizationPass) hintsForIgnoring(ctx context.Context, e *core.BinaryExpression, created map[string]struct{}, kept keptLabels) bool {
	// Grouping labels can narrow the RHS when the immediate LHS preserves them.
	include := kept.filter(includeFromLHS(e.LHS, created))
	if len(include) > 0 && len(e.VectorMatching.MatchingLabels) > 0 {
		// Remove ignoring labels from the include set.
		filtered := make([]string, 0, len(include))
		for _, lbl := range include {
			if !slices.Contains(e.VectorMatching.MatchingLabels, lbl) {
				filtered = append(filtered, lbl)
			}
		}
		include = filtered
	}

	if len(include) == 0 && !kept.allExcept {
		// The narrowed side only keeps some labels, so exclude-matching mode would build matchers
		// for labels it doesn't keep. Build matchers for the labels it keeps instead. Each of these
		// labels is a matching label, so a series without it on one side can only match a series
		// without it on the other side.
		include = slices.DeleteFunc(filterLabelsForDefaultMatching(kept.names, created), func(lbl string) bool {
			return slices.Contains(e.VectorMatching.MatchingLabels, lbl)
		})

		if len(include) == 0 {
			return false
		}
	}

	if len(include) > 0 {
		if e.Hints == nil {
			e.Hints = &core.BinaryExpressionHints{}
		}
		e.Hints.Include = include

		sl := spanlogger.FromContext(ctx, n.logger)
		sl.DebugLog("msg", "setting include query hint on binary expression from LHS aggregation", "labels", include)
		return true
	}

	// No LHS aggregation labels found: fall back to exclude-matching mode.
	// Tell the operator to build RHS matchers from all LHS labels at query time,
	// excluding the ignoring labels, any synthesised labels and the labels that the narrowed
	// side doesn't keep.
	// Setting Exclude with an empty Include signals exclude-matching mode.
	exclude := slices.Clone(e.VectorMatching.MatchingLabels)
	for lbl := range created {
		if !slices.Contains(exclude, lbl) {
			exclude = append(exclude, lbl)
		}
	}
	for _, lbl := range kept.names {
		if !slices.Contains(exclude, lbl) {
			exclude = append(exclude, lbl)
		}
	}
	slices.Sort(exclude)

	if e.Hints == nil {
		e.Hints = &core.BinaryExpressionHints{}
	}
	e.Hints.Exclude = exclude

	sl := spanlogger.FromContext(ctx, n.logger)
	sl.DebugLog("msg", "setting exclude-matching query hint on binary expression", "excluded_labels", exclude)
	return true
}

// includeFromLHS returns safe Include labels for default and ignoring matching.
func includeFromLHS(node planning.Node, created map[string]struct{}) []string {
	switch e := node.(type) {
	case *core.AggregateExpression:
		// Without aggregations make output labels depend on input labels.
		if e.Without {
			return nil
		}

		// Ungrouped aggregations and by () remove every output label.
		if len(e.Grouping) == 0 {
			return nil
		}

		// A by clause provides safe matching labels, except created labels and the metric name.
		return filterLabelsForDefaultMatching(e.Grouping, created)
	// These three wrappers preserve labels that default matching uses.
	case *core.DeduplicateAndMerge:
		return includeFromLHS(e.Inner, created)
	case *core.StepInvariantExpression:
		return includeFromLHS(e.Inner, created)
	case *core.UnaryExpression:
		return includeFromLHS(e.Inner, created)
	}

	// Other nodes can alter labels, so they stop traversal.
	return nil
}

// filterLabelsForDefaultMatching removes created labels and the metric name.
func filterLabelsForDefaultMatching(lbls []string, created map[string]struct{}) []string {
	out := make([]string, 0, len(lbls))
	for _, lbl := range lbls {
		if lbl == model.MetricNameLabel {
			continue
		}
		if _, ok := created[lbl]; !ok {
			out = append(out, lbl)
		}
	}

	return out
}

// createdLabels returns a set of label names created by a call to label_replace, label_join or
// histogram_quantiles by any children of the given node.
func createdLabels(node planning.Node) map[string]struct{} {
	created := make(map[string]struct{})

	_ = optimize.Walk(node, optimize.VisitorFunc(func(n planning.Node, path []planning.Node) (bool, error) {
		if f, ok := n.(*core.FunctionCall); ok {
			switch f.Function {
			case functions.FUNCTION_LABEL_REPLACE, functions.FUNCTION_LABEL_JOIN, functions.FUNCTION_HISTOGRAM_QUANTILES:
				// The second parameter for label_replace, label_join and histogram_quantiles is the
				// destination label. It is synthesised by the function and does not exist on the raw
				// series fetched from storage, so we must not generate a matcher for it.
				if len(f.Args) > 1 {
					if lbl, ok := f.Args[1].(*core.StringLiteral); ok {
						created[lbl.Value] = struct{}{}
					}
				}
			}
		}

		return true, nil
	}))

	return created
}

// narrowedSide returns the side of e that receives the matchers built from the hints.
func narrowedSide(e *core.BinaryExpression) planning.Node {
	// group_left loads the "one" side (the RHS) first and narrows the "many" side (the LHS).
	if e.VectorMatching.Card == parser.CardManyToOne {
		return e.LHS
	}

	return e.RHS
}

// keptLabels describes the labels that keep their value from the series in storage up to the
// output of a node.
//
// A matcher for a label can only be passed to a node if the node keeps the label. Otherwise the
// value of the label in the output series is not the value in the input series, so the matcher
// selects the wrong input series. This is also true when the matcher accepts series without the
// label, for example when some series on the other side of the binary operation don't have it.
type keptLabels struct {
	// allExcept is true if the node keeps all labels except names, and false if it keeps only names.
	allExcept bool
	names     []string
}

var (
	keepAllLabels = keptLabels{allExcept: true}
	keepNoLabels  = keptLabels{}
)

func keepOnly(names ...string) keptLabels {
	return keptLabels{names: names}
}

func keepAllExcept(names ...string) keptLabels {
	return keptLabels{allExcept: true, names: names}
}

func (k keptLabels) keeps(name string) bool {
	return slices.Contains(k.names, name) != k.allExcept
}

// filter returns the labels in lbls that k keeps.
func (k keptLabels) filter(lbls []string) []string {
	return slices.DeleteFunc(slices.Clone(lbls), func(lbl string) bool {
		return !k.keeps(lbl)
	})
}

// intersect returns the labels that both k and other keep.
func (k keptLabels) intersect(other keptLabels) keptLabels {
	switch {
	case !k.allExcept:
		return keepOnly(other.filter(k.names)...)
	case !other.allExcept:
		return keepOnly(k.filter(other.names)...)
	default:
		return keepAllExcept(slices.Concat(k.names, other.names)...)
	}
}

// labelsKeptBy returns the labels that node keeps on every path that matchers passed to it can take.
func labelsKeptBy(node planning.Node) keptLabels {
	switch n := node.(type) {
	case *core.VectorSelector, *core.MatrixSelector:
		return keepAllLabels
	case *core.DeduplicateAndMerge:
		return labelsKeptBy(n.Inner)
	case *core.DropName:
		return labelsKeptBy(n.Inner)
	case *core.StepInvariantExpression:
		return labelsKeptBy(n.Inner)
	case *core.Subquery:
		return labelsKeptBy(n.Inner)
	case *core.UnaryExpression:
		return labelsKeptBy(n.Inner)
	case *commonsubexpressionelimination.Duplicate:
		return labelsKeptBy(n.Inner)
	case *commonsubexpressionelimination.DuplicateFilter:
		return labelsKeptBy(n.Inner)
	case *rangevectorsplitting.SplitFunctionCall:
		return labelsKeptBy(n.Inner)
	case *core.AggregateExpression:
		return labelsKeptByAggregation(n.AggregateExpressionDetails, n.Param).intersect(labelsKeptBy(n.Inner))
	case *multiaggregation.MultiAggregationInstance:
		return labelsKeptByAggregation(n.Aggregation, n.Param).intersect(labelsKeptBy(n.Group.Inner))
	case *core.FunctionCall:
		return labelsKeptByFunction(n)
	case *core.BinaryExpression:
		return labelsKeptByBinaryExpression(n)
	}

	// Other nodes can change labels in ways that we don't know.
	return keepNoLabels
}

func labelsKeptByAggregation(aggregation *core.AggregateExpressionDetails, param planning.Node) keptLabels {
	if aggregation.Op == core.AGGREGATION_LIMIT_RATIO {
		// limit_ratio selects each series from a hash of its own labels, so a matcher that removes
		// other series doesn't change which series it selects. It keeps all labels of these series.
		return keepAllLabels
	}

	// This also applies to topk, bottomk and limitk, which keep all labels of the series that they
	// select. They select series in each group, so a matcher for a label that is not a grouping
	// label changes which series they select.
	var kept keptLabels
	switch {
	case aggregation.Without:
		kept = keepAllExcept(aggregation.Grouping...)
	case len(aggregation.Grouping) > 0:
		kept = keepOnly(aggregation.Grouping...)
	default:
		return keepNoLabels
	}

	if aggregation.Op == core.AGGREGATION_COUNT_VALUES {
		// count_values sets its label to the sample value.
		lbl, ok := param.(*core.StringLiteral)
		if !ok {
			return keepNoLabels
		}

		kept = kept.intersect(keepAllExcept(lbl.Value))
	}

	return kept
}

// functionLabelBehaviour describes how a function changes the labels of its input series.
type functionLabelBehaviour int

const (
	// The output series have the labels of the input series, except maybe the metric name.
	// label_replace, label_join and histogram_quantiles set a label, which createdLabels handles.
	functionKeepsLabels functionLabelBehaviour = iota

	// The output series have the labels of the input series, except the le label.
	functionRemovesBucketLabel

	// The output depends on whether there are any input series, not on their labels.
	functionUsesNoLabels

	// The output series have the labels of the input series, and the info function adds labels
	// from info series.
	functionAddsInfoLabels
)

// functionLabelBehaviours contains every function that returns an instant vector.
var functionLabelBehaviours = map[functions.Function]functionLabelBehaviour{
	functions.FUNCTION_ABS:                          functionKeepsLabels,
	functions.FUNCTION_ABSENT:                       functionUsesNoLabels,
	functions.FUNCTION_ABSENT_OVER_TIME:             functionUsesNoLabels,
	functions.FUNCTION_ACOS:                         functionKeepsLabels,
	functions.FUNCTION_ACOSH:                        functionKeepsLabels,
	functions.FUNCTION_ASIN:                         functionKeepsLabels,
	functions.FUNCTION_ASINH:                        functionKeepsLabels,
	functions.FUNCTION_ATAN:                         functionKeepsLabels,
	functions.FUNCTION_ATANH:                        functionKeepsLabels,
	functions.FUNCTION_AVG_OVER_TIME:                functionKeepsLabels,
	functions.FUNCTION_CEIL:                         functionKeepsLabels,
	functions.FUNCTION_CHANGES:                      functionKeepsLabels,
	functions.FUNCTION_CLAMP:                        functionKeepsLabels,
	functions.FUNCTION_CLAMP_MAX:                    functionKeepsLabels,
	functions.FUNCTION_CLAMP_MIN:                    functionKeepsLabels,
	functions.FUNCTION_COS:                          functionKeepsLabels,
	functions.FUNCTION_COSH:                         functionKeepsLabels,
	functions.FUNCTION_COUNT_OVER_TIME:              functionKeepsLabels,
	functions.FUNCTION_DAY_OF_MONTH:                 functionKeepsLabels,
	functions.FUNCTION_DAY_OF_WEEK:                  functionKeepsLabels,
	functions.FUNCTION_DAY_OF_YEAR:                  functionKeepsLabels,
	functions.FUNCTION_DAYS_IN_MONTH:                functionKeepsLabels,
	functions.FUNCTION_DEG:                          functionKeepsLabels,
	functions.FUNCTION_DELTA:                        functionKeepsLabels,
	functions.FUNCTION_DERIV:                        functionKeepsLabels,
	functions.FUNCTION_DOUBLE_EXPONENTIAL_SMOOTHING: functionKeepsLabels,
	functions.FUNCTION_EXP:                          functionKeepsLabels,
	functions.FUNCTION_FIRST_OVER_TIME:              functionKeepsLabels,
	functions.FUNCTION_FLOOR:                        functionKeepsLabels,
	functions.FUNCTION_HISTOGRAM_AVG:                functionKeepsLabels,
	functions.FUNCTION_HISTOGRAM_COUNT:              functionKeepsLabels,
	functions.FUNCTION_HISTOGRAM_FRACTION:           functionRemovesBucketLabel,
	functions.FUNCTION_HISTOGRAM_QUANTILE:           functionRemovesBucketLabel,
	functions.FUNCTION_HISTOGRAM_QUANTILES:          functionRemovesBucketLabel,
	functions.FUNCTION_HISTOGRAM_STDDEV:             functionKeepsLabels,
	functions.FUNCTION_HISTOGRAM_STDVAR:             functionKeepsLabels,
	functions.FUNCTION_HISTOGRAM_SUM:                functionKeepsLabels,
	functions.FUNCTION_HOUR:                         functionKeepsLabels,
	functions.FUNCTION_IDELTA:                       functionKeepsLabels,
	functions.FUNCTION_INCREASE:                     functionKeepsLabels,
	functions.FUNCTION_INFO:                         functionAddsInfoLabels,
	functions.FUNCTION_IRATE:                        functionKeepsLabels,
	functions.FUNCTION_LABEL_JOIN:                   functionKeepsLabels,
	functions.FUNCTION_LABEL_REPLACE:                functionKeepsLabels,
	functions.FUNCTION_LAST_OVER_TIME:               functionKeepsLabels,
	functions.FUNCTION_LN:                           functionKeepsLabels,
	functions.FUNCTION_LOG10:                        functionKeepsLabels,
	functions.FUNCTION_LOG2:                         functionKeepsLabels,
	functions.FUNCTION_MAD_OVER_TIME:                functionKeepsLabels,
	functions.FUNCTION_MAX_OVER_TIME:                functionKeepsLabels,
	functions.FUNCTION_MIN_OVER_TIME:                functionKeepsLabels,
	functions.FUNCTION_MINUTE:                       functionKeepsLabels,
	functions.FUNCTION_MONTH:                        functionKeepsLabels,
	functions.FUNCTION_PREDICT_LINEAR:               functionKeepsLabels,
	functions.FUNCTION_PRESENT_OVER_TIME:            functionKeepsLabels,
	functions.FUNCTION_QUANTILE_OVER_TIME:           functionKeepsLabels,
	functions.FUNCTION_RAD:                          functionKeepsLabels,
	functions.FUNCTION_RATE:                         functionKeepsLabels,
	functions.FUNCTION_RESETS:                       functionKeepsLabels,
	functions.FUNCTION_ROUND:                        functionKeepsLabels,
	functions.FUNCTION_SGN:                          functionKeepsLabels,
	functions.FUNCTION_SIN:                          functionKeepsLabels,
	functions.FUNCTION_SINH:                         functionKeepsLabels,
	functions.FUNCTION_SORT:                         functionKeepsLabels,
	functions.FUNCTION_SORT_BY_LABEL:                functionKeepsLabels,
	functions.FUNCTION_SORT_BY_LABEL_DESC:           functionKeepsLabels,
	functions.FUNCTION_SORT_DESC:                    functionKeepsLabels,
	functions.FUNCTION_SQRT:                         functionKeepsLabels,
	functions.FUNCTION_STDDEV_OVER_TIME:             functionKeepsLabels,
	functions.FUNCTION_STDVAR_OVER_TIME:             functionKeepsLabels,
	functions.FUNCTION_SUM_OVER_TIME:                functionKeepsLabels,
	functions.FUNCTION_TAN:                          functionKeepsLabels,
	functions.FUNCTION_TANH:                         functionKeepsLabels,
	functions.FUNCTION_TIMESTAMP:                    functionKeepsLabels,
	functions.FUNCTION_TS_OF_FIRST_OVER_TIME:        functionKeepsLabels,
	functions.FUNCTION_TS_OF_LAST_OVER_TIME:         functionKeepsLabels,
	functions.FUNCTION_TS_OF_MAX_OVER_TIME:          functionKeepsLabels,
	functions.FUNCTION_TS_OF_MIN_OVER_TIME:          functionKeepsLabels,
	functions.FUNCTION_VECTOR:                       functionKeepsLabels,
	functions.FUNCTION_YEAR:                         functionKeepsLabels,
}

func labelsKeptByFunction(f *core.FunctionCall) keptLabels {
	behaviour, ok := functionLabelBehaviours[f.Function]
	if !ok {
		return keepNoLabels
	}

	switch behaviour {
	case functionUsesNoLabels:
		return keepNoLabels
	case functionAddsInfoLabels:
		return labelsKeptByInfo(f)
	}

	// Matchers are only passed to the arguments that are instant or range vectors.
	kept := keepAllLabels
	for _, arg := range f.Args {
		kept = kept.intersect(labelsKeptByVector(arg))
	}

	if behaviour == functionRemovesBucketLabel {
		kept = kept.intersect(keepAllExcept(labels.BucketLabel))
	}

	return kept
}

// labelsKeptByInfo returns the labels that info keeps. info adds the data labels of info series,
// so it can change the value of these labels.
func labelsKeptByInfo(f *core.FunctionCall) keptLabels {
	if len(f.Args) != 2 {
		return keepNoLabels
	}

	selector, ok := f.Args[1].(*core.DataLabelSelector)
	if !ok {
		return keepNoLabels
	}

	var dataLabels []string
	for _, m := range selector.Matchers {
		if m.Name != model.MetricNameLabel {
			dataLabels = append(dataLabels, m.Name)
		}
	}

	if len(dataLabels) == 0 {
		// Without data label matchers, info can add any label.
		return keepNoLabels
	}

	return keepAllExcept(dataLabels...).intersect(labelsKeptBy(f.Args[0]))
}

func labelsKeptByBinaryExpression(e *core.BinaryExpression) keptLabels {
	if e.VectorMatching == nil {
		// One or both sides are scalars, and the output series have the labels of the vector side.
		return labelsKeptByVector(e.LHS).intersect(labelsKeptByVector(e.RHS))
	}

	switch e.Op {
	case core.BINARY_LAND, core.BINARY_LUNLESS:
		// The output series are the LHS series, and only the LHS receives the matchers. The RHS
		// receives matchers built from this operation's own hints.
		return labelsKeptBy(e.LHS)
	}

	if e.VectorMatching.Card == parser.CardManyToOne || e.VectorMatching.Card == parser.CardOneToMany {
		return labelsKeptByGroupedBinaryExpression(e)
	}

	// The matchers go to both sides. With one-to-one matching, the output series only have the
	// matching labels. With or, a series from the RHS is only in the output if no LHS series has
	// the same values for the matching labels, so a matcher for another label can add RHS series.
	kept := keepAllExcept(e.VectorMatching.MatchingLabels...)
	if e.VectorMatching.On {
		kept = keepOnly(e.VectorMatching.MatchingLabels...)
	}

	return kept.intersect(labelsKeptBy(e.LHS)).intersect(labelsKeptBy(e.RHS))
}

// labelsKeptByGroupedBinaryExpression returns the labels that a group_left or group_right
// operation keeps. The output series have the labels of the "many" side, except the included
// labels, which come from the "one" side.
func labelsKeptByGroupedBinaryExpression(e *core.BinaryExpression) keptLabels {
	oneSide, manySide := e.RHS, e.LHS
	if e.VectorMatching.Card == parser.CardOneToMany {
		oneSide, manySide = e.LHS, e.RHS
	}

	keptByOneSide := labelsKeptBy(oneSide)
	kept := keepAllExcept(e.VectorMatching.Include...).intersect(labelsKeptBy(manySide))

	if !e.VectorMatching.On {
		// The "one" side receives the matchers for the ignored labels too, but these labels don't
		// affect which series from the "one" side match.
		return kept.intersect(keepAllExcept(e.VectorMatching.MatchingLabels...)).intersect(keptByOneSide)
	}

	// With on (...), only the matchers for the matching labels go to the "one" side. The other
	// labels come from the "many" side.
	var notKeptByOneSide []string
	for _, lbl := range e.VectorMatching.MatchingLabels {
		if !keptByOneSide.keeps(lbl) {
			notKeptByOneSide = append(notKeptByOneSide, lbl)
		}
	}

	return kept.intersect(keepAllExcept(notKeptByOneSide...))
}

// labelsKeptByVector returns the labels that node keeps if it is an instant or range vector. Other
// nodes, such as scalars and strings, don't receive matchers.
func labelsKeptByVector(node planning.Node) keptLabels {
	resultType, err := node.ResultType()
	if err != nil {
		return keepNoLabels
	}

	if resultType != parser.ValueTypeVector && resultType != parser.ValueTypeMatrix {
		return keepAllLabels
	}

	return labelsKeptBy(node)
}
