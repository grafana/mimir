// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/binops"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func (h *BinaryExpressionHints) ToOperatorType() *binops.Hints {
	if h == nil {
		return nil
	}
	return &binops.Hints{
		Include: slices.Clone(h.Include),
	}
}

func (f *VectorMatchFillValues) ToPrometheusType() parser.VectorMatchFillValues {
	// Why are we doing this?
	// The prometheus VectorMatchFillValues uses *float64 fields.
	// In the current mimir protobufs configuration, we are not able to do an optional double (which would map to a *float64)
	// ie message VectorMatchFillValues {
	//  	optional double rhs = 1;
	//  	optional double lhs = 2;
	//}

	out := parser.VectorMatchFillValues{}

	if f.RhsSet {
		out.RHS = &f.Rhs
	}
	if f.LhsSet {
		out.LHS = &f.Lhs
	}
	return out
}

func VectorMatchFillValuesFrom(in parser.VectorMatchFillValues) VectorMatchFillValues {
	out := VectorMatchFillValues{}

	if in.RHS != nil {
		out.Rhs = *in.RHS
		out.RhsSet = true
	}
	if in.LHS != nil {
		out.Lhs = *in.LHS
		out.LhsSet = true
	}
	return out
}

func (v *VectorMatching) ToPrometheusType() *parser.VectorMatching {
	if v == nil {
		return nil
	}
	return &parser.VectorMatching{
		Card:           v.Card,
		MatchingLabels: v.MatchingLabels,
		On:             v.On,
		Include:        v.Include,
		FillValues:     v.FillValues.ToPrometheusType(),
	}
}

func VectorMatchingFrom(v *parser.VectorMatching) *VectorMatching {
	if v == nil {
		return nil
	}
	return &VectorMatching{
		Card:           v.Card,
		MatchingLabels: v.MatchingLabels,
		On:             v.On,
		Include:        v.Include,
		FillValues:     VectorMatchFillValuesFrom(v.FillValues),
	}
}

func PositionRangeFrom(pos posrange.PositionRange) PositionRange {
	return PositionRange(pos)
}

func (p PositionRange) ToPrometheusType() posrange.PositionRange {
	return posrange.PositionRange(p)
}

func LabelMatchersFromPrometheusType(matchers []*labels.Matcher) []*LabelMatcher {
	if len(matchers) == 0 {
		return nil
	}

	converted := make([]*LabelMatcher, 0, len(matchers))

	for _, m := range matchers {
		matcher := LabelMatcherFromPrometheusType(m)
		converted = append(converted, &matcher)
	}

	return converted
}

func LabelMatcherFromPrometheusType(m *labels.Matcher) LabelMatcher {
	return LabelMatcher{
		Name:  m.Name,
		Value: m.Value,
		Type:  m.Type,
	}
}

func LabelMatchersToOperatorType(matchers []*LabelMatcher) types.Matchers {
	if len(matchers) == 0 {
		return nil
	}

	converted := make([]types.Matcher, 0, len(matchers))
	for _, m := range matchers {
		converted = append(converted, types.Matcher{
			Type:  m.Type,
			Name:  m.Name,
			Value: m.Value,
		})
	}

	return converted
}

func matchersEqual(a, b *LabelMatcher) bool {
	return a.Type == b.Type &&
		a.Name == b.Name &&
		a.Value == b.Value
}

// LabelMatchersStringer generates a human-readable version of multiple LabelMatchers
// for use in logs or traces.
type LabelMatchersStringer []*LabelMatcher

func (l LabelMatchersStringer) String() string {
	var builder strings.Builder
	builder.WriteByte('{')

	for i, m := range l {
		builder.WriteString(m.Name)
		builder.WriteString(m.Type.String())
		builder.WriteString(strconv.Quote(m.Value))

		if i < len(l)-1 {
			builder.WriteString(", ")
		}
	}

	builder.WriteByte('}')
	return builder.String()
}

var itemTypeToAggregationOperation = map[parser.ItemType]AggregationOperation{
	parser.SUM:          AGGREGATION_SUM,
	parser.AVG:          AGGREGATION_AVG,
	parser.COUNT:        AGGREGATION_COUNT,
	parser.MIN:          AGGREGATION_MIN,
	parser.MAX:          AGGREGATION_MAX,
	parser.GROUP:        AGGREGATION_GROUP,
	parser.STDDEV:       AGGREGATION_STDDEV,
	parser.STDVAR:       AGGREGATION_STDVAR,
	parser.TOPK:         AGGREGATION_TOPK,
	parser.BOTTOMK:      AGGREGATION_BOTTOMK,
	parser.COUNT_VALUES: AGGREGATION_COUNT_VALUES,
	parser.QUANTILE:     AGGREGATION_QUANTILE,
	parser.LIMITK:       AGGREGATION_LIMITK,
	parser.LIMIT_RATIO:  AGGREGATION_LIMIT_RATIO,
}

var aggregationOperationToItemType = invert(itemTypeToAggregationOperation)

func AggregationOperationFrom(itemType parser.ItemType) (AggregationOperation, error) {
	op, ok := itemTypeToAggregationOperation[itemType]
	if !ok {
		return AGGREGATION_UNKNOWN, compat.NewNotSupportedError(fmt.Sprintf("unknown aggregation operation: %v", itemType.String()))
	}

	return op, nil
}

func (o AggregationOperation) Describe() string {
	item, ok := aggregationOperationToItemType[o]
	if !ok {
		return o.String()
	}

	return item.String()
}

func (o AggregationOperation) ToItemType() (parser.ItemType, bool) {
	item, ok := aggregationOperationToItemType[o]
	return item, ok
}

var itemTypeToBinaryOperation = map[parser.ItemType]BinaryOperation{
	parser.LAND:    BINARY_LAND,
	parser.LOR:     BINARY_LOR,
	parser.LUNLESS: BINARY_LUNLESS,
	parser.ATAN2:   BINARY_ATAN2,
	parser.SUB:     BINARY_SUB,
	parser.ADD:     BINARY_ADD,
	parser.MUL:     BINARY_MUL,
	parser.MOD:     BINARY_MOD,
	parser.DIV:     BINARY_DIV,
	parser.POW:     BINARY_POW,
	parser.EQLC:    BINARY_EQLC,
	parser.NEQ:     BINARY_NEQ,
	parser.LTE:     BINARY_LTE,
	parser.LSS:     BINARY_LSS,
	parser.GTE:     BINARY_GTE,
	parser.GTR:     BINARY_GTR,
}

var binaryOperationToItemType = invert(itemTypeToBinaryOperation)

func BinaryOperationFrom(itemType parser.ItemType) (BinaryOperation, error) {
	op, ok := itemTypeToBinaryOperation[itemType]
	if !ok {
		return BINARY_UNKNOWN, compat.NewNotSupportedError(fmt.Sprintf("unknown binary operation: %v", itemType.String()))
	}

	return op, nil
}

func (o BinaryOperation) Describe() string {
	item, ok := binaryOperationToItemType[o]
	if !ok {
		return o.String()
	}

	return item.String()
}

func (o BinaryOperation) ToItemType() (parser.ItemType, bool) {
	item, ok := binaryOperationToItemType[o]
	return item, ok
}

func UnaryOperationFrom(itemType parser.ItemType) (UnaryOperation, error) {
	if itemType != parser.SUB {
		return UNARY_UNKNOWN, compat.NewNotSupportedError(fmt.Sprintf("unknown unary operation: %v", itemType.String()))
	}

	return UNARY_SUB, nil
}

func (o UnaryOperation) Describe() string {
	if o != UNARY_SUB {
		return o.String()
	}

	return parser.ItemType(parser.SUB).String()
}

func invert[A, B comparable](original map[A]B) map[B]A {
	inverted := make(map[B]A, len(original))

	for k, v := range original {
		before := len(inverted)
		inverted[v] = k
		after := len(inverted)

		if before == after {
			panic(fmt.Sprintf("duplicate value %v detected", v))
		}
	}

	return inverted
}

func TimeFromTimestamp(ts *int64) *time.Time {
	if ts == nil {
		return nil
	}

	t := timestamp.Time(*ts)
	return &t
}

func TimestampFromTime(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	ts := timestamp.FromTime(*t)
	return &ts
}
