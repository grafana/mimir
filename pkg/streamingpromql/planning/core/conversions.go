// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
)

func (v *VectorMatching) ToPrometheusType() *parser.VectorMatching {
	return (*parser.VectorMatching)(v)
}

func VectorMatchingFrom(v *parser.VectorMatching) *VectorMatching {
	return (*VectorMatching)(v)
}

func PositionRangeFrom(pos posrange.PositionRange) PositionRange {
	return PositionRange(pos)
}

func (p PositionRange) ToPrometheusType() posrange.PositionRange {
	return posrange.PositionRange(p)
}

func LabelMatchersFrom(matchers []*labels.Matcher) []*LabelMatcher {
	if len(matchers) == 0 {
		return nil
	}

	converted := make([]*LabelMatcher, 0, len(matchers))

	for _, m := range matchers {
		converted = append(converted, &LabelMatcher{
			Name:  m.Name,
			Value: m.Value,
			Type:  m.Type,
		})
	}

	return converted
}

// LabelMatchersToPrometheusType converts matchers to a labels.Matcher slice.
//
// Any regex matchers will have their patterns parsed, and this method will return an error if parsing fails.
func LabelMatchersToPrometheusType(matchers []*LabelMatcher) ([]*labels.Matcher, error) {
	if len(matchers) == 0 {
		return nil, nil
	}

	converted := make([]*labels.Matcher, 0, len(matchers))

	for _, m := range matchers {
		m, err := labels.NewMatcher(m.Type, m.Name, m.Value)
		if err != nil {
			return nil, err
		}

		converted = append(converted, m)
	}

	return converted, nil
}

func matchersEqual(a, b *LabelMatcher) bool {
	return a.Type == b.Type &&
		a.Name == b.Name &&
		a.Value == b.Value
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
