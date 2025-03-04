// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

func init() {
	// TODO: register these encoders and decoders in the engine rather than globally to ensure they don't affect anything else
	jsoniter.RegisterTypeEncoderFunc("planning.QueryPlan", encodeQueryPlan, func(_ unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("planning.QueryPlan", decodeQueryPlan)

	jsoniter.RegisterTypeEncoderFunc("posrange.PositionRange", encodePositionRange, func(_ unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("posrange.PositionRange", decodePositionRange)

	jsoniter.RegisterTypeEncoderFunc("labels.Matcher", encodeMatcher, func(_ unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("labels.Matcher", decodeMatcher)

	jsoniter.RegisterTypeEncoderFunc("parser.ItemType", encodeItemType, func(_ unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("parser.ItemType", decodeItemType)
	buildItemTypeMap()
}

func encodeQueryPlan(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	plan := (*QueryPlan)(ptr)

	stream.WriteObjectStart()

	stream.WriteObjectField("timeRange")
	stream.WriteVal(plan.TimeRange)
	stream.WriteMore()

	stream.WriteObjectField("nodes")
	encodeNodes(plan.Root, stream)

	stream.WriteObjectEnd()
}

func encodeNodes(root Node, stream *jsoniter.Stream) {
	stream.WriteArrayStart()
	encodeNode(root, stream, map[Node]int{})
	stream.WriteArrayEnd()
}

func encodeNode(n Node, stream *jsoniter.Stream, nodesToIndex map[Node]int) {
	children := n.Children()

	// Check all children have been encoded already.
	for _, child := range children {
		if _, haveWritten := nodesToIndex[child]; !haveWritten {
			encodeNode(child, stream, nodesToIndex)
		}
	}

	if len(nodesToIndex) > 0 {
		stream.WriteMore()
	}

	stream.WriteObjectStart()

	stream.WriteObjectField("type")
	stream.WriteString(n.Type())
	stream.WriteMore()

	stream.WriteObjectField("details")
	stream.WriteVal(n)

	if len(children) > 0 {
		stream.WriteMore()
		stream.WriteObjectField("children")
		stream.WriteArrayStart()

		for childIdx, child := range children {
			nodeIdx := nodesToIndex[child]

			if childIdx > 0 {
				stream.WriteMore()
			}

			stream.WriteInt(nodeIdx)
		}

		stream.WriteArrayEnd()
	}

	stream.WriteObjectEnd()

	nodesToIndex[n] = len(nodesToIndex)
}

func decodeQueryPlan(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	plan := (*QueryPlan)(ptr)

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, key string) bool {
		switch key {
		case "timeRange":
			iter.ReadVal(&plan.TimeRange)
		case "nodes":
			plan.Root = decodeNodes(iter)
		default:
			// Unknown field, skip.
			iter.Skip()
		}

		return true
	})
}

func decodeNodes(iter *jsoniter.Iterator) Node {
	var nodes []Node

	iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
		var node Node
		var nodeType string
		var childrenIndices []int

		ok := iter.ReadObjectCB(func(iter *jsoniter.Iterator, key string) bool {
			switch key {
			case "type":
				nodeType = iter.ReadString()
			case "details":
				if nodeType == "" {
					iter.ReportError("decodeNodes", "expected to read node type before details")
					return false
				}

				node = decodeNodeDetails(nodeType, iter)
			case "children":
				iter.ReadVal(&childrenIndices)
			default:
				// Unknown field, skip.
				iter.Skip()
			}

			return true
		})

		if !ok {
			return false
		}

		children := make([]Node, 0, len(childrenIndices))

		for _, idx := range childrenIndices {
			if idx >= len(nodes) {
				iter.ReportError("decodeNodes", fmt.Sprintf("node of type %q specifies child with index %v, but have only read %v nodes so far", nodeType, idx, len(nodes)))
				return false
			}

			child := nodes[idx]
			children = append(children, child)
		}

		err := node.SetChildren(children)
		if err != nil {
			iter.ReportError("decodeNodes", err.Error())
			return false
		}

		nodes = append(nodes, node)
		return true
	})

	if len(nodes) == 0 {
		iter.ReportError("decodeNodes", "no nodes found")
		return nil
	}

	return nodes[len(nodes)-1] // Root node will always be last node.
}

func decodeNodeDetails(nodeType string, iter *jsoniter.Iterator) Node {
	// FIXME: move this into Engine so we can register different node types without this switch statement
	var n Node

	switch nodeType {
	case "AggregateExpression":
		n = &AggregateExpression{}
	case "BinaryExpression":
		n = &BinaryExpression{}
	case "FunctionCall":
		n = &FunctionCall{}
	case "NumberLiteral":
		n = &NumberLiteral{}
	case "StringLiteral":
		n = &StringLiteral{}
	case "UnaryExpression":
		n = &UnaryExpression{}
	case "VectorSelector":
		n = &VectorSelector{}
	case "MatrixSelector":
		n = &MatrixSelector{}
	case "Subquery":
		n = &Subquery{}
	default:
		iter.ReportError("decodeNodeDetails", fmt.Sprintf("unknown node type %q", nodeType))
		return nil
	}

	iter.ReadVal(n)

	return n
}

func encodePositionRange(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	posRange := (*posrange.PositionRange)(ptr)

	stream.WriteArrayStart()
	stream.WriteInt(int(posRange.Start))
	stream.WriteMore()
	stream.WriteInt(int(posRange.End))
	stream.WriteArrayEnd()
}

// TODO: test malformed input to this (eg. object, not enough array elements, too many elements etc.)
func decodePositionRange(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	posRange := (*posrange.PositionRange)(ptr)

	if !iter.ReadArray() {
		iter.ReportError("posrange.PositionRange", "expected [")
		return
	}

	posRange.Start = posrange.Pos(iter.ReadInt())

	if !iter.ReadArray() {
		iter.ReportError("posrange.PositionRange", "expected ,")
		return
	}

	posRange.End = posrange.Pos(iter.ReadInt())

	if iter.ReadArray() {
		iter.ReportError("posrange.PositionRange", "expected ]")
		return
	}
}

func encodeMatcher(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	matcher := (*labels.Matcher)(ptr)

	stream.WriteObjectStart()

	stream.WriteObjectField("type")
	stream.WriteInt(int(matcher.Type))
	stream.WriteMore()

	stream.WriteObjectField("name")
	stream.WriteString(matcher.Name)
	stream.WriteMore()

	stream.WriteObjectField("value")
	stream.WriteString(matcher.Value)

	stream.WriteObjectEnd()
}

// TODO: test to check that regexp is correctly parsed
func decodeMatcher(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	matcher := (*labels.Matcher)(ptr)

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, key string) bool {
		switch key {
		case "type":
			matcher.Type = labels.MatchType(iter.ReadInt())
		case "name":
			matcher.Name = iter.ReadString()
		case "value":
			matcher.Value = iter.ReadString()
		default:
			// Unknown field, skip.
			iter.Skip()
		}

		return true
	})

	// Recreate the matcher so that the internal regexp is populated, if needed.
	// TODO: could possibly skip this for non-regexp matchers
	m, err := labels.NewMatcher(matcher.Type, matcher.Name, matcher.Value)
	if err != nil {
		iter.ReportError("labels.NewMatcher", err.Error())
		return
	}

	*matcher = *m
}

var stringToItemType = map[string]parser.ItemType{}

func buildItemTypeMap() {
	for i, s := range parser.ItemTypeStr {
		stringToItemType[s] = i
	}
}

// Encode ItemTypes as strings, rather than their IDs, as the IDs are not guaranteed to be stable
// and so upgrading Prometheus could cause backwards compatibility issues.
func encodeItemType(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	itemType := (*parser.ItemType)(ptr)

	stream.WriteString(itemType.String())
}

func decodeItemType(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	value := iter.ReadString()
	t, ok := stringToItemType[value]

	if !ok {
		iter.ReportError("decodeItemType", fmt.Sprintf("unknown item type %q", value))
		return
	}

	itemType := (*parser.ItemType)(ptr)
	*itemType = t
}
