// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"reflect"
	"time"

	"github.com/gogo/protobuf/proto"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type QueryPlan struct {
	TimeRange types.QueryTimeRange
	Root      Node

	OriginalExpression string
}

// Node represents a node in the query plan graph.
type Node interface {
	// Details returns the properties of this node that should be encoded during serialization.
	Details() proto.Message

	// Children returns a slice of all children of this node, if any.
	//
	// Modifying the returned slice has no effect, however, modifying the elements of the returned slice
	// modifies the corresponding child of this node.
	//
	// eg. Children()[0] = nil has no effect
	//
	// eg. Children()[0].DoStuff = true modifies the first child of this node
	Children() []Node

	// SetChildren replaces the children of this node with the provided nodes.
	//
	// SetChildren will return an error if an unsupported number of children is provided.
	//
	// Calling SetChildren(Children()) is a no-op.
	SetChildren(children []Node) error

	// EquivalentTo returns true if other represents the same operation as this node.
	//
	// Information such as the position of the corresponding expression in the original query string
	// should be ignored.
	EquivalentTo(other Node) bool

	// Describe returns a human-readable representation of this node.
	//
	// Returning an empty string is valid.
	Describe() string

	// ChildrenLabels returns human-readable labels for the children of this node.
	// The number of labels returned must match the number of children returned by Children.
	// Each label must be unique.
	//
	// For example, a binary expression would return "LHS" and "RHS".
	//
	// Returning an empty string for a label is valid.
	ChildrenLabels() []string

	// ChildrenTimeRange returns the time range used by children of this node.
	//
	// Most nodes will return timeRange as is, with the exception of subqueries.
	ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange

	// OperatorFactory returns a factory that produces operators for this node.
	OperatorFactory(children []types.Operator, timeRange types.QueryTimeRange, params *OperatorParameters) (OperatorFactory, error)

	// ResultType returns the kind of result this node produces.
	//
	// May return an error if the kind of result cannot be determined (eg. because the node references an unknown function).
	ResultType() (parser.ValueType, error)

	// FIXME: implementations for many of the above methods can be generated automatically
}

type OperatorParameters struct {
	Queryable                storage.Queryable
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	Stats                    *types.QueryStats
	LookbackDelta            time.Duration
}

func (p *QueryPlan) ToEncodedPlan(includeDescriptions bool, includeDetails bool) (*EncodedQueryPlan, error) {
	encoder := newQueryPlanEncoder(includeDescriptions, includeDetails)
	rootNode, err := encoder.encodeNode(p.Root)
	if err != nil {
		return nil, err
	}

	encoded := &EncodedQueryPlan{
		TimeRange:          toEncodedTimeRange(p.TimeRange),
		Nodes:              encoder.nodes,
		RootNode:           rootNode,
		OriginalExpression: p.OriginalExpression,
	}

	return encoded, nil
}

func toEncodedTimeRange(t types.QueryTimeRange) EncodedQueryTimeRange {
	return EncodedQueryTimeRange{
		StartT:               t.StartT,
		EndT:                 t.EndT,
		IntervalMilliseconds: t.IntervalMilliseconds,
		IsInstant:            t.IsInstant,
	}
}

func fromEncodedTimeRange(e EncodedQueryTimeRange) types.QueryTimeRange {
	if e.IsInstant {
		return types.NewInstantQueryTimeRange(timestamp.Time(e.StartT))
	}

	return types.NewRangeQueryTimeRange(timestamp.Time(e.StartT), timestamp.Time(e.EndT), time.Duration(e.IntervalMilliseconds)*time.Millisecond)
}

type queryPlanEncoder struct {
	nodes               []*EncodedNode
	nodesToIndex        map[Node]int64
	includeDescriptions bool // Include descriptions of nodes and their children, for display to a human
	includeDetails      bool // Include details of nodes, for reconstruction in another process
}

func newQueryPlanEncoder(includeDescriptions bool, includeDetails bool) *queryPlanEncoder {
	return &queryPlanEncoder{
		nodesToIndex:        make(map[Node]int64),
		includeDescriptions: includeDescriptions,
		includeDetails:      includeDetails,
	}
}

func (e *queryPlanEncoder) encodeNode(n Node) (int64, error) {
	encoded := &EncodedNode{}
	children := n.Children()

	if len(children) > 0 {
		childIndices := make([]int64, 0, len(children))

		// Check all children have been encoded already.
		for _, child := range children {
			idx, haveWritten := e.nodesToIndex[child]

			if !haveWritten {
				var err error
				idx, err = e.encodeNode(child)

				if err != nil {
					return -1, err
				}
			}

			childIndices = append(childIndices, idx)
		}

		encoded.Children = childIndices
	}

	if e.includeDetails {
		var err error
		encoded.Details, err = prototypes.MarshalAny(n.Details())
		if err != nil {
			return -1, err
		}
	}

	if e.includeDescriptions {
		encoded.Type = reflect.TypeOf(n).Elem().Name()
		encoded.Description = n.Describe()
		encoded.ChildrenLabels = n.ChildrenLabels()
	}

	e.nodes = append(e.nodes, encoded)
	idx := int64(len(e.nodes) - 1)
	e.nodesToIndex[n] = idx

	return idx, nil
}

func (p *EncodedQueryPlan) ToDecodedPlan() (*QueryPlan, error) {
	if p.RootNode < 0 || p.RootNode >= int64(len(p.Nodes)) {
		return nil, fmt.Errorf("root node index %v out of range with %v nodes in plan", p.RootNode, len(p.Nodes))
	}

	decoder := newQueryPlanDecoder(p.Nodes)
	root, err := decoder.decodeNode(p.RootNode)

	if err != nil {
		return nil, err
	}

	return &QueryPlan{
		TimeRange:          fromEncodedTimeRange(p.TimeRange),
		Root:               root,
		OriginalExpression: p.OriginalExpression,
	}, nil
}

type queryPlanDecoder struct {
	encodedNodes []*EncodedNode
	nodes        []Node
}

func newQueryPlanDecoder(encodedNodes []*EncodedNode) *queryPlanDecoder {
	return &queryPlanDecoder{
		encodedNodes: encodedNodes,
		nodes:        make([]Node, len(encodedNodes)),
	}
}

func (d *queryPlanDecoder) decodeNode(idx int64) (Node, error) {
	if idx < 0 || idx >= int64(len(d.nodes)) {
		return nil, fmt.Errorf("node index %v out of range with %v nodes in plan", idx, len(d.nodes))
	}

	if d.nodes[idx] != nil {
		return d.nodes[idx], nil
	}

	encodedNode := d.encodedNodes[idx]
	children := make([]Node, 0, len(encodedNode.Children))
	for _, childIdx := range encodedNode.Children {
		child, err := d.decodeNode(childIdx)
		if err != nil {
			return nil, err
		}

		children = append(children, child)
	}

	if encodedNode.Details == nil {
		return nil, fmt.Errorf("node %v has no details (was the encoded query plan created with includeDetails = false?)", idx)
	}

	name, err := prototypes.AnyMessageName(encodedNode.Details)
	if err != nil {
		return nil, err
	}

	nodeFactory, exists := knownNodeTypes[name]
	if !exists {
		return nil, fmt.Errorf("unknown node type: %s", name)
	}

	node := nodeFactory()
	if err := prototypes.UnmarshalAny(encodedNode.Details, node.Details()); err != nil {
		return nil, err
	}

	if err := node.SetChildren(children); err != nil {
		return nil, err
	}

	d.nodes[idx] = node

	return node, nil
}

type NodeFactory func() Node

// Map of details message type (eg. "planning.SubqueryDetails") to a factory method that returns a new instance of that type of node (eg. Subquery).
var knownNodeTypes = map[string]NodeFactory{}

// RegisterNodeFactory registers a NodeFactory used during deserialization of a query plan.
func RegisterNodeFactory(f NodeFactory) {
	n := f()
	details := n.Details()
	if details == nil {
		panic("RegisterNodeFactory called with factory that returns node with nil Details()")
	}

	name := proto.MessageName(details)

	if name == "" {
		// If you're seeing the message below, then you likely need to enable the gogoproto.messagename_all option for the .proto file.
		panic("RegisterNodeFactory called with details type that returns empty name - is the type missing a XXX_MessageName method?")
	}

	if _, exists := knownNodeTypes[name]; exists {
		panic(fmt.Sprintf("RegisterNodeFactory already registered name %v", name))
	}

	knownNodeTypes[name] = f
}
