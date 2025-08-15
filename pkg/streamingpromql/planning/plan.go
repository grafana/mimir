// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
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

	// NodeType returns the identifier of this node that should be used during serialization.
	NodeType() NodeType

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
	//
	// Implementations may retain the provided Materializer for later use.
	OperatorFactory(materializer *Materializer, timeRange types.QueryTimeRange, params *OperatorParameters) (OperatorFactory, error)

	// ResultType returns the kind of result this node produces.
	//
	// May return an error if the kind of result cannot be determined (eg. because the node references an unknown function).
	ResultType() (parser.ValueType, error)

	// QueriedTimeRange returns the range of data queried from ingesters and store-gateways by this node
	// and its children.
	//
	// If no data is queried by this node and its children, QueriedTimeRange.AnyDataQueried will be false.
	QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) QueriedTimeRange

	// FIXME: implementations for many of the above methods can be generated automatically
}

type QueriedTimeRange struct {
	MinT           time.Time
	MaxT           time.Time
	AnyDataQueried bool
}

func (t QueriedTimeRange) Union(other QueriedTimeRange) QueriedTimeRange {
	if !t.AnyDataQueried {
		return other
	}

	if !other.AnyDataQueried {
		return t
	}

	minT := t.MinT
	maxT := t.MaxT

	if other.MinT.Before(t.MinT) {
		minT = other.MinT
	}

	if other.MaxT.After(t.MaxT) {
		maxT = other.MaxT
	}

	return QueriedTimeRange{
		MinT:           minT,
		MaxT:           maxT,
		AnyDataQueried: true,
	}
}

type OperatorParameters struct {
	Queryable                storage.Queryable
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	LookbackDelta            time.Duration
	EagerLoadSelectors       bool
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

func (e EncodedQueryTimeRange) ToDecodedTimeRange() types.QueryTimeRange {
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
		encoded.NodeType = n.NodeType()
		var err error
		encoded.Details, err = proto.Marshal(n.Details())
		if err != nil {
			return -1, err
		}
	}

	if e.includeDescriptions {
		encoded.Type = NodeTypeName(n)
		encoded.Description = n.Describe()
		encoded.ChildrenLabels = n.ChildrenLabels()
	}

	e.nodes = append(e.nodes, encoded)
	idx := int64(len(e.nodes) - 1)
	e.nodesToIndex[n] = idx

	return idx, nil
}

// NodeTypeName returns the human-readable name of the type of n.
//
// This should not be used in performance-sensitive code.
func NodeTypeName(n Node) string {
	return reflect.TypeOf(n).Elem().Name()
}

// ToDecodedPlan converts this encoded plan to its decoded form.
// It returns references to the specified nodeIndices.
func (p *EncodedQueryPlan) ToDecodedPlan(nodeIndices ...int64) (*QueryPlan, []Node, error) {
	if p.RootNode < 0 || p.RootNode >= int64(len(p.Nodes)) {
		return nil, nil, fmt.Errorf("root node index %v out of range with %v nodes in plan", p.RootNode, len(p.Nodes))
	}

	decoder := newQueryPlanDecoder(p.Nodes)
	root, err := decoder.decodeNode(p.RootNode)

	if err != nil {
		return nil, nil, err
	}

	nodes := make([]Node, 0, len(nodeIndices))
	for _, idx := range nodeIndices {
		n, err := decoder.decodeNode(idx)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, n)
	}

	return &QueryPlan{
		TimeRange:          p.TimeRange.ToDecodedTimeRange(),
		Root:               root,
		OriginalExpression: p.OriginalExpression,
	}, nodes, nil
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

	nodeFactory, exists := knownNodeTypes[encodedNode.NodeType]
	if !exists {
		return nil, fmt.Errorf("unknown node type: %d", encodedNode.NodeType)
	}

	node := nodeFactory()
	if err := proto.Unmarshal(encodedNode.Details, node.Details()); err != nil {
		return nil, err
	}

	if err := node.SetChildren(children); err != nil {
		return nil, err
	}

	d.nodes[idx] = node

	return node, nil
}

type NodeFactory func() Node

// Map of node type to a factory method that returns a new instance of that type of node (eg. Subquery).
var knownNodeTypes = map[NodeType]NodeFactory{}

// RegisterNodeFactory registers a NodeFactory used during deserialization of a query plan.
func RegisterNodeFactory(f NodeFactory) {
	node := f()
	id := node.NodeType()

	if _, exists := knownNodeTypes[id]; exists {
		panic(fmt.Sprintf("RegisterNodeFactory already registered node type %d", id))
	}

	knownNodeTypes[id] = f
}

// String returns a human-readable representation of the query plan, intended for use during debugging and tests.
func (p *QueryPlan) String() string {
	printer := &planPrinter{
		builder:                     &strings.Builder{},
		nodeReferenceCounts:         make(map[Node]int),
		repeatedNodesPrintedAlready: make(map[Node]struct{}),
		repeatedNodeLabels:          make(map[Node]string),
	}

	printer.identifyRepeatedNodes(p.Root)
	printer.printNode(p.Root, 0, "")

	return strings.TrimRight(printer.builder.String(), "\n")
}

type planPrinter struct {
	builder                     *strings.Builder
	nodeReferenceCounts         map[Node]int
	repeatedNodesPrintedAlready map[Node]struct{}
	repeatedNodeLabels          map[Node]string
}

func (p *planPrinter) identifyRepeatedNodes(n Node) {
	if p.nodeReferenceCounts[n] > 1 {
		// We already know this node is repeated, nothing more to do.
		return
	}

	p.nodeReferenceCounts[n]++
	if p.nodeReferenceCounts[n] > 1 {
		// Just saw this node for the second time, assign a label to it and then we are done.
		p.repeatedNodeLabels[n] = fmt.Sprintf("ref#%v", len(p.repeatedNodeLabels)+1)
		return
	}

	for _, child := range n.Children() {
		p.identifyRepeatedNodes(child)
	}
}

func (p *planPrinter) printNode(n Node, indent int, label string) {
	p.builder.WriteString(strings.Repeat("\t", indent))
	p.builder.WriteString("- ")

	if label != "" {
		p.builder.WriteString(label)
		p.builder.WriteString(": ")
	}

	ref, repeated := p.repeatedNodeLabels[n]
	if repeated {
		_, printedAlready := p.repeatedNodesPrintedAlready[n]
		p.builder.WriteString(ref)
		p.builder.WriteRune(' ')

		if printedAlready {
			p.builder.WriteString(NodeTypeName(n))
			p.builder.WriteString(" ...\n")
			return
		}

		p.repeatedNodesPrintedAlready[n] = struct{}{}
	}

	p.builder.WriteString(NodeTypeName(n))

	description := n.Describe()
	if description != "" {
		p.builder.WriteString(": ")
		p.builder.WriteString(description)
	}

	p.builder.WriteRune('\n')
	childLabels := n.ChildrenLabels()

	for childIdx, child := range n.Children() {
		p.printNode(child, indent+1, childLabels[childIdx])
	}
}
