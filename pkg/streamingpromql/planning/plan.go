// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type QueryPlanVersion uint64

func (v QueryPlanVersion) String() string {
	return strconv.FormatUint(uint64(v), 10)
}

// IMPORTANT:
// Do not change the value or meaning of these constants once they have been merged.
// Doing so could result in queriers receiving query plans they don't understand, which could
// lead to errors or silently incorrect behaviour or results.

const QueryPlanVersionZero = QueryPlanVersion(0)

// QueryPlanV1 introduces:
// 1. DropName node
// 2. StepInvariantExpression node
const QueryPlanV1 = QueryPlanVersion(1)

// QueryPlanV2 introduces support for limitk and limit_ratio PromQL aggregates
const QueryPlanV2 = QueryPlanVersion(2)

// QueryPlanV3 introduces support for evaluating multiple query plan nodes in a single querier request.
const QueryPlanV3 = QueryPlanVersion(3)

// QueryPlanV4 introduces support for evaluating smoothed and anchored extended range modifiers.
const QueryPlanV4 = QueryPlanVersion(4)

// QueryPlanV5 introduces support for multi-aggregation nodes.
const QueryPlanV5 = QueryPlanVersion(5)

// QueryPlanV6 introduces support for query splitting with intermediate result caching.
const QueryPlanV6 = QueryPlanVersion(6)

// QueryPlanV7 introduces support for subset selector elimination.
const QueryPlanV7 = QueryPlanVersion(7)

var MaximumSupportedQueryPlanVersion = QueryPlanV7


type QueryPlan struct {
	Root       Node
	Parameters *QueryParameters

	// The version of this query plan.
	//
	// Queriers use this to ensure they do not attempt to execute a query plan that contains features they
	// cannot safely or correctly execute (eg. new nodes or new meaning for existing node details).
	Version QueryPlanVersion
}

type QueryParameters struct {
	OriginalExpression       string
	TimeRange                types.QueryTimeRange
	EnableDelayedNameRemoval bool
}

// Node represents a node in the query plan graph.
type Node interface {
	// Details returns the properties of this node that should be encoded during serialization.
	Details() proto.Message

	// NodeType returns the identifier of this node that should be used during serialization.
	NodeType() NodeType

	// Child returns the child at index idx.
	//
	// Children are returned in the same order as they are provided to SetChildren and ReplaceChild.
	//
	// Child panics if idx is out of range. The number of children can be determined by calling ChildCount.
	Child(idx int) Node

	// ChildCount returns the number of children of this node.
	ChildCount() int

	// SetChildren replaces the children of this node with the provided nodes.
	//
	// SetChildren will return an error if an unsupported number of children is provided.
	SetChildren(children []Node) error

	// ReplaceChild replaces the child at index idx with the provided node.
	//
	// idx is zero-based and counts in the same order as ChildrenIter and SetChildren.
	//
	// ReplaceChild will return an error if idx is out of range.
	ReplaceChild(idx int, child Node) error

	// EquivalentToIgnoringHintsAndChildren returns true if other represents the same operation as this node.
	//
	// The equivalence of child nodes should not be considered when determining equivalence
	//
	// Information such as the position of the corresponding expression in the original query string
	// or hints should not be considered when determining equivalence.
	EquivalentToIgnoringHintsAndChildren(other Node) bool

	// MergeHints merges any hints from other into this node.
	//
	// It does not apply this recursively to its children.
	//
	// Calling MergeHints with two nodes that are different types or not equivalent may result
	// in an error or undefined behavior.
	MergeHints(other Node) error

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

	// ResultType returns the kind of result this node produces.
	//
	// May return an error if the kind of result cannot be determined (eg. because the node references an unknown function).
	ResultType() (parser.ValueType, error)

	// QueriedTimeRange returns the range of data queried from ingesters and store-gateways by this node
	// and its children.
	//
	// If no data is queried by this node and its children, QueriedTimeRange.AnyDataQueried will be false.
	QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (QueriedTimeRange, error)

	// ExpressionPosition returns the position of the subexpression this node represents in the original
	// expression.
	ExpressionPosition() (posrange.PositionRange, error)

	// MinimumRequiredPlanVersion returns the minimum query plan version required to execute a plan that includes this node.
	// It does not consider the query plan version required by any of its children (for that, use planning.MinimumRequiredPlanVersion).
	MinimumRequiredPlanVersion() QueryPlanVersion

	// FIXME: implementations for many of the above methods can be generated automatically
}

// ChildrenIter returns an iterator over all children of n.
//
// Children are returned in the same order as they are provided to SetChildren and ReplaceChild.
func ChildrenIter(n Node) func(func(Node) bool) {
	return func(yield func(Node) bool) {
		count := n.ChildCount()

		for idx := range count {
			if !yield(n.Child(idx)) {
				return
			}
		}
	}
}

type QueriedTimeRange struct {
	// The earliest timestamp queried, or a zero time.Time value if AnyDataQueried is false.
	MinT time.Time

	// The latest timestamp queried, or a zero time.Time value if AnyDataQueried is false.
	MaxT time.Time

	// If false, the node does not query any data (eg. a number literal).
	AnyDataQueried bool
}

func NewQueriedTimeRange(minT time.Time, maxT time.Time) QueriedTimeRange {
	return QueriedTimeRange{
		MinT:           minT,
		MaxT:           maxT,
		AnyDataQueried: true,
	}
}

func NoDataQueried() QueriedTimeRange {
	return QueriedTimeRange{AnyDataQueried: false}
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

	return NewQueriedTimeRange(minT, maxT)
}

type OperatorParameters struct {
	Queryable                storage.Queryable
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	QueryStats               *types.QueryStats
	LookbackDelta            time.Duration
	EagerLoadSelectors       bool
	QueryParameters          *QueryParameters
	Logger                   log.Logger
}

// RangeParams describes the time range parameters for range vector selectors and subqueries.
// It includes the range duration (e.g., [5m]) and optional time modifiers (offset and @ timestamp).
type RangeParams struct {
	IsSet  bool
	Range  time.Duration
	Offset time.Duration
	// Timestamp is a non-pointer value with HasTimestamp flag to make RangeParams
	// suitable for use as a map key in OperatorFactoryKey.
	HasTimestamp bool
	Timestamp    time.Time
}

// SplitNode represents a planning node that supports range vector splitting with intermediate result caching.
// Nodes implementing this interface can be split into sub-ranges for parallel execution and caching.
type SplitNode interface {
	Node

	// IsSplittable returns true if the node can actually be split. While a node satisfying this interface can usually
	// be split, there might be some edge cases where it's not possible or not implemented yet.
	IsSplittable() bool

	// SplittingCacheKey returns a cache key for this node's intermediate results.
	SplittingCacheKey() string

	GetRangeParams() RangeParams
}

// ToEncodedPlan converts this query plan to its encoded form.
//
// If nodes is not empty:
// - only nodes reachable from the provided nodes will be encoded
// - the corresponding indices in the encoded plan for the provided nodes will be returned
// - RootNode on the returned plan will not be populated
//
// If nodes is empty:
// - all nodes reachable from the plan's root will be encoded
// - the corresponding index in the encoded plan for the root node will be returned
// - RootNode on the returned plan will be populated
func (p *QueryPlan) ToEncodedPlan(includeDescriptions bool, includeDetails bool, nodes ...Node) (*EncodedQueryPlan, []int64, error) {
	encoder := newQueryPlanEncoder(includeDescriptions, includeDetails)

	encoded := &EncodedQueryPlan{
		TimeRange:                ToEncodedTimeRange(p.Parameters.TimeRange),
		OriginalExpression:       p.Parameters.OriginalExpression,
		EnableDelayedNameRemoval: p.Parameters.EnableDelayedNameRemoval,
		Version:                  p.Version,
	}

	var encodedNodeIndices []int64

	if len(nodes) > 0 {
		encodedNodeIndices = make([]int64, 0, len(nodes))

		for _, n := range nodes {
			idx, err := encoder.encodeNode(n)
			if err != nil {
				return nil, nil, err
			}

			encodedNodeIndices = append(encodedNodeIndices, idx)
		}
	} else {
		idx, err := encoder.encodeNode(p.Root)
		if err != nil {
			return nil, nil, err
		}
		encoded.RootNode = idx
		encodedNodeIndices = []int64{idx}
	}

	encoded.Nodes = encoder.nodes

	return encoded, encodedNodeIndices, nil
}

// DeterminePlanVersion will set the plan Version to the largest MinimumRequiredPlanVersion found within the plan nodes.
func (p *QueryPlan) DeterminePlanVersion() error {
	if p.Root == nil {
		return errors.New("query plan version can not be determined without a root node")
	}
	p.Version = MinimumRequiredPlanVersion(p.Root)
	return nil
}

// MinimumRequiredPlanVersion returns the minimum required query plan version of node and all its children.
func MinimumRequiredPlanVersion(node Node) QueryPlanVersion {
	maxVersion := node.MinimumRequiredPlanVersion()
	for child := range ChildrenIter(node) {
		maxVersion = max(maxVersion, MinimumRequiredPlanVersion(child))
	}
	return maxVersion
}

func ToEncodedTimeRange(t types.QueryTimeRange) EncodedQueryTimeRange {
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
	if idx, haveWritten := e.nodesToIndex[n]; haveWritten {
		return idx, nil
	}

	encoded := &EncodedNode{}
	childCount := n.ChildCount()

	if childCount > 0 {
		childIndices := make([]int64, 0, childCount)

		// Check all children have been encoded already.
		for childIdx := range childCount {
			child := n.Child(childIdx)
			idx, err := e.encodeNode(child)
			if err != nil {
				return -1, err
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

// DecodeNodes decodes nodes for the provided nodeIndices from the encoded plan.
func (p *EncodedQueryPlan) DecodeNodes(nodeIndices ...int64) ([]Node, error) {
	if p.Version > MaximumSupportedQueryPlanVersion {
		return nil, apierror.Newf(apierror.TypeBadData, "query plan has version %v, but the maximum supported query plan version is %v", p.Version, MaximumSupportedQueryPlanVersion)
	}

	decoder := newQueryPlanDecoder(p.Nodes)

	nodes := make([]Node, 0, len(nodeIndices))
	for _, idx := range nodeIndices {
		n, err := decoder.decodeNode(idx)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	return nodes, nil
}

func (p *EncodedQueryPlan) DecodeParameters() *QueryParameters {
	return &QueryParameters{
		OriginalExpression:       p.OriginalExpression,
		TimeRange:                p.TimeRange.ToDecodedTimeRange(),
		EnableDelayedNameRemoval: p.EnableDelayedNameRemoval,
	}
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

	for child := range ChildrenIter(n) {
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

	for childIdx, label := range n.ChildrenLabels() {
		p.printNode(n.Child(childIdx), indent+1, label)
	}
}
