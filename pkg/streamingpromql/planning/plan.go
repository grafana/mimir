// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type QueryPlan struct {
	TimeRange types.QueryTimeRange `json:"timeRange"`
	Root      Node
}

// Node represents a node in the query plan graph.
type Node interface {
	// Type returns the type of this node.
	Type() string

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

	// Equals returns true if other represents the same operation as this node.
	//
	// Information such as the position of the corresponding expression in the original query string
	// should be ignored.
	Equals(other Node) bool

	// FIXME: most of the above methods can be generated automatically
}
