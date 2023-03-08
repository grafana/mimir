package eventlogger

import (
	"context"
	"fmt"
)

// NodeType defines the possible Node type's in the system.
type NodeType int

const (
	_ NodeType = iota
	NodeTypeFilter
	NodeTypeFormatter
	NodeTypeSink
	NodeTypeFormatterFilter // A node that formats and then filters the events based on the new format.
)

// A Node in a graph
type Node interface {
	// Process does something with the Event: filter, redaction,
	// marshalling, persisting.
	Process(ctx context.Context, e *Event) (*Event, error)
	// Reopen is used to re-read any config stored externally
	// and to close and reopen files, e.g. for log rotation.
	Reopen() error
	// Type describes the type of the node.  This is mostly just used to
	// validate that pipelines are sensibly arranged, e.g. ending with a sink.
	Type() NodeType
}

type linkedNode struct {
	node   Node
	nodeID NodeID
	next   []*linkedNode
}

// linkNodes is a convenience function that connects Nodes together into a
// linked list.
func linkNodes(nodes []Node, ids []NodeID) (*linkedNode, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes given")
	}

	root := &linkedNode{node: nodes[0]}
	cur := root

	for _, n := range nodes[1:] {
		next := &linkedNode{node: n}
		cur.next = []*linkedNode{next}
		cur = next
	}

	return root, nil
}

// linkNodesAndSinks is a convenience function that connects
// the inner Nodes together into a linked list.  Then it appends the sinks
// to the end as a set of fan-out leaves.
func linkNodesAndSinks(inner, sinks []Node, nodeIDs, sinkIDs []NodeID) (*linkedNode, error) {
	root, err := linkNodes(inner, nodeIDs)
	if err != nil {
		return nil, err
	}

	// This is inefficient but since it's only used in setup we don't care:
	cur := root
	for cur.next != nil {
		cur = cur.next[0]
	}

	for i, s := range sinks {
		cur.next = append(cur.next, &linkedNode{node: s, nodeID: sinkIDs[i]})
	}

	return root, nil
}
