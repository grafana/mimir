package eventlogger

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Broker is the top-level entity used in the library for configuring the system
// and for sending events.
//
// Brokers have registered Nodes which may be composed into registered Pipelines
// for EventTypes.
//
// A Node may be a filter, formatter or sink (see NodeType).
//
// A Broker may have multiple Pipelines.
//
// EventTypes may have multiple Pipelines.
//
// A Pipeline for an EventType may contain multiple filters, one formatter and
// one sink.
//
// If a Pipeline does not have a formatter, then the event will not be written
// to the Sink.
//
// A Node can be shared across multiple pipelines.
type Broker struct {
	nodes  map[NodeID]Node
	graphs map[EventType]*graph
	lock   sync.RWMutex

	*clock
}

// NewBroker creates a new Broker.
func NewBroker() *Broker {
	return &Broker{
		nodes:  make(map[NodeID]Node),
		graphs: make(map[EventType]*graph),
	}
}

// clock only exists to make testing simpler.
type clock struct {
	now time.Time
}

// Now returns the current time
func (c *clock) Now() time.Time {
	if c == nil {
		return time.Now()
	}
	return c.now
}

// StopTimeAt allows you to "stop" the Broker's timestamp clock at a predicable
// point in time, so timestamps are predictable for testing.
func (b *Broker) StopTimeAt(now time.Time) {
	b.clock = &clock{now: now}
}

// Status describes the result of a Send.
type Status struct {
	// complete lists the IDs of sinks that successfully wrote the Event.
	complete []NodeID
	// Warnings lists any non-fatal errors that occurred while sending an Event.
	Warnings []error
}

func (s Status) getError(threshold int) error {
	if len(s.complete) < threshold {
		return fmt.Errorf("event not written to enough sinks")
	}
	return nil
}

// Send writes an event of type t to all registered pipelines concurrently and
// reports on the result.  An error will only be returned if a pipeline's delivery
// policies could not be satisfied.
func (b *Broker) Send(ctx context.Context, t EventType, payload interface{}) (Status, error) {
	b.lock.RLock()
	g, ok := b.graphs[t]
	b.lock.RUnlock()

	if !ok {
		return Status{}, fmt.Errorf("No graph for EventType %s", t)
	}

	e := &Event{
		Type:      t,
		CreatedAt: b.clock.Now(),
		Formatted: make(map[string][]byte),
		Payload:   payload,
	}

	return g.process(ctx, e)
}

// Reopen calls every registered Node's Reopen() function.  The intention is to
// ask all nodes to reopen any files they have open.  This is typically used as
// part of log rotation: after rotating, the rotator sends a signal to the
// application, which then would invoke this method.  Another typically use-case
// is to have all Nodes reevaluated any external configuration they might have.
func (b *Broker) Reopen(ctx context.Context) error {
	b.lock.RLock()
	defer b.lock.RUnlock()

	for _, g := range b.graphs {
		if err := g.reopen(ctx); err != nil {
			return err
		}
	}

	return nil
}

// NodeID is a string that uniquely identifies a Node.
type NodeID string

// RegisterNode assigns a node ID to a node.  Node IDs should be unique. A Node
// may be a filter, formatter or sink (see NodeType). Nodes can be shared across
// multiple pipelines.
func (b *Broker) RegisterNode(id NodeID, node Node) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.nodes[id] = node
	return nil
}

// PipelineID is a string that uniquely identifies a Pipeline within a given EventType.
type PipelineID string

// Pipeline defines a pipe: its ID, the EventType it's for, and the nodes
// that it contains. Nodes can be shared across multiple pipelines.
type Pipeline struct {
	// PipelineID uniquely identifies the Pipeline
	PipelineID PipelineID

	// EventType defines the type of event the Pipeline processes
	EventType EventType

	// NodeIDs defines Pipeline's the list of nodes
	NodeIDs []NodeID
}

// RegisterPipeline adds a pipeline to the broker.
func (b *Broker) RegisterPipeline(def Pipeline) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	g, ok := b.graphs[def.EventType]
	if !ok {
		g = &graph{}
		b.graphs[def.EventType] = g
	}

	nodes := make([]Node, len(def.NodeIDs))
	for i, n := range def.NodeIDs {
		node, ok := b.nodes[n]
		if !ok {
			return fmt.Errorf("nodeID %q not registered", n)
		}
		nodes[i] = node
	}
	root, err := linkNodes(nodes, def.NodeIDs)
	if err != nil {
		return err
	}

	err = g.doValidate(nil, root)
	if err != nil {
		return err
	}

	g.roots.Store(def.PipelineID, root)

	return nil
}

// RemovePipeline removes a pipeline from the broker.
func (b *Broker) RemovePipeline(t EventType, id PipelineID) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	g, ok := b.graphs[t]
	if !ok {
		return fmt.Errorf("No graph for EventType %s", t)
	}

	g.roots.Delete(id)
	return nil
}

// SetSuccessThreshold sets the success threshold per eventType.  For the
// overall processing of a given event to be considered a success, at least as
// many sinks as the threshold value must successfully process the event.
func (b *Broker) SetSuccessThreshold(t EventType, successThreshold int) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if successThreshold < 0 {
		return fmt.Errorf("successThreshold must be 0 or greater")
	}

	g, ok := b.graphs[t]
	if !ok {
		g = &graph{}
		b.graphs[t] = g
	}

	g.successThreshold = successThreshold
	return nil
}
