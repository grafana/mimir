package eventlogger

import (
	"context"
	"fmt"
	"sync"

	"github.com/hashicorp/go-multierror"
)

// graph
type graph struct {

	// roots maps PipelineIDs to root Nodes
	roots graphMap

	// successThreshold specifies how many sinks must store an event for Process
	// to not return an error.
	successThreshold int
}

// Process the Event by routing it through all of the graph's nodes,
// starting with the root node.
func (g *graph) process(ctx context.Context, e *Event) (Status, error) {
	statusChan := make(chan Status)
	var wg sync.WaitGroup
	go func() {
		g.roots.Range(func(_ PipelineID, root *linkedNode) bool {
			wg.Add(1)
			g.doProcess(ctx, root, e, statusChan, &wg)
			return true
		})
		wg.Wait()
		close(statusChan)
	}()
	var status Status
	var done bool
	for !done {
		select {
		case <-ctx.Done():
			done = true
		case s, ok := <-statusChan:
			if ok {
				status.Warnings = append(status.Warnings, s.Warnings...)
				status.complete = append(status.complete, s.complete...)
			} else {
				done = true
			}
		}
	}
	return status, status.getError(g.successThreshold)
}

// Recursively process every node in the graph.
func (g *graph) doProcess(ctx context.Context, node *linkedNode, e *Event, statusChan chan Status, wg *sync.WaitGroup) {
	defer wg.Done()

	// Process the current Node
	e, err := node.node.Process(ctx, e)
	if err != nil {
		select {
		case <-ctx.Done():
		case statusChan <- Status{Warnings: []error{err}}:
		}
		return
	}
	// If the Event is nil, it has been filtered out and we are done.
	if e == nil {
		select {
		case <-ctx.Done():
		case statusChan <- Status{complete: []NodeID{node.nodeID}}:
		}
		return
	}

	// Process any child nodes.  This is depth-first.
	if len(node.next) != 0 {
		// If the new Event is nil, it has been filtered out and we are done.
		if e == nil {
			statusChan <- Status{}
			return
		}

		for _, child := range node.next {
			wg.Add(1)
			go g.doProcess(ctx, child, e, statusChan, wg)
		}
	} else {
		select {
		case <-ctx.Done():
		case statusChan <- Status{complete: []NodeID{node.nodeID}}:
		}
	}
}

func (g *graph) reopen(ctx context.Context) error {
	var errors *multierror.Error

	g.roots.Range(func(_ PipelineID, root *linkedNode) bool {
		err := g.doReopen(ctx, root)
		if err != nil {
			errors = multierror.Append(errors, err)
		}
		return true
	})

	return errors.ErrorOrNil()
}

// Recursively reopen every node in the graph.
func (g *graph) doReopen(ctx context.Context, node *linkedNode) error {
	// Process the current Node
	err := node.node.Reopen()
	if err != nil {
		return err
	}

	// Process any child nodes.  This is depth-first.
	for _, child := range node.next {

		err = g.doReopen(ctx, child)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *graph) validate() error {
	var errors *multierror.Error

	g.roots.Range(func(_ PipelineID, root *linkedNode) bool {
		err := g.doValidate(nil, root)
		if err != nil {
			errors = multierror.Append(errors, err)
		}
		return true
	})

	return errors.ErrorOrNil()
}

func (g *graph) doValidate(parent, node *linkedNode) error {
	isInner := len(node.next) > 0

	switch {
	case len(node.next) == 0 && node.node.Type() != NodeTypeSink:
		return fmt.Errorf("non-sink node has no children")
	case !isInner && parent == nil:
		return fmt.Errorf("sink node at root")
	case !isInner && (parent.node.Type() != NodeTypeFormatter && parent.node.Type() != NodeTypeFormatterFilter):
		return fmt.Errorf("sink node without preceding formatter or formatter filter")
	case !isInner:
		return nil
	}

	// Process any child nodes.  This is depth-first.
	for _, child := range node.next {
		err := g.doValidate(node, child)
		if err != nil {
			return err
		}
	}

	return nil
}
