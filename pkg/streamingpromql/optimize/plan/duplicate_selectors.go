// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
)

type DuplicateSelectors struct{}

func (d *DuplicateSelectors) Name() string {
	return "Duplicate selectors"
}

func (d *DuplicateSelectors) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	// TODO

	return plan, nil
}

type Duplicate struct {
	Inner planning.Node
}

func (d *Duplicate) Type() string {
	return "Duplicate"
}

func (d *Duplicate) Children() []planning.Node {
	return []planning.Node{d.Inner}
}

func (d *Duplicate) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Duplicate supports 1 child, but got %d", len(children))
	}

	d.Inner = children[0]

	return nil
}

func (d *Duplicate) Equals(other planning.Node) bool {
	otherDuplicate, ok := other.(*Duplicate)

	return ok && d.Inner.Equals(otherDuplicate.Inner)
}
