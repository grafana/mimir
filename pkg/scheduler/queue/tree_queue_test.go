// SPDX-License-Identifier: AGPL-3.0-only

package queue

import (
	"fmt"
	"strings"

	//"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func newTenantQuerierAssignments() *tenantQuerierAssignments {
	return &tenantQuerierAssignments{
		tenantQuerierIDs: make(map[TenantID]map[QuerierID]struct{}),
		tenantNodes:      make(map[string][]*Node),
	}
}

func Test_NewTree(t *testing.T) {
	tests := []struct {
		name      string
		treeAlgos []DequeueAlgorithm
		state     *tenantQuerierAssignments
		expectErr bool
	}{
		{
			name:      "create round-robin tree",
			treeAlgos: []DequeueAlgorithm{&roundRobinState{}},
		},
		{
			name:      "create shuffle-shard tree",
			treeAlgos: []DequeueAlgorithm{newTenantQuerierAssignments()},
		},
		{
			name:      "create shuffle-shard tree with no tenant-querier map, we should create an empty one",
			treeAlgos: []DequeueAlgorithm{&tenantQuerierAssignments{}},
		},
		{
			name:      "fail to create tree without defined dequeuing algorithm",
			treeAlgos: []DequeueAlgorithm{nil},
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTree(tt.treeAlgos...)
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

		})
	}
}

func Test_EnqueueBackByPath(t *testing.T) {
	tests := []struct {
		name             string
		treeAlgosByDepth []DequeueAlgorithm
		children         []QueuePath
		expectErr        bool
	}{
		{
			name:             "enqueue round-robin node to round-robin node",
			treeAlgosByDepth: []DequeueAlgorithm{&roundRobinState{}, &roundRobinState{}},
			children:         []QueuePath{{"round-robin-child-1"}},
		},
		{
			name:             "enqueue shuffle-shard node to round-robin node",
			treeAlgosByDepth: []DequeueAlgorithm{&roundRobinState{}, &tenantQuerierAssignments{tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{}}},
			children:         []QueuePath{{"shuffle-shard-child-1"}},
		},
		{
			name:             "enqueue shuffle-shard node with no tenant-querier map to round-robin node, we should create an empty one",
			treeAlgosByDepth: []DequeueAlgorithm{&roundRobinState{}, &tenantQuerierAssignments{}},
			children:         []QueuePath{{"shuffle-shard-child-1"}},
		},
		{
			name: "enqueue round-robin node to shuffle-shard node",
			treeAlgosByDepth: []DequeueAlgorithm{
				newTenantQuerierAssignments(),
				&roundRobinState{},
			},
			children: []QueuePath{{"round-robin-child-1"}},
		},
		{
			name: "create tree with multiple shuffle-shard depths",
			treeAlgosByDepth: []DequeueAlgorithm{
				newTenantQuerierAssignments(),
				&roundRobinState{},
				newTenantQuerierAssignments(),
			},
			children: []QueuePath{{"child"}, {"grandchild"}},
		},
		{
			name:             "enqueue beyond max-depth",
			treeAlgosByDepth: []DequeueAlgorithm{&roundRobinState{}},
			children:         []QueuePath{{"child"}, {"child, grandchild"}},
			expectErr:        true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(tt.treeAlgosByDepth...)
			require.NoError(t, err)

			for _, childPath := range tt.children {
				err = tree.EnqueueBackByPath(childPath, "some-object")
			}
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_EnqueueFrontByPath(t *testing.T) {
	type enqueueObj struct {
		obj  any
		path QueuePath
	}
	someQuerier := QuerierID("placeholder")
	tests := []struct {
		name             string
		treeAlgosByDepth []DequeueAlgorithm
		enqueueObjs      []enqueueObj
		expected         []any
	}{
		{
			name:             "enqueue to front of round-robin node",
			treeAlgosByDepth: []DequeueAlgorithm{&roundRobinState{}},
			enqueueObjs: []enqueueObj{
				{"query-1", QueuePath{}},
				{"query-2", QueuePath{}},
			},
			expected: []any{"query-2", "query-1"},
		},
		{
			name: "enqueue to front of shuffle-shard node",
			treeAlgosByDepth: []DequeueAlgorithm{&tenantQuerierAssignments{
				currentQuerier: &someQuerier,
			}},
			enqueueObjs: []enqueueObj{
				{"query-1", QueuePath{}},
				{"query-2", QueuePath{}},
			},
			expected: []any{"query-2", "query-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(tt.treeAlgosByDepth...)
			require.NoError(t, err)

			for _, o := range tt.enqueueObjs {
				err = tree.EnqueueFrontByPath(o.path, o.obj)
			}
			require.NoError(t, err)

			for _, expectedVal := range tt.expected {
				_, v := tree.Dequeue()
				require.Equal(t, expectedVal, v)
			}
			_, v := tree.Dequeue()
			require.Nil(t, v)
		})
	}
}

func Test_Dequeue_RootNode(t *testing.T) {
	tests := []struct {
		name          string
		rootAlgo      DequeueAlgorithm
		enqueueToRoot []any
	}{
		{
			name:     "dequeue from empty round-robin root node",
			rootAlgo: &roundRobinState{},
		},
		{
			name:     "dequeue from empty shuffle-shard root node",
			rootAlgo: newTenantQuerierAssignments(),
		},
		{
			name:          "dequeue from non-empty round-robin root node",
			rootAlgo:      &roundRobinState{},
			enqueueToRoot: []any{"something-in-root"},
		},
		{
			name:          "dequeue from non-empty shuffle-shard root node",
			rootAlgo:      newTenantQuerierAssignments(),
			enqueueToRoot: []any{"something-else-in-root"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			querierID := QuerierID("placeholder")
			switch tt.rootAlgo.(type) {
			case *tenantQuerierAssignments:
				tt.rootAlgo.(*tenantQuerierAssignments).currentQuerier = &querierID
			}
			tree, err := NewTree(tt.rootAlgo)
			require.NoError(t, err)

			path := QueuePath{}
			for _, elt := range tt.enqueueToRoot {
				err = tree.EnqueueBackByPath(path, elt)
				require.NoError(t, err)
			}

			for _, elt := range tt.enqueueToRoot {
				dequeuePath, v := tree.Dequeue()
				require.Equal(t, path, dequeuePath)
				require.Equal(t, elt, v)

			}

			dequeuePath, v := tree.Dequeue()
			require.Equal(t, path, dequeuePath)
			require.Nil(t, v)

		})
	}
}

func Test_RoundRobinDequeue(t *testing.T) {
	tests := []struct {
		name                      string
		selfQueueObjects          []string
		children                  []string
		childQueueObjects         map[string][]any
		grandchildren             []string
		grandchildrenQueueObjects map[string]struct {
			path QueuePath
			objs []any
		}
		expected []string
	}{
		{
			name:              "dequeue from round-robin child when local queue empty",
			children:          []string{"child-1"},
			childQueueObjects: map[string][]any{"child-1": {"child-1:some-object"}},
			expected:          []string{"child-1:some-object"},
		},
		{
			name:              "dequeue from round-robin root when on node's turn",
			selfQueueObjects:  []string{"root:object-1", "root:object-2"},
			children:          []string{"child-1"},
			childQueueObjects: map[string][]any{"child-1": {"child-1:object-1"}},
			expected:          []string{"root:object-1", "child-1:object-1"},
		},
		{
			name:              "dequeue from second round-robin child when first child is empty",
			children:          []string{"child-1", "child-2"},
			childQueueObjects: map[string][]any{"child-1": {nil}, "child-2": {"child-2:some-object"}},
			expected:          []string{"child-2:some-object"},
		},
		{
			name:              "dequeue from round-robin grandchild when non-empty",
			children:          []string{"child-1", "child-2"},
			childQueueObjects: map[string][]any{"child-1": {"child-1:object-1", "child-1:object-2"}, "child-2": {"child-2:object-1"}},
			expected:          []string{"child-1:object-1", "child-2:object-1", "grandchild-1:object-1"},
			grandchildren:     []string{"grandchild-1"},
			grandchildrenQueueObjects: map[string]struct {
				path QueuePath
				objs []any
			}{"grandchild-1": {
				path: QueuePath{"child-1", "grandchild-1"},
				objs: []any{"grandchild-1:object-1"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(&roundRobinState{}, &roundRobinState{}, &roundRobinState{})
			require.NoError(t, err)

			for _, sqo := range tt.selfQueueObjects {
				err = tree.EnqueueBackByPath(QueuePath{}, sqo)
				require.NoError(t, err)
			}

			for _, child := range tt.children {
				for _, obj := range tt.childQueueObjects[child] {
					_ = tree.EnqueueBackByPath(QueuePath{child}, obj)
				}
			}

			for _, grandchild := range tt.grandchildren {
				gqo := tt.grandchildrenQueueObjects[grandchild]
				for _, obj := range gqo.objs {
					err = tree.EnqueueBackByPath(gqo.path, obj)
					require.NoError(t, err)
				}
			}

			for _, expected := range tt.expected {
				_, val := tree.Dequeue()
				v, ok := val.(string)
				require.True(t, ok)
				require.Equal(t, expected, v)
			}
		})
	}
}

func Test_DequeueOrderAfterEnqueue(t *testing.T) {
	type opType string
	enqueue := opType("enqueue")
	dequeue := opType("dequeue")
	placeholderQuerier := QuerierID("")

	type op struct {
		kind opType
		path QueuePath
		obj  any
	}

	tests := []struct {
		name             string
		treeAlgosByDepth []DequeueAlgorithm
		operationOrder   []op
	}{
		{
			name:             "round-robin node should dequeue from first child one more time after new node added",
			treeAlgosByDepth: []DequeueAlgorithm{&roundRobinState{}, &roundRobinState{}},
			operationOrder: []op{
				{enqueue, QueuePath{"child-1"}, "obj-1"},
				{enqueue, QueuePath{"child-1"}, "obj-2"},
				{dequeue, QueuePath{"child-1"}, "obj-1"},
				{enqueue, QueuePath{"child-2"}, "obj-3"},
				{dequeue, QueuePath{"child-1"}, "obj-2"},
				{dequeue, QueuePath{"child-2"}, "obj-3"},
				{dequeue, QueuePath{}, nil},
			},
		},
		{
			name: "should dequeue from new (next-in-queue) shuffle-shard child immediately after it is added",
			treeAlgosByDepth: []DequeueAlgorithm{
				&tenantQuerierAssignments{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{},
					tenantNodes:      map[string][]*Node{},
					currentQuerier:   &placeholderQuerier,
				},
				&tenantQuerierAssignments{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{},
					tenantNodes:      map[string][]*Node{},
					currentQuerier:   &placeholderQuerier,
				},
			},
			operationOrder: []op{
				{enqueue, QueuePath{"child-1"}, "obj-1"},
				{enqueue, QueuePath{"child-1"}, "obj-2"},
				{dequeue, QueuePath{"child-1"}, "obj-1"},
				{enqueue, QueuePath{"child-2"}, "obj-3"},
				{dequeue, QueuePath{"child-2"}, "obj-3"},
				{dequeue, QueuePath{"child-1"}, "obj-2"},
				{dequeue, QueuePath{}, nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(tt.treeAlgosByDepth...)
			require.NoError(t, err)

			for _, operation := range tt.operationOrder {
				fmt.Println(operation)
				if operation.kind == enqueue {
					err = tree.EnqueueBackByPath(operation.path, operation.obj)
					require.NoError(t, err)
				}
				if operation.kind == dequeue {
					path, obj := tree.Dequeue()
					require.Equal(t, operation.path, path)
					require.Equal(t, operation.obj, obj)
				}
			}
		})
	}
}

func Test_TenantQuerierAssignmentsDequeue(t *testing.T) {
	type enqueueObj struct {
		obj  any
		path QueuePath
	}

	tests := []struct {
		name             string
		treeAlgosByDepth []DequeueAlgorithm
		state            *tenantQuerierAssignments
		currQuerier      []QuerierID
		enqueueObjs      []enqueueObj
		expected         []any
		expectErr        bool
	}{
		{
			name: "happy path - tenant found in tenant-querier map under first child",
			treeAlgosByDepth: []DequeueAlgorithm{
				&roundRobinState{},
				newTenantQuerierAssignments(),
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
				tenantNodes:      map[string][]*Node{},
			},
			currQuerier: []QuerierID{"querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
			},
			expected: []any{"query-1"},
		},
		{
			name: "tenant exists, but not for querier",
			treeAlgosByDepth: []DequeueAlgorithm{
				&roundRobinState{},
				newTenantQuerierAssignments(),
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}},
				tenantNodes:      map[string][]*Node{},
			},
			currQuerier: []QuerierID{"querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
			},
			expected: []any{nil},
		},
		{
			name: "1 of 3 tenants exist for querier",
			treeAlgosByDepth: []DequeueAlgorithm{
				&roundRobinState{},
				newTenantQuerierAssignments(),
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
			},
			currQuerier: []QuerierID{"querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
				{obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}},
				{obj: "query-3", path: QueuePath{"query-component-1", "tenant-3"}},
			},
			expected: []any{"query-3", nil},
		},
		{
			name: "tenant exists for querier on next parent node",
			treeAlgosByDepth: []DequeueAlgorithm{
				&roundRobinState{},
				newTenantQuerierAssignments(),
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
			},
			currQuerier: []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
				{obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}},
				{obj: "query-3", path: QueuePath{"query-component-2", "tenant-3"}},
				{obj: "query-4", path: QueuePath{"query-component-1", "tenant-3"}},
			},
			expected: []any{"query-4", "query-3", nil},
		},
		{
			name: "2 of 3 tenants exist for querier",
			treeAlgosByDepth: []DequeueAlgorithm{
				&roundRobinState{},
				newTenantQuerierAssignments(),
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
				tenantNodes:      map[string][]*Node{},
			},
			currQuerier: []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
				{obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}},
				{obj: "query-3", path: QueuePath{"query-component-2", "tenant-3"}},
			},
			expected: []any{"query-1", "query-3", nil},
		},
		{
			name: "root node is shuffle-shard node",
			treeAlgosByDepth: []DequeueAlgorithm{
				&tenantQuerierAssignments{tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{}},
				&roundRobinState{},
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": {}},
				tenantNodes:      map[string][]*Node{},
			},
			currQuerier: []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-2", "query-component-1"}},
				{obj: "query-2", path: QueuePath{"tenant-1", "query-component-2"}},
				{obj: "query-3", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{"query-2", "query-3", nil},
		},
		{
			name: "dequeuing for one querier returns nil, but does return for a different querier",
			treeAlgosByDepth: []DequeueAlgorithm{
				&tenantQuerierAssignments{tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{}},
				&roundRobinState{},
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
				tenantNodes:      map[string][]*Node{},
			},
			currQuerier: []QuerierID{"querier-2", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{nil, "query-1"},
		},
		{
			name: "no querier set in state",
			treeAlgosByDepth: []DequeueAlgorithm{
				&tenantQuerierAssignments{tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{}},
				&tenantQuerierAssignments{tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{}},
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
				tenantNodes:      map[string][]*Node{},
			},
			currQuerier: []QuerierID{""},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{nil},
		},
		{
			// This also dequeues if the tenant _is not_ in the tenant querier map; is this expected? (probably)
			name: "dequeue from a tenant with a nil tenant-querier map",
			treeAlgosByDepth: []DequeueAlgorithm{
				&tenantQuerierAssignments{tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{}},
				&roundRobinState{},
				&roundRobinState{},
			},
			state: &tenantQuerierAssignments{
				tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": nil},
				tenantNodes:      map[string][]*Node{},
			},
			currQuerier: []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-1", "query-component-1"}},
				{obj: "query-2", path: QueuePath{"tenant-2", "query-component-1"}},
				{obj: "query-3", path: QueuePath{"tenant-3", "query-component-1"}},
			},
			expected: []any{"query-1", "query-2", "query-3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentQuerier := QuerierID("placeholder")
			tt.state.currentQuerier = &currentQuerier

			// We need a reference to state in order to be able to
			// update the state's currentQuerier.
			for i, da := range tt.treeAlgosByDepth {
				switch da.(type) {
				case *tenantQuerierAssignments:
					tqas := tt.treeAlgosByDepth[i].(*tenantQuerierAssignments)
					tqas.tenantQuerierIDs = tt.state.tenantQuerierIDs
					tqas.currentQuerier = tt.state.currentQuerier
				}
			}

			tree, err := NewTree(tt.treeAlgosByDepth...)
			require.NoError(t, err)

			for _, o := range tt.enqueueObjs {
				err = tree.EnqueueBackByPath(o.path, o.obj)
				require.NoError(t, err)
			}
			// currQuerier at position i is used to dequeue the expected result at position i
			require.Equal(t, len(tt.currQuerier), len(tt.expected))
			for i := 0; i < len(tt.expected); i++ {
				currentQuerier = tt.currQuerier[i]
				_, v := tree.Dequeue()
				require.Equal(t, tt.expected[i], v)
			}
		})
	}

}

// Test_ChangeTenantQuerierAssignments illustrates that we can update a state in tenantQuerierAssignments,
// and the tree dequeue behavior will adjust accordingly.
func Test_ChangeTenantQuerierAssignments(t *testing.T) {
	tqa := tenantQuerierAssignments{
		tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{
			"tenant-1": {"querier-1": {}},
			"tenant-2": {"querier-2": {}},
		},
	}

	state := &tenantQuerierAssignments{
		tenantQuerierIDs: tqa.tenantQuerierIDs,
		currentQuerier:   nil,
	}

	tree, err := NewTree(state, &roundRobinState{}, &roundRobinState{})
	require.NoError(t, err)

	err = tree.EnqueueBackByPath(QueuePath{"tenant-1", "query-component-1"}, "query-1")
	err = tree.EnqueueBackByPath(QueuePath{"tenant-2", "query-component-1"}, "query-2")
	err = tree.EnqueueBackByPath(QueuePath{"tenant-2", "query-component-1"}, "query-3")
	err = tree.EnqueueBackByPath(QueuePath{"tenant-2", "query-component-1"}, "query-4")
	err = tree.EnqueueBackByPath(QueuePath{"tenant-3", "query-component-1"}, "query-5")
	require.NoError(t, err)

	querier1 := QuerierID("querier-1")
	querier2 := QuerierID("querier-2")
	querier3 := QuerierID("querier-3")

	// set state to querier-2 should dequeue query-2
	state.currentQuerier = &querier2
	state.tenantOrderIndex = -1
	_, v := tree.Dequeue()
	require.Equal(t, "query-2", v)

	// update state to querier-1 should dequeue query-1
	state.currentQuerier = &querier1
	state.tenantOrderIndex = -1
	_, v = tree.Dequeue()
	require.Equal(t, "query-1", v)

	// update tqa map to add querier-3 as assigned to tenant-2, then set state to querier-3 should dequeue query-3
	tqa.tenantQuerierIDs["tenant-2"]["querier-3"] = struct{}{}
	state.currentQuerier = &querier3
	state.tenantOrderIndex = -1
	_, v = tree.Dequeue()
	require.Equal(t, "query-3", v)

	// during reshuffle, we only ever reassign tenant values, we don't assign an entirely new map value
	// to tenantQuerierIDs. Reassign tenant-2 to an empty map value, and query-5 (tenant-3), which can be handled
	// by any querier, should be dequeued,
	tqa.tenantQuerierIDs["tenant-2"] = map[QuerierID]struct{}{}
	_, v = tree.Dequeue()
	require.Equal(t, "query-5", v)

	// then we should not be able to dequeue query-4
	tqa.tenantQuerierIDs["tenant-2"] = map[QuerierID]struct{}{}
	_, v = tree.Dequeue()
	require.Nil(t, v)

}

// Test_DequeueBalancedRoundRobinTree checks dequeuing behavior from a balanced round-robin tree.
//
// Dequeuing from a balanced tree allows the test to have a simple looped structures
// while running checks to ensure that round-robin order is respected.
func Test_DequeueBalancedRoundRobinTree(t *testing.T) {
	firstDimensions := []string{"0", "1", "2"}
	secondDimensions := []string{"a", "b", "c"}
	itemsPerDimension := 5
	tree := makeBalancedRoundRobinTree(t, firstDimensions, secondDimensions, itemsPerDimension)
	require.NotNil(t, tree)

	count := 0

	// TreeQueue will fairly dequeue from all levels of the tree
	rotationsBeforeRepeat := len(firstDimensions) * len(secondDimensions)
	// track dequeued paths to ensure round-robin dequeuing does not repeat before expected
	dequeuedPathCache := make([]QueuePath, rotationsBeforeRepeat)

	for !tree.IsEmpty() {
		dequeuedPath, _ := tree.Dequeue()

		// require dequeued path has not repeated before the expected number of rotations
		require.NotContains(t, dequeuedPathCache, dequeuedPath)

		dequeuedPathCache = append(dequeuedPathCache[1:], dequeuedPath)
		count++
	}

	// count items enqueued to nodes at depth 1
	expectedFirstDimensionCount := len(firstDimensions) * itemsPerDimension
	// count items enqueued to nodes at depth 2
	expectedSecondDimensionCount := len(firstDimensions) * len(secondDimensions) * itemsPerDimension

	require.Equal(t, expectedFirstDimensionCount+expectedSecondDimensionCount, count)

}

// Test_DequeueUnbalancedRoundRobinTree checks dequeuing behavior from an unbalanced tree.
//
// Assertions are done one by one to illustrate and check the behaviors of dequeuing from
// an unbalanced tree, where the same node will be dequeued from twice if the node remains
// nonempty while its sibling nodes have been exhausted and deleted from the tree.
func Test_DequeueUnbalancedRoundRobinTree(t *testing.T) {
	tree := makeUnbalancedRoundRobinTree(t)

	// dequeue from root until exhausted
	_, v := tree.Dequeue()
	require.Equal(t, "root:0:val0", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:1:val0", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:2:0:val0", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:1:0:val0", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:2:1:val0", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:1:val1", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:2:0:val1", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:1:0:val1", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:2:1:val1", v)

	_, v = tree.Dequeue()
	require.Equal(t, "root:2:1:val2", v)

	// all items have been dequeued
	require.Equal(t, 0, tree.rootNode.ItemCount())
	require.Equal(t, 1, tree.rootNode.nodeCount())

	// require nothing in local or child queues
	require.True(t, tree.IsEmpty())
}

func Test_EnqueueDuringDequeueRespectsRoundRobin(t *testing.T) {
	tree, err := NewTree(&roundRobinState{}, &roundRobinState{}, &roundRobinState{})
	require.NoError(t, err)
	require.NotNil(t, tree)

	root := tree.rootNode

	cache := map[string]struct{}{}

	// enqueue two items to path root:0
	childPath := QueuePath{"0"}
	item := makeItemForChildQueue(root, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	item = makeItemForChildQueue(root, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))

	// enqueue one item to path root:1
	childPath = QueuePath{"1"}
	item = makeItemForChildQueue(root, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))

	// enqueue two items to path root:2
	childPath = QueuePath{"2"}
	item = makeItemForChildQueue(root, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	item = makeItemForChildQueue(root, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))

	require.Equal(t, []string{"0", "1", "2"}, root.queueOrder)

	// dequeue first item
	dequeuedPath, _ := tree.Dequeue()
	require.Equal(t, QueuePath{"0"}, dequeuedPath)

	// dequeue second item; root:1 is now exhausted and deleted
	dequeuedPath, _ = tree.Dequeue()
	require.Equal(t, QueuePath{"1"}, dequeuedPath)
	require.Nil(t, root.getNode(QueuePath{"1"}))
	require.Equal(t, []string{"0", "2"}, root.queueOrder)

	// dequeue third item
	dequeuedPath, _ = tree.Dequeue()
	require.Equal(t, QueuePath{"2"}, dequeuedPath)

	// root:1 was previously exhausted; root:0, then root:2 will be next in the rotation
	// here we insert something new into root:1 to test that it
	// does not jump the line in front of root:0 or root:2
	item = makeItemForChildQueue(root, QueuePath{"1"}, cache)
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"1"}, item))
	require.NotNil(t, root.getNode(QueuePath{"1"}))
	require.Equal(t, []string{"0", "2", "1"}, root.queueOrder)

	// dequeue fourth item; the newly-enqueued root:1 item
	// has not jumped the line in front of root:0
	dequeuedPath, _ = tree.Dequeue()
	require.Equal(t, QueuePath{"0"}, dequeuedPath)

	// dequeue fifth item; the newly-enqueued root:1 item
	// has not jumped the line in front of root:2
	dequeuedPath, _ = tree.Dequeue()
	require.Equal(t, QueuePath{"2"}, dequeuedPath)

	// dequeue sixth item; verifying the order 0->2->1 is being followed
	dequeuedPath, _ = tree.Dequeue()
	require.Equal(t, QueuePath{"1"}, dequeuedPath)

	// all items have been dequeued
	require.Equal(t, 0, root.ItemCount())
	require.Equal(t, 1, root.nodeCount())

	// require nothing in local or child queues
	require.True(t, tree.IsEmpty())
}

func Test_NodeCannotDeleteItself(t *testing.T) {
	tests := []struct {
		name     string
		nodeType DequeueAlgorithm
	}{
		{"round robin", &roundRobinState{}},
		{"tenant querier assignment", &tenantQuerierAssignments{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(&roundRobinState{})
			require.NoError(t, err)
			require.NotNil(t, tree)

			require.False(t, tree.rootNode.dequeueAlgorithm.deleteChildNode(tree.rootNode, tree.rootNode))
			require.NotNil(t, tree.rootNode)
		})
	}
}

func makeBalancedRoundRobinTree(t *testing.T, firstDimensions, secondDimensions []string, itemsPerDimension int) *TreeQueue {
	tree, err := NewTree(&roundRobinState{}, &roundRobinState{}, &roundRobinState{})
	require.NoError(t, err)
	require.Equal(t, 1, tree.rootNode.nodeCount())
	require.Equal(t, 0, tree.rootNode.ItemCount())

	cache := map[string]struct{}{}

	for _, firstDimName := range firstDimensions {
		for k := 0; k < itemsPerDimension; k++ {
			childPath := QueuePath{firstDimName}
			item := makeItemForChildQueue(tree.rootNode, childPath, cache)
			require.NoError(t, tree.EnqueueBackByPath(childPath, item))
		}
		for _, secondDimName := range secondDimensions {
			for k := 0; k < itemsPerDimension; k++ {
				childPath := QueuePath{firstDimName, secondDimName}
				item := makeItemForChildQueue(tree.rootNode, childPath, cache)
				require.NoError(t, tree.EnqueueBackByPath(childPath, item))
			}
		}
	}
	return tree
}

func makeUnbalancedRoundRobinTree(t *testing.T) *TreeQueue {
	/*
	   root
	   ├── child0
	   │		 └── localQueue
	   │		     └── val0
	   ├── child1
	   │		 ├── child0
	   │		 │		 └── localQueue
	   │		 │		     ├── val0
	   │		 │		     └── val1
	   │		 └── localQueue
	   │		     ├── val0
	   │		     └── val1
	   ├── child2
	   │		 ├── child0
	   │		 │		 └── localQueue
	   │		 │		     ├── val0
	   │		 │		     └── val1
	   │		 ├── child1
	   │		 │		 └── localQueue
	   │		 │		     ├── val0
	   │		 │		     ├── val1
	   │		 │		     └── val2
	   │		 └── localQueue
	   └── localQueue
	*/
	tree, err := NewTree(&roundRobinState{}, &roundRobinState{}, &roundRobinState{})
	require.NoError(t, err)
	require.Equal(t, 1, tree.rootNode.nodeCount())
	require.Equal(t, 0, tree.rootNode.ItemCount())

	cache := map[string]struct{}{}

	// enqueue one item to root:0
	childPath := QueuePath{"0"}
	item := makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 2, tree.rootNode.nodeCount())
	require.Equal(t, 1, tree.rootNode.ItemCount())

	// enqueue two items to root:1
	childPath = QueuePath{"1"}
	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 3, tree.rootNode.nodeCount())
	require.Equal(t, 2, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 3, tree.rootNode.nodeCount())
	require.Equal(t, 3, tree.rootNode.ItemCount())

	// enqueue two items to root:1:0
	childPath = QueuePath{"1", "0"}
	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 4, tree.rootNode.nodeCount())
	require.Equal(t, 4, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 4, tree.rootNode.nodeCount())
	require.Equal(t, 5, tree.rootNode.ItemCount())

	// enqueue two items to root:2:0
	childPath = QueuePath{"2", "0"}
	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 6, tree.rootNode.nodeCount())
	require.Equal(t, 6, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 6, tree.rootNode.nodeCount())
	require.Equal(t, 7, tree.rootNode.ItemCount())

	// enqueue three items to root:2:1
	childPath = QueuePath{"2", "1"}
	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, tree.rootNode.nodeCount())
	require.Equal(t, 8, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, tree.rootNode.nodeCount())
	require.Equal(t, 9, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, tree.rootNode.nodeCount())
	require.Equal(t, 10, tree.rootNode.ItemCount())

	return tree
}

// makeItemForChildQueue constructs a queue item to match its enqueued path
// by joining the path components and appending an incrementing value for each path.
//
// e.g. for a tree named "root":
//   - childQueuePath{"1", "0"}'s first item will be "root:1:0:val0"
//   - childQueuePath{"1", "0"}'s second item will be "root:1:0:val1"
func makeItemForChildQueue(
	parent *Node, childPath QueuePath, cache map[string]struct{},
) string {
	path := append(QueuePath{parent.name}, childPath...)

	i := 0
	for {
		item := strings.Join(path, ":") + fmt.Sprintf(":val%d", i)
		if _, ok := cache[item]; !ok {
			cache[item] = struct{}{}
			return item
		}
		i++
	}
}
