// SPDX-License-Identifier: AGPL-3.0-only

package tree

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// enqueueObj is intended for use in tests; it represents an obj to enqueue to path, or an expected path and obj
// for a given dequeue
type enqueueObj struct {
	obj  any
	path QueuePath
}

func Test_NewTree(t *testing.T) {
	tests := []struct {
		name      string
		treeAlgos []QueuingAlgorithm
		expectErr bool
	}{
		{
			name:      "create round-robin tree",
			treeAlgos: []QueuingAlgorithm{&RoundRobinState{}},
		},
		{
			name:      "create tenant-querier tree",
			treeAlgos: []QueuingAlgorithm{&TenantQuerierQueuingAlgorithm{}},
		},
		{
			name:      "successfully create a tree with no queuing algorithms", // creates a single node with a queue
			treeAlgos: []QueuingAlgorithm{},
		},
		{
			name:      "fail to create tree with nil dequeuing algorithm",
			treeAlgos: []QueuingAlgorithm{nil},
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
		name                string
		treeAlgosByDepth    []QueuingAlgorithm
		childPathsToEnqueue []QueuePath
		expectErr           bool
	}{
		{
			name:                "enqueue round-robin node to round-robin node",
			treeAlgosByDepth:    []QueuingAlgorithm{&RoundRobinState{}},
			childPathsToEnqueue: []QueuePath{{"child-1"}},
		},
		{
			name:             "enqueue tenant-querier node to round-robin node",
			treeAlgosByDepth: []QueuingAlgorithm{&RoundRobinState{}, NewTenantQuerierQueuingAlgorithm()},
			childPathsToEnqueue: []QueuePath{
				{"tenant-querier-1", "child-1"},
				{"tenant-querier-2", "child-2"},
				{"tenant-querier-1", "child-2"},
			},
		},
		{
			name: "enqueue round-robin node to tenant-querier node",
			treeAlgosByDepth: []QueuingAlgorithm{
				NewTenantQuerierQueuingAlgorithm(),
			},
			childPathsToEnqueue: []QueuePath{{"child-1"}, {"child-2"}},
		},
		{
			name: "enqueue tenant-querier node to tenant-querier node",
			treeAlgosByDepth: []QueuingAlgorithm{
				NewTenantQuerierQueuingAlgorithm(),
				NewTenantQuerierQueuingAlgorithm(),
			},
			childPathsToEnqueue: []QueuePath{{"child", "grandchild"}},
		},
		{
			name: "fail to enqueue to a node at depth 1 in tree with max-depth of 2",
			treeAlgosByDepth: []QueuingAlgorithm{
				NewTenantQuerierQueuingAlgorithm(),
				&RoundRobinState{},
				NewTenantQuerierQueuingAlgorithm(),
			},
			childPathsToEnqueue: []QueuePath{{"child"}},
			expectErr:           true,
		},
		{
			name:                "fail to enqueue beyond max-depth",
			treeAlgosByDepth:    []QueuingAlgorithm{&RoundRobinState{}},
			childPathsToEnqueue: []QueuePath{{"child", "grandchild"}},
			expectErr:           true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(tt.treeAlgosByDepth...)
			require.NoError(t, err)

			for _, childPath := range tt.childPathsToEnqueue {
				err = tree.EnqueueBackByPath(childPath, "some-object")
			}
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(tt.childPathsToEnqueue), tree.ItemCount())
			}
		})
	}
}

func Test_EnqueueFrontByPath(t *testing.T) {
	someQuerier := "placeholder"
	tests := []struct {
		name             string
		treeAlgosByDepth []QueuingAlgorithm
		enqueueObjs      []enqueueObj
		expected         []any
	}{
		{
			name:             "enqueue to front of a leaf off round-robin node",
			treeAlgosByDepth: []QueuingAlgorithm{},
			enqueueObjs: []enqueueObj{
				{"query-1", QueuePath{}},
				{"query-2", QueuePath{}},
			},
			expected: []any{"query-2", "query-1"},
		},
		{
			name:             "enqueue to front of a leaf off tenant-querier node",
			treeAlgosByDepth: []QueuingAlgorithm{NewTenantQuerierQueuingAlgorithm()},
			enqueueObjs: []enqueueObj{
				{"query-1", QueuePath{"tq-1"}},
				{"query-2", QueuePath{"tq-1"}},
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
				_, v := tree.Dequeue(&DequeueArgs{QuerierID: someQuerier})
				require.Equal(t, expectedVal, v)
			}
			_, v := tree.Dequeue(&DequeueArgs{QuerierID: someQuerier})
			require.Nil(t, v)
		})
	}
}

func Test_Dequeue_SingleAlgo(t *testing.T) {
	tests := []struct {
		name        string
		rootAlgo    []QueuingAlgorithm
		enqueueObjs []enqueueObj
		dequeueArgs *DequeueArgs
		expectNil   bool
	}{
		{
			name: "dequeue from empty leaf node",
		},
		{
			name:     "dequeue from empty round-robin root node",
			rootAlgo: []QueuingAlgorithm{&RoundRobinState{}},
		},
		{
			name:        "dequeue from empty tenant-querier root node",
			rootAlgo:    []QueuingAlgorithm{NewTenantQuerierQueuingAlgorithm()},
			dequeueArgs: &DequeueArgs{QuerierID: "placeholder", LastTenantIndex: newQuerierTenantIndex},
		},
		{
			name:        "dequeue from non-empty leaf node",
			rootAlgo:    []QueuingAlgorithm{},
			enqueueObjs: []enqueueObj{{"something-in-root", QueuePath{}}},
		},
		{
			name:        "dequeue from non-empty round-robin root node",
			rootAlgo:    []QueuingAlgorithm{&RoundRobinState{}},
			enqueueObjs: []enqueueObj{{"round-robin-child-object", QueuePath{"round-robin-child"}}},
		},
		{
			name:        "dequeue from non-empty tenant-querier root node",
			rootAlgo:    []QueuingAlgorithm{NewTenantQuerierQueuingAlgorithm()},
			enqueueObjs: []enqueueObj{{"tqa-child-object", QueuePath{"tqa-child"}}},
			dequeueArgs: &DequeueArgs{QuerierID: "placeholder", LastTenantIndex: newQuerierTenantIndex},
		},
		{
			name:        "dequeue from non-empty tenant-querier root node with no current querier",
			rootAlgo:    []QueuingAlgorithm{NewTenantQuerierQueuingAlgorithm()},
			enqueueObjs: []enqueueObj{{"tqa-child-object", QueuePath{"tqa-child"}}},
			expectNil:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.LessOrEqual(t, len(tt.rootAlgo), 1)
			tree, err := NewTree(tt.rootAlgo...)
			require.NoError(t, err)

			for _, obj := range tt.enqueueObjs {
				err = tree.EnqueueBackByPath(obj.path, obj.obj)
				require.NoError(t, err)
			}

			for _, obj := range tt.enqueueObjs {
				dequeuePath, v := tree.Dequeue(tt.dequeueArgs)
				if !tt.expectNil {
					require.Equal(t, obj.path, dequeuePath)
					require.Equal(t, obj.obj, v)
				} else {
					require.Equal(t, QueuePath{}, dequeuePath)
					require.Nil(t, v)
				}

			}

			dequeuePath, v := tree.Dequeue(tt.dequeueArgs)
			require.Equal(t, QueuePath{}, dequeuePath)
			require.Nil(t, v)

		})
	}
}

func Test_RoundRobinDequeue(t *testing.T) {
	tests := []struct {
		name                      string
		treeDepth                 int
		enqueueObjs               []enqueueObj
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
			name:        "dequeue from round-robin child",
			treeDepth:   2,
			enqueueObjs: []enqueueObj{{"child-1:some-object", QueuePath{"child-1"}}},
			expected:    []string{"child-1:some-object"},
		},
		{
			name:        "dequeue from second round-robin child when first child is empty",
			treeDepth:   2,
			enqueueObjs: []enqueueObj{{nil, QueuePath{"child-1"}}, {"child-2:some-object", QueuePath{"cihld-2"}}},
			expected:    []string{"child-2:some-object"},
		},
		{
			name:      "dequeue from round-robin grandchild",
			treeDepth: 3,
			enqueueObjs: []enqueueObj{
				{"grandchild-1:object-1", QueuePath{"child-1", "grandchild-1"}},
				{"grandchild-1:object-2", QueuePath{"child-1", "grandchild-1"}},
				{"grandchild-2:object-1", QueuePath{"child-2", "grandchild-2"}},
			},
			expected:      []string{"grandchild-1:object-1", "grandchild-2:object-1", "grandchild-1:object-2"},
			grandchildren: []string{"grandchild-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := make([]QueuingAlgorithm, 0)
			for i := 0; i < tt.treeDepth-1; i++ {
				args = append(args, &RoundRobinState{})
			}
			tree, err := NewTree(args...)
			require.NoError(t, err)

			for _, eo := range tt.enqueueObjs {
				err = tree.EnqueueBackByPath(eo.path, eo.obj)
				require.NoError(t, err)
			}

			for _, expected := range tt.expected {
				_, val := tree.Dequeue(nil)
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
	placeholderQuerier := "some-querier"

	type op struct {
		kind opType
		path QueuePath
		obj  any
	}

	tests := []struct {
		name             string
		treeAlgosByDepth []QueuingAlgorithm
		operationOrder   []op
	}{
		{
			name:             "round-robin node should dequeue from first child one more time after new node added",
			treeAlgosByDepth: []QueuingAlgorithm{&RoundRobinState{}},
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
			name: "should dequeue from new tenant-querier child before repeat-dequeueing",
			treeAlgosByDepth: []QueuingAlgorithm{
				NewTenantQuerierQueuingAlgorithm(),
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
				if operation.kind == enqueue {
					err = tree.EnqueueBackByPath(operation.path, operation.obj)
					require.NoError(t, err)
				}
				if operation.kind == dequeue {
					path, obj := tree.Dequeue(&DequeueArgs{QuerierID: placeholderQuerier})
					require.Equal(t, operation.path, path)
					require.Equal(t, operation.obj, obj)
				}
			}
		})
	}
}

func Test_TenantQuerierAssignmentsDequeue(t *testing.T) {
	tests := []struct {
		name             string
		treeAlgosByDepth []QueuingAlgorithm
		currQuerier      []string
		enqueueObjs      []enqueueObj
		expected         []any
		expectErr        bool
	}{
		{
			name: "happy path - tenant found in tenant-querier map under first child",
			treeAlgosByDepth: []QueuingAlgorithm{
				&RoundRobinState{},
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
					tenantNodes:      map[string][]*Node{},
				},
			},
			currQuerier: []string{"querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
			},
			expected: []any{"query-1"},
		},
		{
			name: "tenant exists, but not for querier",
			treeAlgosByDepth: []QueuingAlgorithm{
				&RoundRobinState{},
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}},
					tenantNodes:      map[string][]*Node{},
				},
			},
			currQuerier: []string{"querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
			},
			expected: []any{nil},
		},
		{
			name: "1 of 3 tenants exist for querier",
			treeAlgosByDepth: []QueuingAlgorithm{
				&RoundRobinState{},
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
					tenantNodes:      map[string][]*Node{},
				},
			},
			currQuerier: []string{"querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
				{obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}},
				{obj: "query-3", path: QueuePath{"query-component-1", "tenant-3"}},
			},
			expected: []any{"query-3", nil},
		},
		{
			name: "tenant exists for querier on next parent node",
			treeAlgosByDepth: []QueuingAlgorithm{
				&RoundRobinState{},
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
					tenantNodes:      map[string][]*Node{},
				},
			},
			currQuerier: []string{"querier-1", "querier-1", "querier-1"},
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
			treeAlgosByDepth: []QueuingAlgorithm{
				&RoundRobinState{},
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
					tenantNodes:      map[string][]*Node{},
				},
			},
			currQuerier: []string{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
				{obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}},
				{obj: "query-3", path: QueuePath{"query-component-2", "tenant-3"}},
			},
			expected: []any{"query-1", "query-3", nil},
		},
		{
			name: "root node is tenant-querier node",
			treeAlgosByDepth: []QueuingAlgorithm{
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": {}},
					tenantNodes:      map[string][]*Node{},
				},
				&RoundRobinState{},
			},
			currQuerier: []string{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-2", "query-component-1"}},
				{obj: "query-2", path: QueuePath{"tenant-1", "query-component-2"}},
				{obj: "query-3", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{"query-2", "query-3", nil},
		},
		{
			name: "dequeuing for one querier returns nil, but does return for a different querier",
			treeAlgosByDepth: []QueuingAlgorithm{
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
					tenantNodes:      map[string][]*Node{},
				},
				&RoundRobinState{},
			},
			currQuerier: []string{"querier-2", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{nil, "query-1"},
		},
		{
			name: "no querier set in state",
			treeAlgosByDepth: []QueuingAlgorithm{
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
					tenantNodes:      map[string][]*Node{},
				},
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
					tenantNodes:      map[string][]*Node{},
				},
			},
			currQuerier: []string{""},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{nil},
		},
		{
			// This also dequeues if the tenant _is not_ in the tenant querier map; is this expected? (probably)
			name: "dequeue from a tenant with a nil tenant-querier map",
			treeAlgosByDepth: []QueuingAlgorithm{
				&TenantQuerierQueuingAlgorithm{
					tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": nil},
					tenantNodes:      map[string][]*Node{},
				},
				&RoundRobinState{},
			},
			currQuerier: []string{"querier-1", "querier-1", "querier-1"},
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
			tree, err := NewTree(tt.treeAlgosByDepth...)
			require.NoError(t, err)

			for _, o := range tt.enqueueObjs {
				err = tree.EnqueueBackByPath(o.path, o.obj)
				require.NoError(t, err)
			}
			// currQuerier at position i is used to dequeue the expected result at position i
			require.Equal(t, len(tt.currQuerier), len(tt.expected))
			for i := 0; i < len(tt.expected); i++ {
				_, v := tree.Dequeue(&DequeueArgs{QuerierID: tt.currQuerier[i], LastTenantIndex: i - 1})
				require.Equal(t, tt.expected[i], v)
			}
		})
	}

}

// Test_ChangeTenantQuerierAssignments illustrates that we can update a state in tenantQuerierAssignments,
// and the tree dequeue behavior will adjust accordingly.
func Test_ChangeTenantQuerierAssignments(t *testing.T) {
	tqa := &TenantQuerierQueuingAlgorithm{
		tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{
			"tenant-1": {"querier-1": {}},
			"tenant-2": {"querier-2": {}},
		},
		tenantNodes:    map[string][]*Node{},
		currentQuerier: "",
	}

	tree, err := NewTree(tqa, &RoundRobinState{})
	require.NoError(t, err)

	enqueueObjs := []enqueueObj{
		{"query-1", QueuePath{"tenant-1", "query-component-1"}},
		{"query-2", QueuePath{"tenant-2", "query-component-1"}},
		{"query-3", QueuePath{"tenant-2", "query-component-1"}},
		{"query-4", QueuePath{"tenant-2", "query-component-1"}},
		{"query-5", QueuePath{"tenant-3", "query-component-1"}},
	}

	for _, eo := range enqueueObjs {
		err = tree.EnqueueBackByPath(eo.path, eo.obj)
		require.NoError(t, err)
	}

	querier1 := "querier-1"
	querier2 := "querier-2"
	querier3 := "querier-3"

	// dequeue for querier-2 should dequeue query-2
	_, v := tree.Dequeue(&DequeueArgs{QuerierID: querier2, LastTenantIndex: newQuerierTenantIndex})
	require.Equal(t, "query-2", v)

	// dequeue for querier-1 should dequeue query-1
	_, v = tree.Dequeue(&DequeueArgs{QuerierID: querier1, LastTenantIndex: newQuerierTenantIndex})
	require.Equal(t, "query-1", v)

	// update tqa map to add querier-3 as assigned to tenant-2, then dequeue for querier-3 should dequeue query-3
	tqa.tenantQuerierIDs["tenant-2"]["querier-3"] = struct{}{}
	_, v = tree.Dequeue(&DequeueArgs{QuerierID: querier3, LastTenantIndex: newQuerierTenantIndex})
	require.Equal(t, "query-3", v)

	// during reshuffle, we only ever reassign tenant values, we don't assign an entirely new map value
	// to tenantQuerierIDs. Reassign tenant-2 to an empty map value, and query-5 (tenant-3), which can be handled
	// by any querier, should be dequeued,
	tqa.tenantQuerierIDs["tenant-2"] = map[QuerierID]struct{}{}
	_, v = tree.Dequeue(&DequeueArgs{QuerierID: querier3})
	require.Equal(t, "query-5", v)

	// then we should not be able to dequeue query-4
	tqa.tenantQuerierIDs["tenant-2"] = map[QuerierID]struct{}{}
	_, v = tree.Dequeue(&DequeueArgs{QuerierID: querier3})
	require.Nil(t, v)

}

// Test_DequeueBalancedRoundRobinTree checks dequeuing behavior from a balanced round-robin tree.
//
// Dequeuing from a balanced tree allows the test to have a simple looped structures
// while running checks to ensure that round-robin order is respected.
func Test_DequeueBalancedRoundRobinTree(t *testing.T) {
	/* balanced tree structure:
		root
		├── 0
		│   ├── a
		│	│	├── 0:a:val0
	    │   │   ├── 0:a:val1
	    │   │   ├── 0:a:val2
		│   │   ├── 0:a:val3
		│   │   └── 0:a:val4
		│   ├── b
		│	│	├── 0:b:val0
	    │   │   ├── 0:b:val1
	    │   │   ├── 0:b:val2
		│   │   ├── 0:b:val3
		│   │   └── 0:b:val4
		│   └── c
		│	 	├── 0:c:val0
	    │       ├── 0:c:val1
	    │       ├── 0:c:val2
		│       ├── 0:c:val3
		│       └── 0:c:val4
		├── 1
		│   ├── a
		│	│	...
		│   ├── b
		│	│	...
		│   └── c
		│		...
		└── 2
		    ├── a
		 	│	...
		    ├── b
		 	│	...
		    └── c
		 		...
	*/
	firstDimensions := []string{"0", "1", "2"}
	secondDimensions := []string{"a", "b", "c"}
	itemsPerDimension := 5

	tree := makeBalancedRoundRobinTree(t, firstDimensions, secondDimensions, itemsPerDimension)
	require.NotNil(t, tree)

	count := 0

	// MultiAlgorithmTreeQueue will fairly dequeue from each child node; subtract one to avoid counting
	// the local queue of first-dimension node.
	rotationsBeforeRepeat := len(firstDimensions)*len(secondDimensions) - 1
	// track dequeued paths to ensure round-robin dequeuing does not repeat before expected
	dequeuedPathCache := make([]QueuePath, rotationsBeforeRepeat)

	for !tree.IsEmpty() {
		dequeuedPath, _ := tree.Dequeue(nil)

		// require dequeued path has not repeated before the expected number of rotations
		require.NotContains(t, dequeuedPathCache, dequeuedPath)

		dequeuedPathCache = append(dequeuedPathCache[1:], dequeuedPath)
		count++
	}

	// count items enqueued; there should be size(firstDim) * size(secondDim) * itemsPerDim
	expectedSecondDimensionCount := len(firstDimensions) * len(secondDimensions) * itemsPerDimension

	require.Equal(t, expectedSecondDimensionCount, count)

}

// Test_DequeueUnbalancedRoundRobinTree checks dequeuing behavior from an unbalanced tree.
//
// Assertions are done one by one to illustrate and check the behaviors of dequeuing from
// an unbalanced tree, where the same node will be dequeued from twice if the node remains
// nonempty while its sibling nodes have been exhausted and deleted from the tree.
func Test_DequeueUnbalancedRoundRobinTree(t *testing.T) {
	tree := makeUnbalancedRoundRobinTree(t)

	expectedVals := []string{
		"root:0:a:val0",
		"root:1:a:val0",
		"root:2:a:val0",
		"root:1:a:val1",
		"root:2:b:val0",
		"root:2:a:val1",
		"root:2:b:val1",
		"root:2:b:val2",
	}

	for _, expected := range expectedVals {
		_, v := tree.Dequeue(nil)
		require.Equal(t, expected, v)
	}

	// all items have been dequeued
	require.Equal(t, 0, tree.rootNode.ItemCount())
	require.Equal(t, 1, nodeCount(tree.rootNode))

	// require nothing in local or child queues
	require.True(t, tree.IsEmpty())
}

func Test_EnqueueDuringDequeueRespectsRoundRobin(t *testing.T) {
	tree, err := NewTree(&RoundRobinState{})
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

	require.Equal(t, []string{"1", "2", "0"}, root.queueOrder)

	// dequeue first item
	dequeuedPath, _ := tree.Dequeue(nil)
	require.Equal(t, QueuePath{"0"}, dequeuedPath)

	// dequeue second item; root:1 is now exhausted and deleted
	dequeuedPath, _ = tree.Dequeue(nil)
	require.Equal(t, QueuePath{"1"}, dequeuedPath)
	require.Nil(t, root.getNode(QueuePath{"1"}))
	require.Equal(t, []string{"2", "0"}, root.queueOrder)

	// dequeue third item
	dequeuedPath, _ = tree.Dequeue(nil)
	require.Equal(t, QueuePath{"2"}, dequeuedPath)

	// root:1 was previously exhausted; root:0, then root:2 will be next in the rotation
	// here we insert something new into root:1 to test that it
	// does not jump the line in front of root:0 or root:2
	item = makeItemForChildQueue(root, QueuePath{"1"}, cache)
	require.NoError(t, tree.EnqueueBackByPath(QueuePath{"1"}, item))
	require.NotNil(t, root.getNode(QueuePath{"1"}))
	require.Equal(t, []string{"2", "1", "0"}, root.queueOrder)

	// dequeue fourth item; the newly-enqueued root:1 item
	// has not jumped the line in front of root:0
	dequeuedPath, _ = tree.Dequeue(nil)
	require.Equal(t, QueuePath{"0"}, dequeuedPath)

	// dequeue fifth item; the newly-enqueued root:1 item
	// has not jumped the line in front of root:2
	dequeuedPath, _ = tree.Dequeue(nil)
	require.Equal(t, QueuePath{"2"}, dequeuedPath)

	// dequeue sixth item; verifying the order 0->2->1 is being followed
	dequeuedPath, _ = tree.Dequeue(nil)
	require.Equal(t, QueuePath{"1"}, dequeuedPath)

	// all items have been dequeued
	require.Equal(t, 0, root.ItemCount())
	require.Equal(t, 1, nodeCount(root))

	// require nothing in local or child queues
	require.True(t, tree.IsEmpty())
}

// Test_NodeCannotDeleteItself creates an empty node, dequeues from it, and ensures that the node still exists
// and has not deleted itself.
func Test_NodeCannotDeleteItself(t *testing.T) {
	tests := []struct {
		name     string
		nodeType []QueuingAlgorithm
	}{
		{"root node is leaf node", []QueuingAlgorithm{}},
		{"round robin", []QueuingAlgorithm{&RoundRobinState{}}},
		{"tenant querier assignment", []QueuingAlgorithm{&TenantQuerierQueuingAlgorithm{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.LessOrEqual(t, len(tt.nodeType), 1)
			tree, err := NewTree(tt.nodeType...)
			require.NoError(t, err)
			require.NotNil(t, tree)

			_, _ = tree.Dequeue(nil)

			require.NotNil(t, tree.rootNode)
			if tree.rootNode.isLeaf() {
				require.Zero(t, tree.rootNode.getLocalQueue().Len())
			} else {
				require.Empty(t, tree.rootNode.queueMap)
			}
		})
	}
}

func makeBalancedRoundRobinTree(t *testing.T, firstDimensions, secondDimensions []string, itemsPerDimension int) *MultiAlgorithmTreeQueue {
	tree, err := NewTree(&RoundRobinState{}, &RoundRobinState{})
	require.NoError(t, err)
	require.Equal(t, 1, nodeCount(tree.rootNode))
	require.Equal(t, 0, tree.rootNode.ItemCount())

	cache := map[string]struct{}{}

	for _, firstDimName := range firstDimensions {
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

func makeUnbalancedRoundRobinTree(t *testing.T) *MultiAlgorithmTreeQueue {
	/*
	   root
	   ├── 0
	   │   └── a
	   │	   └── localQueue
	   │           └──val0
	   ├── 1
	   │   └── a
	   │	   └── localQueue
	   │		   ├── val0
	   │		   └── val1
	   └── 2
	       ├── a
	       │   └── localQueue
	       │	   ├── val0
	       │	   └── val1
	       └── b
	           └── localQueue
	        	   ├── val0
	        	   ├── val1
	        	   └── val2

	*/
	tree, err := NewTree(&RoundRobinState{}, &RoundRobinState{})
	require.NoError(t, err)
	require.Equal(t, 1, nodeCount(tree.rootNode))
	require.Equal(t, 0, tree.rootNode.ItemCount())

	cache := map[string]struct{}{}

	// enqueue one item to root:0:a
	childPath := QueuePath{"0", "a"}
	item := makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 3, nodeCount(tree.rootNode))
	require.Equal(t, 1, tree.rootNode.ItemCount())

	// enqueue two items to root:1:a
	childPath = QueuePath{"1", "a"}
	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 5, nodeCount(tree.rootNode))
	require.Equal(t, 2, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 5, nodeCount(tree.rootNode))
	require.Equal(t, 3, tree.rootNode.ItemCount())

	// enqueue two items to root:2:a
	childPath = QueuePath{"2", "a"}
	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, nodeCount(tree.rootNode))
	require.Equal(t, 4, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 7, nodeCount(tree.rootNode))
	require.Equal(t, 5, tree.rootNode.ItemCount())

	// enqueue three items to root:2:b
	childPath = QueuePath{"2", "b"}
	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 8, nodeCount(tree.rootNode))
	require.Equal(t, 6, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 8, nodeCount(tree.rootNode))
	require.Equal(t, 7, tree.rootNode.ItemCount())

	item = makeItemForChildQueue(tree.rootNode, childPath, cache)
	require.NoError(t, tree.EnqueueBackByPath(childPath, item))
	require.Equal(t, 8, nodeCount(tree.rootNode))
	require.Equal(t, 8, tree.rootNode.ItemCount())

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

func nodeCount(n *Node) int {
	count := 1
	for _, child := range n.queueMap {
		count += nodeCount(child)
	}
	return count
}
