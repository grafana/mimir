package queue

import (
	//"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

// TODO (casie): Write a test for dequeuing from tqa childA, enqueue to new childB, expect to dequeue next from childB
// TODO (casie): Write a test for enqueueFrontByPath()

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
			name:      "fail to create tree without defined dequeueing algorithm",
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
		rootAlgo         DequeueAlgorithm
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
			// TODO (casie): ugly
			querierID := QuerierID("placeholder")
			switch tt.rootAlgo.(type) {
			case *tenantQuerierAssignments:
				tt.rootAlgo.(*tenantQuerierAssignments).currentQuerier = &querierID
			}
			tree, err := NewTree(tt.rootAlgo)
			require.NoError(t, err)

			for _, elt := range tt.enqueueToRoot {
				err = tree.EnqueueBackByPath(QueuePath{}, elt)
				require.NoError(t, err)
			}
			rootPath := QueuePath{"root"}
			for _, elt := range tt.enqueueToRoot {
				path, v := tree.Dequeue()
				require.Equal(t, rootPath, path)
				require.Equal(t, elt, v)

			}

			path, v := tree.Dequeue()
			require.Equal(t, rootPath, path)
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
			name:             "should dequeue from new (next-in-queue) child immediately after it is added",
			treeAlgosByDepth: []DequeueAlgorithm{&roundRobinState{}, &roundRobinState{}},
			operationOrder: []op{
				{enqueue, QueuePath{"child-1"}, "obj-1"},
				{enqueue, QueuePath{"child-1"}, "obj-2"},
				{dequeue, QueuePath{"root", "child-1"}, "obj-1"},
				{enqueue, QueuePath{"child-2"}, "obj-3"},
				{dequeue, QueuePath{"root", "child-2"}, "obj-3"},
				{dequeue, QueuePath{"root", "child-1"}, "obj-2"},
				{dequeue, QueuePath{"root"}, nil},
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
				{dequeue, QueuePath{"root", "child-1"}, "obj-1"},
				{enqueue, QueuePath{"child-2"}, "obj-3"},
				{dequeue, QueuePath{"root", "child-2"}, "obj-3"},
				{dequeue, QueuePath{"root", "child-1"}, "obj-2"},
				{dequeue, QueuePath{"root"}, nil},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(tt.treeAlgosByDepth...)
			require.NoError(t, err)
			root := tree.rootNode

			for _, operation := range tt.operationOrder {
				if operation.kind == enqueue {
					err = tree.EnqueueBackByPath(operation.path, operation.obj)
					require.NoError(t, err)
				}
				if operation.kind == dequeue {
					path, obj := root.dequeue()
					require.Equal(t, operation.path, path)
					require.Equal(t, operation.obj, obj)
				}
			}
		})
	}
}

func Test_ShuffleShardDequeue(t *testing.T) {
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
			name: "dequeueing for one querier returns nil, but does return for a different querier",
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
			// TODO (casie): also dequeues if the tenant _is not_ in the tenant querier map; is this expected? (probably)
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

// This test is a little messy; I can clean it up, but it's meant to illustrate that we can update a state
// in tenantQuerierAssignments, and the tree dequeue behavior will adjust accordingly.
func Test_ChangeShuffleShardState(t *testing.T) {
	tqa := tenantQuerierAssignments{
		tenantQuerierIDs: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": {"querier-2": {}}},
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
	require.NoError(t, err)

	querier1 := QuerierID("querier-1")
	querier2 := QuerierID("querier-2")
	querier3 := QuerierID("querier-3")

	// set state to querier-2 should dequeue query-2
	state.currentQuerier = &querier2
	_, v := tree.Dequeue()
	require.Equal(t, "query-2", v)

	// update state to querier-1 should dequeue query-1
	state.currentQuerier = &querier1
	_, v = tree.Dequeue()
	require.Equal(t, "query-1", v)

	// update tqa map to add querier-3 as assigned to tenant-2, then set state to querier-3 should dequeue query-3
	tqa.tenantQuerierIDs["tenant-2"]["querier-3"] = struct{}{}
	state.currentQuerier = &querier3
	_, v = tree.Dequeue()
	require.Equal(t, "query-3", v)

	// during reshuffle, we only ever reassign tenant values, we don't assign an entirely new map value
	// to tenantQuerierIDs. Reassign tenant-2 to an empty map value , and query-4 should _not_ be dequeued
	tqa.tenantQuerierIDs["tenant-2"] = map[QuerierID]struct{}{}
	_, v = tree.Dequeue()
	require.Nil(t, v)

}
