package queue

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

// TODO (casie): Add tests for dequeuing from multiple different node children states

func Test_NewNode(t *testing.T) {
	tests := []struct {
		name      string
		rootAlgo  DequeueAlgorithm
		childPath OpsPath
		state     *shuffleShardState
		expectErr bool
	}{
		{
			name:     "create round-robin node",
			rootAlgo: &roundRobinState{},
		},
		{
			name:     "create shuffle-shard node",
			rootAlgo: &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
		},
		{
			name:      "create shuffle-shard tree with no tenant-querier map",
			rootAlgo:  &shuffleShardState{},
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewNode("root", tt.rootAlgo)
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
		name      string
		rootAlgo  DequeueAlgorithm
		children  []OpsPath
		expectErr bool
	}{
		{
			name:     "enqueue round-robin node to round-robin node",
			rootAlgo: &roundRobinState{},
			children: []OpsPath{{{"round-robin-child-1", &roundRobinState{}}}},
		},
		{
			name:     "enqueue shuffle-shard node to round-robin node",
			rootAlgo: &roundRobinState{},
			children: []OpsPath{{{
				"shuffle-shard-child-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			}}},
		},
		{
			name:      "enqueue shuffle-shard node with no tenant-querier map to round-robin node",
			rootAlgo:  &roundRobinState{},
			children:  []OpsPath{{{"shuffle-shard-child-1", &shuffleShardState{}}}},
			expectErr: true,
		},
		{
			name:     "enqueue round-robin node to shuffle-shard node",
			rootAlgo: &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			children: []OpsPath{{{"round-robin-child-1", &roundRobinState{}}}},
		},
		{
			name:     "create tree with multiple shuffle-shard depths",
			rootAlgo: &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			children: []OpsPath{
				{
					{"child", &roundRobinState{}},
					{"grandchild", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
				},
			},
		},
		{
			name:     "enqueue different types of nodes at the same depth",
			rootAlgo: &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			children: []OpsPath{
				{{"child-1", &roundRobinState{}}},
				{{"child-2", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, err := NewNode("root", tt.rootAlgo)
			require.NoError(t, err)

			for _, childPath := range tt.children {
				err = root.enqueueBackByPath(childPath, "some-object")
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
			rootAlgo: &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
		},
		{
			name:          "dequeue from non-empty round-robin root node",
			rootAlgo:      &roundRobinState{},
			enqueueToRoot: []any{"something-in-root"},
		},
		{
			name:          "dequeue from non-empty shuffle-shard root node",
			rootAlgo:      &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			enqueueToRoot: []any{"something-else-in-root"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// TODO (casie): ugly
			querierID := QuerierID("placeholder")
			switch tt.rootAlgo.(type) {
			case *shuffleShardState:
				tt.rootAlgo.(*shuffleShardState).currentQuerier = &querierID
			}

			root, err := NewNode("root", tt.rootAlgo)
			require.NoError(t, err)

			for _, elt := range tt.enqueueToRoot {
				err := root.enqueueBackByPath(OpsPath{}, elt)
				require.NoError(t, err)
			}
			for _, elt := range tt.enqueueToRoot {
				path, v := root.dequeue()
				require.Equal(t, QueuePath{"root"}, path)
				require.Equal(t, elt, v)

			}

			if tt.enqueueToRoot == nil {
				path, v := root.dequeue()
				require.Equal(t, QueuePath{"root"}, path)
				require.Nil(t, v)
			}

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
			path OpsPath
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
				path OpsPath
				objs []any
			}{"grandchild-1": {
				path: OpsPath{{"child-1", &roundRobinState{}}, {"grandchild-1", &roundRobinState{}}},
				objs: []any{"grandchild-1:object-1"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, err := NewNode("root", &roundRobinState{})
			require.NoError(t, err)

			for _, sqo := range tt.selfQueueObjects {
				err = root.enqueueBackByPath(OpsPath{}, sqo)
				require.NoError(t, err)
			}

			for _, child := range tt.children {
				for _, obj := range tt.childQueueObjects[child] {
					err = root.enqueueBackByPath(OpsPath{{child, &roundRobinState{}}}, obj)
				}
			}

			for _, grandchild := range tt.grandchildren {
				gqo := tt.grandchildrenQueueObjects[grandchild]
				for _, obj := range gqo.objs {
					err := root.enqueueBackByPath(gqo.path, obj)
					require.NoError(t, err)
				}
			}

			for _, expected := range tt.expected {
				_, val := root.dequeue()
				v, ok := val.(string)
				require.True(t, ok)
				require.Equal(t, expected, v)
			}
		})
	}
}

func Test_ShuffleShardDequeue(t *testing.T) {
	type enqueueObj struct {
		obj  any
		path OpsPath
	}

	tests := []struct {
		name        string
		rootAlgo    DequeueAlgorithm
		state       *shuffleShardState
		currQuerier []QuerierID
		enqueueObjs []enqueueObj
		expected    []any
		expectErr   bool
	}{
		{
			name:     "happy path - tenant found in tenant-querier map under first child",
			rootAlgo: &roundRobinState{},
			state: &shuffleShardState{
				tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
			},
			currQuerier: []QuerierID{"querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: OpsPath{
					{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
					{"tenant-1", &roundRobinState{}},
				}},
			},
			expected: []any{"query-1"},
		},
		{
			name:     "tenant exists, but not for querier",
			rootAlgo: &roundRobinState{},
			state: &shuffleShardState{
				tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}},
			},
			currQuerier: []QuerierID{"querier-1"},
			enqueueObjs: []enqueueObj{
				{
					obj: "query-1",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-1", &roundRobinState{}},
					},
				},
			},
			expected: []any{nil},
		},
		{
			name:     "1 of 3 tenants exist for querier",
			rootAlgo: &roundRobinState{},
			state: &shuffleShardState{
				tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}}},
			currQuerier: []QuerierID{"querier-1"},
			enqueueObjs: []enqueueObj{
				{
					obj: "query-1",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-1", &roundRobinState{}},
					},
				},
				{
					obj: "query-2",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-2", &roundRobinState{}},
					},
				},
				{
					obj: "query-3",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-3", &roundRobinState{}},
					},
				},
			},
			expected: []any{"query-3"},
		},
		{
			name:     "tenant exists for querier on next parent node",
			rootAlgo: &roundRobinState{},
			state: &shuffleShardState{
				tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}}},
			currQuerier: []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{
					obj: "query-1",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-1", &roundRobinState{}},
					},
				},
				{
					obj: "query-2",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-2", &roundRobinState{}},
					},
				},
				{
					obj: "query-3",
					path: OpsPath{
						{"query-component-2", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-3", &roundRobinState{}},
					},
				},
				{
					obj: "query-4",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-3", &roundRobinState{}},
					},
				},
			},
			expected: []any{"query-4", "query-3", nil},
		},
		{
			name:        "2 of 3 tenants exist for querier",
			rootAlgo:    &roundRobinState{},
			state:       &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}}},
			currQuerier: []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{
					obj: "query-1",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-1", &roundRobinState{}},
					},
				},
				{
					obj: "query-2",
					path: OpsPath{
						{"query-component-1", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-2", &roundRobinState{}},
					},
				},
				{
					obj: "query-3",
					path: OpsPath{
						{"query-component-2", &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}}},
						{"tenant-3", &roundRobinState{}},
					},
				},
			},
			expected: []any{"query-1", "query-3", nil},
		},
		{
			name:        "root node is shuffle-shard node",
			rootAlgo:    &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			state:       &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}}},
			currQuerier: []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{
					obj: "query-1",
					path: OpsPath{
						{"tenant-2", &roundRobinState{}},
						{"query-component-1", &roundRobinState{}},
					},
				},
				{
					obj: "query-2",
					path: OpsPath{
						{"tenant-1", &roundRobinState{}},
						{"query-component-2", &roundRobinState{}},
					},
				},
				{
					obj: "query-3",
					path: OpsPath{
						{"tenant-1", &roundRobinState{}},
						{"query-component-1", &roundRobinState{}},
					},
				},
			},
			expected: []any{"query-2", "query-3", nil},
		},
		{
			name:        "dequeueing for one querier returns nil, but does return for a different querier",
			rootAlgo:    &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			state:       &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}}},
			currQuerier: []QuerierID{"querier-2", "querier-1"},
			enqueueObjs: []enqueueObj{
				{
					obj: "query-1",
					path: OpsPath{
						{"tenant-1", &roundRobinState{}},
						{"query-component-1", &roundRobinState{}},
					},
				},
			},
			expected: []any{nil, "query-1"},
		},
		{
			name:        "no querier set in state",
			rootAlgo:    &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{}},
			state:       &shuffleShardState{tenantQuerierMap: map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}}},
			currQuerier: []QuerierID{""},
			enqueueObjs: []enqueueObj{
				{
					obj: "query-1",
					path: OpsPath{
						{"tenant-1", &roundRobinState{}},
						{"query-component-1", &roundRobinState{}},
					},
				},
			},
			expected: []any{nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentQuerier := QuerierID("placeholder")
			tt.state.currentQuerier = &currentQuerier

			// TODO (casie): ugly
			switch tt.rootAlgo.(type) {
			case *shuffleShardState:
				tt.rootAlgo = tt.state
			}
			root, err := NewNode("root", tt.rootAlgo)
			require.NoError(t, err)

			for _, o := range tt.enqueueObjs {
				for i, elt := range o.path {
					switch elt.dequeueAlgorithm.(type) {
					case *shuffleShardState:
						o.path[i].dequeueAlgorithm = tt.state
					}
				}
				err = root.enqueueBackByPath(o.path, o.obj)
				require.NoError(t, err)
			}
			// currQuerier at position i is used to dequeue the expected result at position i
			require.Equal(t, len(tt.currQuerier), len(tt.expected))
			for i := 0; i < len(tt.expected); i++ {
				currentQuerier = tt.currQuerier[i]
				path, v := root.dequeue()
				fmt.Println(path, v)
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

	state := &shuffleShardState{
		tenantQuerierMap: tqa.tenantQuerierIDs,
		currentQuerier:   nil,
	}

	root, err := NewNode("root", state)
	//tree, err := NewTree([]NodeType{shuffleShard, roundRobin, roundRobin}, state)
	require.NoError(t, err)

	err = root.enqueueBackByPath(OpsPath{
		{"tenant-1", &roundRobinState{}},
		{"query-component-1", &roundRobinState{}},
	}, "query-1")
	err = root.enqueueBackByPath(OpsPath{
		{"tenant-2", &roundRobinState{}},
		{"query-component-1", &roundRobinState{}},
	}, "query-2")
	err = root.enqueueBackByPath(OpsPath{
		{"tenant-2", &roundRobinState{}},
		{"query-component-1", &roundRobinState{}},
	}, "query-3")
	err = root.enqueueBackByPath(OpsPath{
		{"tenant-2", &roundRobinState{}},
		{"query-component-1", &roundRobinState{}},
	}, "query-4")
	require.NoError(t, err)

	querier1 := QuerierID("querier-1")
	querier2 := QuerierID("querier-2")
	querier3 := QuerierID("querier-3")

	// set state to querier-2 should dequeue query-2
	state.currentQuerier = &querier2
	_, v := root.dequeue()
	require.Equal(t, "query-2", v)

	// update state to querier-1 should dequeue query-1
	state.currentQuerier = &querier1
	_, v = root.dequeue()
	require.Equal(t, "query-1", v)

	// update tqa map to add querier-3 as assigned to tenant-2, then set state to querier-3 should dequeue query-3
	tqa.tenantQuerierIDs["tenant-2"]["querier-3"] = struct{}{}
	state.currentQuerier = &querier3
	_, v = root.dequeue()
	require.Equal(t, "query-3", v)

	// during reshuffle, we only ever reassign tenant values, we don't assign an entirely new map value
	// to tenantQuerierIDs. Reassign tenant-2 to an empty map value , and query-4 should _not_ be dequeued
	tqa.tenantQuerierIDs["tenant-2"] = map[QuerierID]struct{}{}
	_, v = root.dequeue()
	require.Nil(t, v)

}
