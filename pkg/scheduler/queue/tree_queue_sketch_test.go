package queue

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_NewTree(t *testing.T) {
	tests := []struct {
		name      string
		args      []NodeType
		expectErr bool
	}{
		{
			name: "create round-robin-only tree",
			args: []NodeType{roundRobin, roundRobin},
		},
		{
			name: "create shuffle-shard-only tree",
			args: []NodeType{shuffleShard},
		},
		{
			name:      "create empty tree",
			args:      []NodeType{},
			expectErr: true,
		},
		{
			name:      "create tree with multiple shuffle-shard depths",
			args:      []NodeType{shuffleShard, roundRobin, shuffleShard},
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTree(tt.args, &ShuffleShardState{map[TenantID]map[QuerierID]struct{}{}, nil})
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

		})
	}
}

func Test_EnqueueCreatingNode(t *testing.T) {
	tests := []struct {
		name            string
		nodeTypeByDepth []NodeType
		enqueueNodeName string
		expectErr       bool
	}{
		{
			name:            "enqueue over max-depth to round-robin node",
			nodeTypeByDepth: []NodeType{roundRobin},
			enqueueNodeName: "bad-child-1",
			expectErr:       true,
		},
		{
			name:            "enqueue over max-depth to shuffle-shard node",
			nodeTypeByDepth: []NodeType{shuffleShard},
			enqueueNodeName: "bad-child-1",
			expectErr:       true,
		},
		{
			name:            "enqueue round-robin node to round-robin node",
			nodeTypeByDepth: []NodeType{roundRobin, roundRobin},
			enqueueNodeName: "round-robin-child-1",
		},
		{
			name:            "enqueue shuffle-shard node to round-robin node",
			nodeTypeByDepth: []NodeType{roundRobin, shuffleShard},
			enqueueNodeName: "shuffle-shard-child-1",
		},
		{
			name:            "enqueue round-robin node to shuffle-shard node",
			nodeTypeByDepth: []NodeType{shuffleShard, roundRobin},
			enqueueNodeName: "round-robin-child-1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(tt.nodeTypeByDepth, &ShuffleShardState{map[TenantID]map[QuerierID]struct{}{}, nil})
			require.NoError(t, err)
			err = tree.EnqueueBackByPath(QueuePath{tt.enqueueNodeName}, "some-object")
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
		rootNodeType  NodeType
		enqueueToRoot any
	}{
		{
			name:         "dequeue from empty round-robin root node",
			rootNodeType: roundRobin,
		},
		{
			name:         "dequeue from empty shuffle-shard root node",
			rootNodeType: shuffleShard,
		},
		{
			name:          "dequeue from non-empty round-robin root node",
			rootNodeType:  roundRobin,
			enqueueToRoot: "something-in-root",
		},
		{
			name:          "dequeue from non-empty shuffle-shard root node",
			rootNodeType:  shuffleShard,
			enqueueToRoot: "something-else-in-root",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree([]NodeType{tt.rootNodeType}, &ShuffleShardState{map[TenantID]map[QuerierID]struct{}{}, nil})
			require.NoError(t, err)
			if tt.enqueueToRoot != nil {
				err = tree.EnqueueBackByPath(QueuePath{}, tt.enqueueToRoot)
				require.NoError(t, err)
			}
			path, v := tree.Dequeue()
			require.Equal(t, QueuePath{"root"}, path)
			require.Equal(t, tt.enqueueToRoot, v)

		})
	}
}

func Test_RoundRobinDequeue(t *testing.T) {
	tests := []struct {
		name              string
		nodesByDepth      []NodeType
		selfQueueObjects  []string
		childQueueObjects map[string][]any
		grandchildren     map[string]struct {
			path QueuePath
			objs []any
		}
		numDequeuesToExpected int
		expected              string
	}{
		{
			name:                  "dequeue from round-robin child when local queue empty",
			nodesByDepth:          []NodeType{roundRobin, roundRobin},
			childQueueObjects:     map[string][]any{"child-1": {"child-1:some-object"}},
			numDequeuesToExpected: 1,
			expected:              "child-1:some-object",
		},
		{
			name:                  "dequeue from round-robin root when on node's turn",
			nodesByDepth:          []NodeType{roundRobin, roundRobin},
			selfQueueObjects:      []string{"root:object-1", "root:object-2"},
			childQueueObjects:     map[string][]any{"child-1": {"child-1:object-1"}},
			numDequeuesToExpected: 2,
			expected:              "child-1:object-1",
		},
		{
			name:                  "dequeue from second round-robin child when first child is empty",
			nodesByDepth:          []NodeType{roundRobin, roundRobin},
			childQueueObjects:     map[string][]any{"child-1": {nil}, "child-2": {"child-2:some-object"}},
			numDequeuesToExpected: 1,
			expected:              "child-2:some-object",
		},
		{
			name:                  "dequeue from round-robin grandchild when non-empty",
			nodesByDepth:          []NodeType{roundRobin, roundRobin, roundRobin},
			childQueueObjects:     map[string][]any{"child-1": {"child-1:object-1", "child-1:object-2"}, "child-2": {"child-2:object-1"}},
			numDequeuesToExpected: 3,
			expected:              "grandchild-1:object-1",
			grandchildren: map[string]struct {
				path QueuePath
				objs []any
			}{"grandchild-1": {path: QueuePath{"child-1"}, objs: []any{"grandchild-1:object-1"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree, err := NewTree(tt.nodesByDepth, &ShuffleShardState{map[TenantID]map[QuerierID]struct{}{}, nil})
			require.NoError(t, err)

			for _, sqo := range tt.selfQueueObjects {
				err = tree.EnqueueBackByPath(QueuePath{}, sqo)
				require.NoError(t, err)
			}
			for childName, objs := range tt.childQueueObjects {
				for _, obj := range objs {
					err = tree.EnqueueBackByPath(QueuePath{childName}, obj)
				}
			}

			for grandchildName, grandchildData := range tt.grandchildren {
				for _, obj := range grandchildData.objs {
					err = tree.EnqueueBackByPath(append(grandchildData.path, grandchildName), obj)
					require.NoError(t, err)
				}
			}

			var finalValue any
			for i := 0; i < tt.numDequeuesToExpected; i++ {
				_, finalValue = tree.Dequeue()
			}
			v, ok := finalValue.(string)
			require.True(t, ok)
			require.Equal(t, tt.expected, v)
		})
	}
}

func Test_ShuffleShardDequeue(t *testing.T) {
	type enqueueObj struct {
		obj  any
		path QueuePath
	}
	tests := []struct {
		name         string
		nodesByDepth []NodeType
		tqMap        map[TenantID]map[QuerierID]struct{}
		currQuerier  []QuerierID
		enqueueObjs  []enqueueObj
		expected     []any
		expectErr    bool
	}{
		{
			name:         "happy path - tenant found in tenant-querier map under first child",
			nodesByDepth: []NodeType{roundRobin, shuffleShard, roundRobin},
			tqMap:        map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
			currQuerier:  []QuerierID{"querier-1"},
			enqueueObjs:  []enqueueObj{{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}}},
			expected:     []any{"query-1"},
		},
		{
			name:         "tenant exists, but not for querier",
			nodesByDepth: []NodeType{roundRobin, shuffleShard, roundRobin},
			tqMap:        map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}},
			currQuerier:  []QuerierID{"querier-1"},
			enqueueObjs:  []enqueueObj{{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}}},
			expected:     []any{nil},
		},
		{
			name:         "1 of 3 tenants exist for querier",
			nodesByDepth: []NodeType{roundRobin, shuffleShard, roundRobin},
			tqMap:        map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
			currQuerier:  []QuerierID{"querier-1"},
			enqueueObjs:  []enqueueObj{{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}}, {obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}}, {obj: "query-3", path: QueuePath{"query-component-1", "tenant-3"}}},
			expected:     []any{"query-3"},
		},
		{
			name:         "tenant exists for querier on next parent node",
			nodesByDepth: []NodeType{roundRobin, shuffleShard, roundRobin},
			tqMap:        map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-2": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
			currQuerier:  []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
				{obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}},
				{obj: "query-3", path: QueuePath{"query-component-2", "tenant-3"}},
				{obj: "query-4", path: QueuePath{"query-component-1", "tenant-3"}},
			},
			expected: []any{"query-4", "query-3", nil},
		},
		{
			name:         "2 of 3 tenants exist for querier",
			nodesByDepth: []NodeType{roundRobin, shuffleShard, roundRobin},
			tqMap:        map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}, "tenant-2": {"querier-2": {}}, "tenant-3": {"querier-1": {}}},
			currQuerier:  []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"query-component-1", "tenant-1"}},
				{obj: "query-2", path: QueuePath{"query-component-1", "tenant-2"}},
				{obj: "query-3", path: QueuePath{"query-component-2", "tenant-3"}},
			},
			expected: []any{"query-1", "query-3", nil},
		},
		{
			name:         "root node is shuffle-shard node",
			nodesByDepth: []NodeType{shuffleShard, roundRobin, roundRobin},
			tqMap:        map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
			currQuerier:  []QuerierID{"querier-1", "querier-1", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-2", "query-component-1"}},
				{obj: "query-2", path: QueuePath{"tenant-1", "query-component-2"}},
				{obj: "query-3", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{"query-2", "query-3", nil},
		},
		{
			name:         "dequeueing for one querier returns nil, but does return for a different querier",
			nodesByDepth: []NodeType{shuffleShard, roundRobin, roundRobin},
			tqMap:        map[TenantID]map[QuerierID]struct{}{"tenant-1": {"querier-1": {}}},
			currQuerier:  []QuerierID{"querier-2", "querier-1"},
			enqueueObjs: []enqueueObj{
				{obj: "query-1", path: QueuePath{"tenant-1", "query-component-1"}},
			},
			expected: []any{nil, "query-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentQuerier := QuerierID("")
			tree, err := NewTree(tt.nodesByDepth, &ShuffleShardState{tt.tqMap, &currentQuerier})
			require.NoError(t, err)

			for _, o := range tt.enqueueObjs {
				err = tree.EnqueueBackByPath(o.path, o.obj)
				require.NoError(t, err)
			}
			// currQuerier at position i is used to dequeue the expected result at position i
			require.Equal(t, len(tt.currQuerier), len(tt.expected))
			for i := 0; i < len(tt.expected); i++ {
				tree.currentQuerier = &tt.currQuerier[i]
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

	state := &ShuffleShardState{
		tenantQuerierMap: tqa.tenantQuerierIDs,
		currentQuerier:   nil,
	}

	tree, err := NewTree([]NodeType{shuffleShard, roundRobin, roundRobin}, state)
	require.NoError(t, err)
	err = tree.EnqueueBackByPath(QueuePath{"tenant-1", "query-component-1"}, "query-1")
	err = tree.EnqueueBackByPath(QueuePath{"tenant-2", "query-component-1"}, "query-2")
	err = tree.EnqueueBackByPath(QueuePath{"tenant-2", "query-component-1"}, "query-3")
	require.NoError(t, err)

	querier1 := QuerierID("querier-1")
	querier2 := QuerierID("querier-2")
	querier3 := QuerierID("querier-3")

	// set state to querier-2 should dequeue query-2
	state.currentQuerier = &querier2
	_, v := tree.Dequeue()
	require.Equal(t, "query-2", v)

	// set state to querier-1 should dequeue query-1
	state.currentQuerier = &querier1
	_, v = tree.Dequeue()
	require.Equal(t, "query-1", v)

	// update tqa map to assign querier-3 to tenant-2, then set state to querier-3 should dequeue query-3
	tqa.tenantQuerierIDs["tenant-2"]["querier-3"] = struct{}{}
	state.currentQuerier = &querier3
	_, v = tree.Dequeue()
	require.Equal(t, "query-3", v)

}
