// SPDX-License-Identifier: MPL-2.0

package memberlist

// NodeSelectionDelegate is an optional delegate that can be used to filter and prioritize
// nodes for gossip, probing, and push/pull operations. This allows implementing custom
// routing logic, such as zone-aware or rack-aware gossiping.
type NodeSelectionDelegate interface {
	// SelectNode is called for each node to determine:
	// - selected: whether the node should be included in the selection pool
	// - preferred: whether the node should be prioritized (at least one preferred node is selected)
	SelectNode(Node) (selected, preferred bool)
}
