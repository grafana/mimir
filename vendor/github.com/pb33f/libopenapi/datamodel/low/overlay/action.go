// Copyright 2022-2025 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package overlay

import (
	"context"
	"crypto/sha256"

	"github.com/pb33f/libopenapi/datamodel/low"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/orderedmap"
	"github.com/pb33f/libopenapi/utils"
	"go.yaml.in/yaml/v4"
)

// Action represents a low-level Overlay Action Object.
// https://spec.openapis.org/overlay/v1.0.0#action-object
type Action struct {
	Target      low.NodeReference[string]
	Description low.NodeReference[string]
	Update      low.NodeReference[*yaml.Node]
	Remove      low.NodeReference[bool]
	Extensions  *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]
	KeyNode     *yaml.Node
	RootNode    *yaml.Node
	index       *index.SpecIndex
	context     context.Context
	*low.Reference
	low.NodeMap
}

// GetIndex returns the index.SpecIndex instance attached to the Action object
func (a *Action) GetIndex() *index.SpecIndex {
	return a.index
}

// GetContext returns the context.Context instance used when building the Action object
func (a *Action) GetContext() context.Context {
	return a.context
}

// FindExtension returns a ValueReference containing the extension value, if found.
func (a *Action) FindExtension(ext string) *low.ValueReference[*yaml.Node] {
	return low.FindItemInOrderedMap(ext, a.Extensions)
}

// GetRootNode returns the root yaml node of the Action object
func (a *Action) GetRootNode() *yaml.Node {
	return a.RootNode
}

// GetKeyNode returns the key yaml node of the Action object
func (a *Action) GetKeyNode() *yaml.Node {
	return a.KeyNode
}

// Build will extract extensions for the Action object.
func (a *Action) Build(ctx context.Context, keyNode, root *yaml.Node, idx *index.SpecIndex) error {
	a.KeyNode = keyNode
	root = utils.NodeAlias(root)
	a.RootNode = root
	utils.CheckForMergeNodes(root)
	a.Reference = new(low.Reference)
	a.Nodes = low.ExtractNodes(ctx, root)
	a.Extensions = low.ExtractExtensions(root)
	a.index = idx
	a.context = ctx
	low.ExtractExtensionNodes(ctx, a.Extensions, a.Nodes)

	// Extract the update node directly if present
	for i := 0; i < len(root.Content); i += 2 {
		if i+1 < len(root.Content) && root.Content[i].Value == UpdateLabel {
			a.Update = low.NodeReference[*yaml.Node]{
				Value:     root.Content[i+1],
				KeyNode:   root.Content[i],
				ValueNode: root.Content[i+1],
			}
			break
		}
	}
	return nil
}

// GetExtensions returns all Action extensions and satisfies the low.HasExtensions interface.
func (a *Action) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return a.Extensions
}

// Hash will return a consistent SHA256 Hash of the Action object
func (a *Action) Hash() [32]byte {
	sb := low.GetStringBuilder()
	defer low.PutStringBuilder(sb)

	if !a.Target.IsEmpty() {
		sb.WriteString(a.Target.Value)
		sb.WriteByte('|')
	}
	if !a.Description.IsEmpty() {
		sb.WriteString(a.Description.Value)
		sb.WriteByte('|')
	}
	if !a.Update.IsEmpty() {
		sb.WriteString(low.GenerateHashString(a.Update.Value))
		sb.WriteByte('|')
	}
	if !a.Remove.IsEmpty() {
		if a.Remove.Value {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
		sb.WriteByte('|')
	}
	for _, ext := range low.HashExtensions(a.Extensions) {
		sb.WriteString(ext)
		sb.WriteByte('|')
	}
	return sha256.Sum256([]byte(sb.String()))
}
