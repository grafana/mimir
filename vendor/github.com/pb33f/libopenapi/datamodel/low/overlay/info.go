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

// Info represents a low-level Overlay Info Object.
// https://spec.openapis.org/overlay/v1.0.0#info-object
type Info struct {
	Title      low.NodeReference[string]
	Version    low.NodeReference[string]
	Extensions *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]
	KeyNode    *yaml.Node
	RootNode   *yaml.Node
	index      *index.SpecIndex
	context    context.Context
	*low.Reference
	low.NodeMap
}

// GetIndex returns the index.SpecIndex instance attached to the Info object
func (i *Info) GetIndex() *index.SpecIndex {
	return i.index
}

// GetContext returns the context.Context instance used when building the Info object
func (i *Info) GetContext() context.Context {
	return i.context
}

// FindExtension returns a ValueReference containing the extension value, if found.
func (i *Info) FindExtension(ext string) *low.ValueReference[*yaml.Node] {
	return low.FindItemInOrderedMap(ext, i.Extensions)
}

// GetRootNode returns the root yaml node of the Info object
func (i *Info) GetRootNode() *yaml.Node {
	return i.RootNode
}

// GetKeyNode returns the key yaml node of the Info object
func (i *Info) GetKeyNode() *yaml.Node {
	return i.KeyNode
}

// Build will extract extensions for the Info object.
func (i *Info) Build(ctx context.Context, keyNode, root *yaml.Node, idx *index.SpecIndex) error {
	i.KeyNode = keyNode
	root = utils.NodeAlias(root)
	i.RootNode = root
	utils.CheckForMergeNodes(root)
	i.Reference = new(low.Reference)
	i.Nodes = low.ExtractNodes(ctx, root)
	i.Extensions = low.ExtractExtensions(root)
	i.index = idx
	i.context = ctx
	low.ExtractExtensionNodes(ctx, i.Extensions, i.Nodes)
	return nil
}

// GetExtensions returns all Info extensions and satisfies the low.HasExtensions interface.
func (i *Info) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return i.Extensions
}

// Hash will return a consistent SHA256 Hash of the Info object
func (i *Info) Hash() [32]byte {
	sb := low.GetStringBuilder()
	defer low.PutStringBuilder(sb)

	if !i.Title.IsEmpty() {
		sb.WriteString(i.Title.Value)
		sb.WriteByte('|')
	}
	if !i.Version.IsEmpty() {
		sb.WriteString(i.Version.Value)
		sb.WriteByte('|')
	}
	for _, ext := range low.HashExtensions(i.Extensions) {
		sb.WriteString(ext)
		sb.WriteByte('|')
	}
	return sha256.Sum256([]byte(sb.String()))
}
