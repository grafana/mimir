// Copyright 2022 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package base

import (
	"context"
	"crypto/sha256"

	"github.com/pb33f/libopenapi/datamodel/low"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/orderedmap"
	"github.com/pb33f/libopenapi/utils"
	"go.yaml.in/yaml/v4"
)

// Tag represents a low-level Tag instance that is backed by a low-level one.
//
// Adds metadata to a single tag that is used by the Operation Object. It is not mandatory to have a Tag Object per
// tag defined in the Operation Object instances.
//   - v2: https://swagger.io/specification/v2/#tagObject
//   - v3: https://swagger.io/specification/#tag-object
//   - v3.2: https://spec.openapis.org/oas/v3.2.0#tag-object
type Tag struct {
	Name         low.NodeReference[string]
	Summary      low.NodeReference[string]
	Description  low.NodeReference[string]
	ExternalDocs low.NodeReference[*ExternalDoc]
	Parent       low.NodeReference[string]
	Kind         low.NodeReference[string]
	Extensions   *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]
	KeyNode      *yaml.Node
	RootNode     *yaml.Node
	index        *index.SpecIndex
	context      context.Context
	*low.Reference
	low.NodeMap
}

// GetIndex returns the index.SpecIndex instance attached to the Tag object
func (t *Tag) GetIndex() *index.SpecIndex {
	return t.index
}

// GetContext returns the context.Context instance used when building the Tag object
func (t *Tag) GetContext() context.Context {
	return t.context
}

// FindExtension returns a ValueReference containing the extension value, if found.
func (t *Tag) FindExtension(ext string) *low.ValueReference[*yaml.Node] {
	return low.FindItemInOrderedMap(ext, t.Extensions)
}

// GetRootNode returns the root yaml node of the Tag object
func (t *Tag) GetRootNode() *yaml.Node {
	return t.RootNode
}

// GetKeyNode returns the key yaml node of the Tag object
func (t *Tag) GetKeyNode() *yaml.Node {
	return t.KeyNode
}

// Build will extract extensions and external docs for the Tag.
func (t *Tag) Build(ctx context.Context, keyNode, root *yaml.Node, idx *index.SpecIndex) error {
	t.KeyNode = keyNode
	root = utils.NodeAlias(root)
	t.RootNode = root
	utils.CheckForMergeNodes(root)
	t.Reference = new(low.Reference)
	t.Nodes = low.ExtractNodes(ctx, root)
	t.Extensions = low.ExtractExtensions(root)
	t.index = idx
	t.context = ctx

	low.ExtractExtensionNodes(ctx, t.Extensions, t.Nodes)

	// extract externalDocs
	extDocs, err := low.ExtractObject[*ExternalDoc](ctx, ExternalDocsLabel, root, idx)
	t.ExternalDocs = extDocs
	return err
}

// GetExtensions returns all Tag extensions and satisfies the low.HasExtensions interface.
func (t *Tag) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return t.Extensions
}

// Hash will return a consistent SHA256 Hash of the Tag object
func (t *Tag) Hash() [32]byte {
	// Pre-calculate field count for optimal allocation
	fieldCount := 0
	if !t.Name.IsEmpty() {
		fieldCount++
	}
	if !t.Summary.IsEmpty() {
		fieldCount++
	}
	if !t.Description.IsEmpty() {
		fieldCount++
	}
	if !t.ExternalDocs.IsEmpty() {
		fieldCount++
	}
	if !t.Parent.IsEmpty() {
		fieldCount++
	}
	if !t.Kind.IsEmpty() {
		fieldCount++
	}

	// Use string builder pool
	sb := low.GetStringBuilder()
	defer low.PutStringBuilder(sb)

	if !t.Name.IsEmpty() {
		sb.WriteString(t.Name.Value)
		sb.WriteByte('|')
	}
	if !t.Summary.IsEmpty() {
		sb.WriteString(t.Summary.Value)
		sb.WriteByte('|')
	}
	if !t.Description.IsEmpty() {
		sb.WriteString(t.Description.Value)
		sb.WriteByte('|')
	}
	if !t.ExternalDocs.IsEmpty() {
		sb.WriteString(low.GenerateHashString(t.ExternalDocs.Value))
		sb.WriteByte('|')
	}
	if !t.Parent.IsEmpty() {
		sb.WriteString(t.Parent.Value)
		sb.WriteByte('|')
	}
	if !t.Kind.IsEmpty() {
		sb.WriteString(t.Kind.Value)
		sb.WriteByte('|')
	}
	for _, ext := range low.HashExtensions(t.Extensions) {
		sb.WriteString(ext)
		sb.WriteByte('|')
	}
	return sha256.Sum256([]byte(sb.String()))
}
