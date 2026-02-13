// Copyright 2022 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package base

import (
	"context"
	"crypto/sha256"

	"github.com/pb33f/libopenapi/datamodel/low"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/orderedmap"
	"go.yaml.in/yaml/v4"
)

// Contact represents a low-level representation of the Contact definitions found at
//
//	v2 - https://swagger.io/specification/v2/#contactObject
//	v3 - https://spec.openapis.org/oas/v3.1.0#contact-object
type Contact struct {
	Name       low.NodeReference[string]
	URL        low.NodeReference[string]
	Email      low.NodeReference[string]
	Extensions *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]
	KeyNode    *yaml.Node
	RootNode   *yaml.Node
	index      *index.SpecIndex
	context    context.Context
	*low.Reference
	low.NodeMap
}

func (c *Contact) Build(ctx context.Context, keyNode, root *yaml.Node, idx *index.SpecIndex) error {
	c.KeyNode = keyNode
	c.RootNode = root
	c.Reference = new(low.Reference)
	c.Nodes = low.ExtractNodes(ctx, root)
	c.Extensions = low.ExtractExtensions(root)
	c.context = ctx
	c.index = idx
	return nil
}

// GetIndex will return the index.SpecIndex instance attached to the Contact object
func (c *Contact) GetIndex() *index.SpecIndex {
	return c.index
}

// GetContext will return the context.Context instance used when building the Contact object
func (c *Contact) GetContext() context.Context {
	return c.context
}

// GetRootNode will return the root yaml node of the Contact object
func (c *Contact) GetRootNode() *yaml.Node {
	return c.RootNode
}

// GetKeyNode will return the key yaml node of the Contact object
func (c *Contact) GetKeyNode() *yaml.Node {
	return c.KeyNode
}

// Hash will return a consistent SHA256 Hash of the Contact object
func (c *Contact) Hash() [32]byte {
	// Use string builder pool
	sb := low.GetStringBuilder()
	defer low.PutStringBuilder(sb)

	if !c.Name.IsEmpty() {
		sb.WriteString(c.Name.Value)
		sb.WriteByte('|')
	}
	if !c.URL.IsEmpty() {
		sb.WriteString(c.URL.Value)
		sb.WriteByte('|')
	}
	if !c.Email.IsEmpty() {
		sb.WriteString(c.Email.Value)
		sb.WriteByte('|')
	}

	// Note: Extensions are not included in the hash for Contact
	return sha256.Sum256([]byte(sb.String()))
}

// GetExtensions returns all extensions for Contact
func (c *Contact) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return c.Extensions
}
