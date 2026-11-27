// Copyright 2022-2026 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package v3

import (
	"context"
	"fmt"
	"hash/maphash"

	"github.com/pb33f/go-yaml"
	"github.com/pb33f/libopenapi/datamodel/low"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/orderedmap"
	"github.com/pb33f/libopenapi/utils"
)

// Encoding represents a low-level OpenAPI 3+ Encoding object
//   - https://spec.openapis.org/oas/v3.1.0#encoding-object
type Encoding struct {
	ContentType   low.NodeReference[string]
	Headers       low.NodeReference[*orderedmap.Map[low.KeyReference[string], low.ValueReference[*Header]]]
	Style         low.NodeReference[string]
	Explode       low.NodeReference[bool]
	AllowReserved low.NodeReference[bool]

	// Encoding, PrefixEncoding and ItemEncoding describe nested multipart parts (OpenAPI 3.2+).
	Encoding       low.NodeReference[*orderedmap.Map[low.KeyReference[string], low.ValueReference[*Encoding]]]
	PrefixEncoding low.NodeReference[[]low.ValueReference[*Encoding]]
	ItemEncoding   low.NodeReference[*Encoding]
	Extensions     *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]

	KeyNode   *yaml.Node
	RootNode  *yaml.Node
	index     *index.SpecIndex
	context   context.Context
	nodeStore low.NodeLines
	reference low.Reference
	*low.Reference
	low.NodeMap
}

// GetIndex returns the index.SpecIndex instance attached to the Encoding object
func (en *Encoding) GetIndex() *index.SpecIndex {
	return en.index
}

// GetContext returns the context.Context instance used when building the Encoding object
func (en *Encoding) GetContext() context.Context {
	return en.context
}

// FindHeader attempts to locate a Header with the supplied name
func (en *Encoding) FindHeader(hType string) *low.ValueReference[*Header] {
	return low.FindItemInOrderedMap[*Header](hType, en.Headers.Value)
}

// FindExtension attempts to locate an extension with the supplied name
func (en *Encoding) FindExtension(ext string) *low.ValueReference[*yaml.Node] {
	return low.FindItemInOrderedMap(ext, en.Extensions)
}

// GetExtensions returns all Encoding extensions and satisfies the low.HasExtensions interface.
func (en *Encoding) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return en.Extensions
}

// GetRootNode returns the root yaml node of the Encoding object
func (en *Encoding) GetRootNode() *yaml.Node {
	return en.RootNode
}

// GetKeyNode returns the key yaml node of the Encoding object
func (en *Encoding) GetKeyNode() *yaml.Node {
	return en.KeyNode
}

// Hash will return a consistent Hash of the Encoding object
func (en *Encoding) Hash() uint64 {
	return low.WithHasher(func(h *maphash.Hash) uint64 {
		if en.ContentType.Value != "" {
			low.HashString(h, "contentType", en.ContentType.Value)
		}
		for k, v := range orderedmap.SortAlpha(en.Headers.Value).FromOldest() {
			h.WriteString(fmt.Sprintf("%s-%x", k.Value, v.Value.Hash()))
			h.WriteByte(low.HASH_PIPE)
		}
		if en.Style.Value != "" {
			low.HashString(h, "style", en.Style.Value)
		}
		// explode's default depends on style, so an explicit value is hashed apart from an absent one.
		if !en.Explode.IsEmpty() {
			low.HashLabel(h, "explode")
			low.HashBool(h, en.Explode.Value)
			h.WriteByte(low.HASH_PIPE)
		}
		// a present allowReserved, like style and explode, switches a part to RFC6570 serialization.
		if !en.AllowReserved.IsEmpty() {
			low.HashLabel(h, "allowReserved")
			low.HashBool(h, en.AllowReserved.Value)
			h.WriteByte(low.HASH_PIPE)
		}
		low.HashMap(h, "encoding", en.Encoding.Value)
		hashEncodingFields(h, en.PrefixEncoding, en.ItemEncoding.Value)
		for _, ext := range low.HashExtensions(en.Extensions) {
			h.WriteString(ext)
			h.WriteByte(low.HASH_PIPE)
		}
		return h.Sum64()
	})
}

// Build will extract all Header objects from supplied node.
func (en *Encoding) Build(ctx context.Context, keyNode, root *yaml.Node, idx *index.SpecIndex) error {
	en.KeyNode = keyNode
	root = utils.ResolveMergeNodes(root)
	en.RootNode = root
	en.nodeStore = low.NodeLines{}
	en.Nodes = &en.nodeStore
	if len(root.Content) > 0 {
		en.NodeMap.ExtractNodes(root, false)
	} else {
		en.AddNode(root.Line, root)
	}
	en.reference = low.Reference{}
	en.Reference = &en.reference
	en.index = idx
	en.context = ctx
	en.Extensions = low.ExtractExtensions(root)
	low.ExtractExtensionNodes(ctx, en.Extensions, en.Nodes)

	// headers is read only from this mapping, so a nested part's headers are not copied onto this one.
	_, hL, hN := utils.FindKeyNodeFullTop(HeadersLabel, root.Content)
	if hN != nil {
		headers, err := low.ExtractMapNoLookupExtensions[*Header](ctx, hN, idx, true)
		if err != nil {
			return err
		}
		en.Headers = low.NodeReference[*orderedmap.Map[low.KeyReference[string], low.ValueReference[*Header]]]{
			Value:     headers,
			KeyNode:   hL,
			ValueNode: hN,
		}
		en.Nodes.Store(hL.Line, hL)
		for k, v := range headers.FromOldest() {
			v.Value.Nodes.Store(k.KeyNode.Line, k.KeyNode)
		}
	}
	return extractEncodingFields(ctx, root, idx, en.Nodes, &en.Encoding, &en.PrefixEncoding, &en.ItemEncoding)
}

// FindEncoding attempts to locate a nested Encoding with the supplied property name.
func (en *Encoding) FindEncoding(eType string) *low.ValueReference[*Encoding] {
	return low.FindItemInOrderedMap[*Encoding](eType, en.Encoding.Value)
}

// extractEncodingFields builds the encoding, prefixEncoding and itemEncoding fields shared by the
// Media Type Object and (for nested multipart parts) the Encoding Object. Each field is read only from the
// keys of root itself, so fields belonging to a nested part, or appearing inside an example, are left alone.
func extractEncodingFields(ctx context.Context, root *yaml.Node, idx *index.SpecIndex, nodes *low.NodeLines,
	encoding *low.NodeReference[*orderedmap.Map[low.KeyReference[string], low.ValueReference[*Encoding]]],
	prefixEncoding *low.NodeReference[[]low.ValueReference[*Encoding]],
	itemEncoding *low.NodeReference[*Encoding],
) error {
	// encoding keys are schema property names, so names starting with x- are kept.
	_, encsL, encsN := utils.FindKeyNodeFullTop(EncodingLabel, root.Content)
	if encsN != nil {
		encs, err := low.ExtractMapNoLookupExtensions[*Encoding](ctx, encsN, idx, true)
		if err != nil {
			return err
		}
		*encoding = low.NodeReference[*orderedmap.Map[low.KeyReference[string], low.ValueReference[*Encoding]]]{
			Value:     encs,
			KeyNode:   encsL,
			ValueNode: encsN,
		}
		nodes.Store(encsL.Line, encsL)
		for k, v := range encs.FromOldest() {
			v.Value.Nodes.Store(k.KeyNode.Line, k.KeyNode)
		}
	}

	prefix, prefixL, prefixN, err := low.ExtractArray[*Encoding](ctx, PrefixEncodingLabel, root, idx)
	if err != nil {
		return err
	}
	if prefixL != nil {
		*prefixEncoding = low.NodeReference[[]low.ValueReference[*Encoding]]{
			Value:     prefix,
			KeyNode:   prefixL,
			ValueNode: prefixN,
		}
		nodes.Store(prefixL.Line, prefixL)
	}

	_, itemL, itemN := utils.FindKeyNodeFullTop(ItemEncodingLabel, root.Content)
	if itemN != nil {
		item, err, _, _ := low.ExtractObjectRaw[*Encoding](ctx, itemL, itemN, idx)
		if err != nil {
			return err
		}
		*itemEncoding = low.NodeReference[*Encoding]{
			Value:     item,
			KeyNode:   itemL,
			ValueNode: itemN,
		}
		itemEncoding.SetReference(item.GetReference(), item.GetReferenceNode())
		nodes.Store(itemL.Line, itemL)
	}
	return nil
}

// hashEncodingFields hashes prefixEncoding in order, because each entry applies to the part at its position,
// then itemEncoding. A present but empty prefixEncoding hashes differently from an absent one.
func hashEncodingFields(h *maphash.Hash, prefix low.NodeReference[[]low.ValueReference[*Encoding]], item *Encoding) {
	if !prefix.IsEmpty() {
		low.HashLabel(h, "prefixEncoding")
		h.WriteByte(low.HASH_PIPE)
	}
	for i := range prefix.Value {
		low.HashString(h, "prefixEncoding", low.GenerateHashString(prefix.Value[i].Value))
	}
	if item != nil {
		low.HashString(h, "itemEncoding", low.GenerateHashString(item))
	}
}
