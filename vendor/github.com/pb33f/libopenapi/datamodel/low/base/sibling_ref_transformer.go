// Copyright 2025 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package base

import (
	"sort"

	"github.com/pb33f/go-yaml"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/utils"
)

// SiblingRefTransformer handles transformation of schemas with sibling properties alongside $ref
// into OpenAPI 3.1 compliant allOf structures
type SiblingRefTransformer struct {
	index *index.SpecIndex
}

type transformedSiblingRef struct {
	allOfNode     *yaml.Node
	siblingNode   *yaml.Node
	referenceNode *yaml.Node
	reference     string
}

// NewSiblingRefTransformer creates a new transformer instance
func NewSiblingRefTransformer(idx *index.SpecIndex) *SiblingRefTransformer {
	return &SiblingRefTransformer{
		index: idx,
	}
}

// TransformSiblingRef transforms a node with $ref and sibling properties into an allOf structure
// Example transformation:
//
//	Input:  {title: "MySchema", $ref: "#/components/schemas/Base"}
//	Output: {allOf: [{title: "MySchema"}, {$ref: "#/components/schemas/Base"}]}
//
// unevaluatedProperties and unevaluatedItems stay beside allOf rather than moving into it,
// because they only see what adjacent keywords evaluated, and that includes the $ref target:
//
//	Input:  {$ref: "#/components/schemas/Base", unevaluatedProperties: false}
//	Output: {allOf: [{$ref: "#/components/schemas/Base"}], unevaluatedProperties: false}
//
// $id, $schema, $vocabulary, $anchor, $dynamicAnchor, $defs and definitions also stay beside
// allOf, so the wrapper keeps the identity of the authored object: $id sets the base URI the $ref
// resolves against, $schema names the dialect and $vocabulary the vocabularies it uses, $anchor
// and $dynamicAnchor name the whole object, and $defs and definitions stay at the JSON pointers
// they were written at.
// The other siblings move under allOf/0, so a JSON pointer that reached one of them, such as
// ".../thing#/properties/x", no longer does.
//
//	Input:  {$id: "https://example.com/thing", $ref: "base", title: "Thing"}
//	Output: {allOf: [{title: "Thing"}, {$ref: "base"}], $id: "https://example.com/thing"}
func (srt *SiblingRefTransformer) TransformSiblingRef(node *yaml.Node) (*yaml.Node, error) {
	transformed := srt.transformSiblingRefWithMetadata(node)
	if transformed == nil {
		return node, nil // no transformation needed
	}
	return transformed.allOfNode, nil
}

func (srt *SiblingRefTransformer) transformSiblingRefWithMetadata(node *yaml.Node) *transformedSiblingRef {
	if srt.index == nil || srt.index.GetConfig() == nil || !srt.index.GetConfig().TransformSiblingRefs {
		return nil
	}
	siblings, refValue := srt.ExtractSiblingProperties(node)
	if len(siblings) == 0 || refValue == "" {
		return nil
	}
	siblingNode := srt.createSiblingSchemaNode(node)
	return &transformedSiblingRef{
		allOfNode:     srt.createAllOfStructureWithSiblingNode(refValue, siblingNode),
		siblingNode:   siblingNode,
		referenceNode: node,
		reference:     refValue,
	}
}

// CreateAllOfStructure creates an allOf node structure from ref value and sibling properties
func (srt *SiblingRefTransformer) CreateAllOfStructure(refValue string, siblings map[string]*yaml.Node) *yaml.Node {
	var siblingSchemaNode *yaml.Node
	if len(siblings) > 0 {
		siblingSchemaNode = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		keys := make([]string, 0, len(siblings))
		for key := range siblings {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			valueNode := siblings[key]
			keyNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}
			copiedValueNode := utils.CloneYAMLNode(valueNode)
			siblingSchemaNode.Content = append(siblingSchemaNode.Content, keyNode, copiedValueNode)
		}
	}
	return srt.createAllOfStructureWithSiblingNode(refValue, siblingSchemaNode)
}

func (srt *SiblingRefTransformer) createAllOfStructureWithSiblingNode(refValue string, siblingSchemaNode *yaml.Node) *yaml.Node {
	allOfNode := &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  "!!map",
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "allOf"},
			{Kind: yaml.SequenceNode, Tag: "!!seq", Content: []*yaml.Node{}},
		},
	}

	allOfArrayNode := allOfNode.Content[1]

	// keywords that stay beside allOf go on the wrapper and the rest form the allOf member. The
	// sibling node itself is the member until a keyword moves, so the member pairs are only copied
	// when one does.
	memberNode := siblingSchemaNode
	if siblingSchemaNode != nil {
		var kept []*yaml.Node
		moved := false
		for i := 0; i+1 < len(siblingSchemaNode.Content); i += 2 {
			keyNode, valueNode := siblingSchemaNode.Content[i], siblingSchemaNode.Content[i+1]
			if !staysBesideAllOf(keyNode.Value) {
				if moved {
					kept = append(kept, keyNode, valueNode)
				}
				continue
			}
			if !moved {
				moved = true
				kept = append(make([]*yaml.Node, 0, len(siblingSchemaNode.Content)-2), siblingSchemaNode.Content[:i]...)
			}
			allOfNode.Content = append(allOfNode.Content, keyNode, valueNode)
		}
		if moved {
			memberNode = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: kept}
		}
	}

	if memberNode != nil && len(memberNode.Content) > 0 {
		allOfArrayNode.Content = append(allOfArrayNode.Content, memberNode)
	}

	refSchemaNode := &yaml.Node{
		Kind: yaml.MappingNode,
		Tag:  "!!map",
		Content: []*yaml.Node{
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: "$ref"},
			{Kind: yaml.ScalarNode, Tag: "!!str", Value: refValue},
		},
	}
	allOfArrayNode.Content = append(allOfArrayNode.Content, refSchemaNode)

	return allOfNode
}

// staysBesideAllOf reports whether a sibling keyword stays beside allOf instead of moving into it.
// unevaluatedProperties and unevaluatedItems only see what adjacent keywords evaluated, so they
// stay next to the $ref. $id sets the base URI the $ref resolves against, $schema names the
// dialect of the object and $vocabulary the vocabularies that dialect uses, $anchor and
// $dynamicAnchor name the whole object, and $defs and definitions hold subschemas that JSON
// pointers reach through the object.
//
// definitions is written out because the v2 package, which defines its label, imports this one.
func staysBesideAllOf(key string) bool {
	switch key {
	case UnevaluatedPropertiesLabel, UnevaluatedItemsLabel, IdLabel, SchemaTypeLabel, VocabularyLabel,
		AnchorLabel, DynamicAnchorLabel, DefsLabel, "definitions":
		return true
	}
	return false
}

func (srt *SiblingRefTransformer) createSiblingSchemaNode(node *yaml.Node) *yaml.Node {
	if !utils.IsNodeMap(node) {
		return nil
	}
	siblingNode := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		valueNode := node.Content[i+1]
		if keyNode == nil || keyNode.Value == "$ref" {
			continue
		}
		siblingNode.Content = append(siblingNode.Content, utils.CloneYAMLNode(keyNode), utils.CloneYAMLNode(valueNode))
	}
	return siblingNode
}

// ExtractSiblingProperties extracts sibling properties from a node containing $ref
// returns a map of sibling properties and the $ref value
func (srt *SiblingRefTransformer) ExtractSiblingProperties(node *yaml.Node) (map[string]*yaml.Node, string) {
	if !utils.IsNodeMap(node) || len(node.Content) < 4 { // need at least $ref + one sibling
		return nil, ""
	}

	siblings := make(map[string]*yaml.Node)
	var refValue string

	for i := 0; i < len(node.Content); i += 2 {
		if i+1 >= len(node.Content) {
			break
		}

		keyNode := node.Content[i]
		valueNode := node.Content[i+1]

		if keyNode.Value == "$ref" {
			refValue = valueNode.Value
		} else {
			siblings[keyNode.Value] = valueNode
		}
	}

	if refValue == "" || len(siblings) == 0 {
		return nil, ""
	}

	return siblings, refValue
}

// ShouldTransform determines if a node should be transformed based on configuration and content
func (srt *SiblingRefTransformer) ShouldTransform(node *yaml.Node) bool {
	if srt.index == nil || srt.index.GetConfig() == nil {
		return false
	}

	if !srt.index.GetConfig().TransformSiblingRefs {
		return false
	}

	siblings, refValue := srt.ExtractSiblingProperties(node)
	return len(siblings) > 0 && refValue != ""
}
