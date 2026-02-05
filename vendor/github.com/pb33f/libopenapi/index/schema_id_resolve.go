// Copyright 2022-2025 Princess Beef Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package index

import (
	"fmt"
	"net/url"
	"strings"

	"go.yaml.in/yaml/v4"
)

// ValidateSchemaId checks if a $id value is valid per JSON Schema 2020-12 spec.
// Per the spec, $id MUST NOT contain a fragment identifier (#).
func ValidateSchemaId(id string) error {
	if id == "" {
		return fmt.Errorf("$id cannot be empty")
	}
	if strings.Contains(id, "#") {
		return fmt.Errorf("$id must not contain fragment identifier '#': %s (use $anchor instead)", id)
	}
	return nil
}

// ResolveSchemaId resolves a potentially relative $id against a base URI.
// Returns the fully resolved absolute URI.
func ResolveSchemaId(id string, baseUri string) (string, error) {
	if id == "" {
		return "", fmt.Errorf("$id cannot be empty")
	}

	parsedId, err := url.Parse(id)
	if err != nil {
		return "", fmt.Errorf("invalid $id URI: %s: %w", id, err)
	}

	// Absolute $id is used directly
	if parsedId.IsAbs() {
		return id, nil
	}

	// Relative $id without base - return as-is for later resolution
	if baseUri == "" {
		return id, nil
	}

	parsedBase, err := url.Parse(baseUri)
	if err != nil {
		return "", fmt.Errorf("invalid base URI: %s: %w", baseUri, err)
	}

	resolved := parsedBase.ResolveReference(parsedId)
	return resolved.String(), nil
}

// ResolveRefAgainstSchemaId resolves a $ref value against the current $id scope.
// Absolute refs are returned as-is; relative refs are resolved against the nearest ancestor $id.
func ResolveRefAgainstSchemaId(ref string, scope *SchemaIdScope) (string, error) {
	if ref == "" {
		return "", fmt.Errorf("$ref cannot be empty")
	}

	parsedRef, err := url.Parse(ref)
	if err != nil {
		return "", fmt.Errorf("invalid $ref URI: %s: %w", ref, err)
	}

	if parsedRef.IsAbs() {
		return ref, nil
	}

	if scope == nil || scope.BaseUri == "" {
		return ref, nil
	}

	parsedBase, err := url.Parse(scope.BaseUri)
	if err != nil {
		return "", fmt.Errorf("invalid base URI in scope: %s: %w", scope.BaseUri, err)
	}

	resolved := parsedBase.ResolveReference(parsedRef)
	return resolved.String(), nil
}

// SplitRefFragment splits a reference into base URI and fragment components.
// Example: "https://example.com/schema.json#/definitions/Pet" ->
// baseUri="https://example.com/schema.json", fragment="#/definitions/Pet"
func SplitRefFragment(ref string) (baseUri string, fragment string) {
	idx := strings.Index(ref, "#")
	if idx == -1 {
		return ref, ""
	}
	return ref[:idx], ref[idx:]
}

// ResolveRefViaSchemaId attempts to resolve a $ref via the $id registry.
// Implements JSON Schema 2020-12 $id-based resolution:
// 1. Split ref into base URI and fragment
// 2. Look up base URI in $id registry
// 3. Navigate to fragment within found schema if present
// Returns nil if the ref cannot be resolved via $id.
func (index *SpecIndex) ResolveRefViaSchemaId(ref string) *Reference {
	if ref == "" {
		return nil
	}

	baseUri, fragment := SplitRefFragment(ref)

	// Local fragment refs are not $id-based
	if baseUri == "" {
		return nil
	}

	// Check local index first, then rolodex global registry
	entry := index.GetSchemaById(baseUri)
	if entry == nil && index.rolodex != nil {
		entry = index.rolodex.LookupSchemaById(baseUri)
	}

	if entry == nil {
		return nil
	}

	r := &Reference{
		FullDefinition: ref,
		Definition:     ref,
		Name:           baseUri,
		Node:           entry.SchemaNode,
		IsRemote:       entry.Index != index,
		RemoteLocation: entry.Index.GetSpecAbsolutePath(),
		Index:          entry.Index,
	}

	// Navigate to fragment if present
	if fragment != "" && entry.SchemaNode != nil {
		if fragmentNode := navigateToFragment(entry.SchemaNode, fragment); fragmentNode != nil {
			r.Node = fragmentNode
		}
	}

	return r
}

// navigateToFragment navigates to a JSON pointer fragment within a YAML node.
// Fragment format: "#/path/to/node" or "/path/to/node"
func navigateToFragment(root *yaml.Node, fragment string) *yaml.Node {
	if root == nil || fragment == "" {
		return nil
	}

	path := strings.TrimPrefix(fragment, "#")
	if path == "" || path == "/" {
		return root
	}

	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	current := root
	if current.Kind == yaml.DocumentNode && len(current.Content) > 0 {
		current = current.Content[0]
	}

	for _, segment := range segments {
		if segment == "" {
			continue
		}

		// Decode JSON pointer escapes (~1 = /, ~0 = ~)
		segment = strings.ReplaceAll(segment, "~1", "/")
		segment = strings.ReplaceAll(segment, "~0", "~")

		found := false
		if current.Kind == yaml.MappingNode {
			for i := 0; i < len(current.Content)-1; i += 2 {
				if current.Content[i].Value == segment {
					current = current.Content[i+1]
					found = true
					break
				}
			}
		} else if current.Kind == yaml.SequenceNode {
			idx := 0
			for _, c := range segment {
				if c < '0' || c > '9' {
					return nil
				}
				idx = idx*10 + int(c-'0')
			}
			if idx < len(current.Content) {
				current = current.Content[idx]
				found = true
			}
		}

		if !found {
			return nil
		}
	}

	return current
}
