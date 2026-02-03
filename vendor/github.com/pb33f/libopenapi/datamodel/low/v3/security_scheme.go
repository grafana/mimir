// Copyright 2022 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package v3

import (
	"context"
	"crypto/sha256"
	"strconv"

	"github.com/pb33f/libopenapi/datamodel/low"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/orderedmap"
	"github.com/pb33f/libopenapi/utils"
	"go.yaml.in/yaml/v4"
)

// SecurityScheme represents a low-level OpenAPI 3+ SecurityScheme object.
//
// Defines a security scheme that can be used by the operations.
//
// Supported schemes are HTTP authentication, an API key (either as a header, a cookie parameter or as a query parameter),
// mutual TLS (use of a client certificate), OAuth2’s common flows (implicit, password, client credentials and
// authorization code) as defined in RFC6749 (https://www.rfc-editor.org/rfc/rfc6749), and OpenID Connect Discovery.
// Please note that as of 2020, the implicit  flow is about to be deprecated by OAuth 2.0 Security Best Current Practice.
// Recommended for most use case is Authorization Code Grant flow with PKCE.
//   - https://spec.openapis.org/oas/v3.1.0#security-scheme-object
type SecurityScheme struct {
	Type             low.NodeReference[string]
	Description      low.NodeReference[string]
	Name             low.NodeReference[string]
	In               low.NodeReference[string]
	Scheme           low.NodeReference[string]
	BearerFormat     low.NodeReference[string]
	Flows            low.NodeReference[*OAuthFlows]
	OpenIdConnectUrl low.NodeReference[string]
	OAuth2MetadataUrl low.NodeReference[string] // OpenAPI 3.2+ OAuth2 metadata URL
	Deprecated       low.NodeReference[bool]   // OpenAPI 3.2+ deprecated flag
	Extensions       *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]
	KeyNode          *yaml.Node
	RootNode         *yaml.Node
	index            *index.SpecIndex
	context          context.Context
	*low.Reference
	low.NodeMap
}

// GetIndex returns the index.SpecIndex instance attached to the SecurityScheme object.
func (ss *SecurityScheme) GetIndex() *index.SpecIndex {
	return ss.index
}

// GetContext returns the context.Context instance used when building the SecurityScheme object.
func (ss *SecurityScheme) GetContext() context.Context {
	return ss.context
}

// GetRootNode returns the root yaml node of the SecurityScheme object.
func (ss *SecurityScheme) GetRootNode() *yaml.Node {
	return ss.RootNode
}

// GetKeyNode returns the key yaml node of the SecurityScheme object.
func (ss *SecurityScheme) GetKeyNode() *yaml.Node {
	return ss.KeyNode
}

// FindExtension attempts to locate an extension using the supplied key.
func (ss *SecurityScheme) FindExtension(ext string) *low.ValueReference[*yaml.Node] {
	return low.FindItemInOrderedMap(ext, ss.Extensions)
}

// GetExtensions returns all SecurityScheme extensions and satisfies the low.HasExtensions interface.
func (ss *SecurityScheme) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return ss.Extensions
}

// Build will extract OAuthFlows and extensions from the node.
func (ss *SecurityScheme) Build(ctx context.Context, keyNode, root *yaml.Node, idx *index.SpecIndex) error {
	ss.KeyNode = keyNode
	ss.Reference = new(low.Reference)
	if ok, _, ref := utils.IsNodeRefValue(root); ok {
		ss.SetReference(ref, root)
	}
	root = utils.NodeAlias(root)
	ss.RootNode = root
	utils.CheckForMergeNodes(root)
	ss.Nodes = low.ExtractNodes(ctx, root)
	ss.Extensions = low.ExtractExtensions(root)
	ss.index = idx
	ss.context = ctx

	low.ExtractExtensionNodes(ctx, ss.Extensions, ss.Nodes)

	oa, oaErr := low.ExtractObject[*OAuthFlows](ctx, OAuthFlowsLabel, root, idx)
	if oaErr != nil {
		return oaErr
	}
	if oa.Value != nil {
		ss.Flows = oa
	}
	return nil
}

// Hash will return a consistent SHA256 Hash of the SecurityScheme object
func (ss *SecurityScheme) Hash() [32]byte {
	// Use string builder pool
	sb := low.GetStringBuilder()
	defer low.PutStringBuilder(sb)

	if !ss.Type.IsEmpty() {
		sb.WriteString(ss.Type.Value)
		sb.WriteByte('|')
	}
	if !ss.Description.IsEmpty() {
		sb.WriteString(ss.Description.Value)
		sb.WriteByte('|')
	}
	if !ss.Name.IsEmpty() {
		sb.WriteString(ss.Name.Value)
		sb.WriteByte('|')
	}
	if !ss.In.IsEmpty() {
		sb.WriteString(ss.In.Value)
		sb.WriteByte('|')
	}
	if !ss.Scheme.IsEmpty() {
		sb.WriteString(ss.Scheme.Value)
		sb.WriteByte('|')
	}
	if !ss.BearerFormat.IsEmpty() {
		sb.WriteString(ss.BearerFormat.Value)
		sb.WriteByte('|')
	}
	if !ss.Flows.IsEmpty() {
		sb.WriteString(low.GenerateHashString(ss.Flows.Value))
		sb.WriteByte('|')
	}
	if !ss.OpenIdConnectUrl.IsEmpty() {
		sb.WriteString(ss.OpenIdConnectUrl.Value)
		sb.WriteByte('|')
	}
	if !ss.OAuth2MetadataUrl.IsEmpty() {
		sb.WriteString(ss.OAuth2MetadataUrl.Value)
		sb.WriteByte('|')
	}
	if !ss.Deprecated.IsEmpty() {
		sb.WriteString(strconv.FormatBool(ss.Deprecated.Value))
		sb.WriteByte('|')
	}
	for _, ext := range low.HashExtensions(ss.Extensions) {
		sb.WriteString(ext)
		sb.WriteByte('|')
	}
	return sha256.Sum256([]byte(sb.String()))
}
