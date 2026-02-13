// Copyright 2022 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package v3

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/pb33f/libopenapi/datamodel/low"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/orderedmap"
	"github.com/pb33f/libopenapi/utils"
	"go.yaml.in/yaml/v4"
)

// OAuthFlows represents a low-level OpenAPI 3+ OAuthFlows object.
//   - https://spec.openapis.org/oas/v3.1.0#oauth-flows-object
type OAuthFlows struct {
	Implicit          low.NodeReference[*OAuthFlow]
	Password          low.NodeReference[*OAuthFlow]
	ClientCredentials low.NodeReference[*OAuthFlow]
	AuthorizationCode low.NodeReference[*OAuthFlow]
	Device            low.NodeReference[*OAuthFlow] // OpenAPI 3.2+ device flow
	Extensions        *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]
	KeyNode           *yaml.Node
	RootNode          *yaml.Node
	index             *index.SpecIndex
	context           context.Context
	*low.Reference
	low.NodeMap
}

// GetIndex returns the index.SpecIndex instance attached to the OAuthFlows object.
func (o *OAuthFlows) GetIndex() *index.SpecIndex {
	return o.index
}

// GetContext returns the context.Context instance used when building the OAuthFlows object.
func (o *OAuthFlows) GetContext() context.Context {
	return o.context
}

// GetExtensions returns all OAuthFlows extensions and satisfies the low.HasExtensions interface.
func (o *OAuthFlows) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return o.Extensions
}

// FindExtension will attempt to locate an extension with the supplied name.
func (o *OAuthFlows) FindExtension(ext string) *low.ValueReference[*yaml.Node] {
	return low.FindItemInOrderedMap(ext, o.Extensions)
}

// GetRootNode returns the root yaml node of the OAuthFlows object.
func (o *OAuthFlows) GetRootNode() *yaml.Node {
	return o.RootNode
}

// GetKeyNode returns the key yaml node of the OAuthFlows object.
func (o *OAuthFlows) GetKeyNode() *yaml.Node {
	return o.KeyNode
}

// Build will extract extensions and all OAuthFlow types from the supplied node.
func (o *OAuthFlows) Build(ctx context.Context, keyNode, root *yaml.Node, idx *index.SpecIndex) error {
	o.KeyNode = keyNode
	root = utils.NodeAlias(root)
	o.RootNode = root
	utils.CheckForMergeNodes(root)
	o.Reference = new(low.Reference)
	o.Nodes = low.ExtractNodes(ctx, root)
	o.Extensions = low.ExtractExtensions(root)
	o.index = idx
	o.context = ctx

	v, vErr := low.ExtractObject[*OAuthFlow](ctx, ImplicitLabel, root, idx)
	if vErr != nil {
		return vErr
	}
	o.Implicit = v

	v, vErr = low.ExtractObject[*OAuthFlow](ctx, PasswordLabel, root, idx)
	if vErr != nil {
		return vErr
	}
	o.Password = v

	v, vErr = low.ExtractObject[*OAuthFlow](ctx, ClientCredentialsLabel, root, idx)
	if vErr != nil {
		return vErr
	}
	o.ClientCredentials = v

	v, vErr = low.ExtractObject[*OAuthFlow](ctx, AuthorizationCodeLabel, root, idx)
	if vErr != nil {
		return vErr
	}
	o.AuthorizationCode = v

	v, vErr = low.ExtractObject[*OAuthFlow](ctx, DeviceLabel, root, idx)
	if vErr != nil {
		return vErr
	}
	o.Device = v

	return nil
}

// Hash will return a consistent SHA256 Hash of the OAuthFlow object
func (o *OAuthFlows) Hash() [32]byte {
	// Use string builder pool
	sb := low.GetStringBuilder()
	defer low.PutStringBuilder(sb)

	if !o.Implicit.IsEmpty() {
		sb.WriteString(low.GenerateHashString(o.Implicit.Value))
		sb.WriteByte('|')
	}
	if !o.Password.IsEmpty() {
		sb.WriteString(low.GenerateHashString(o.Password.Value))
		sb.WriteByte('|')
	}
	if !o.ClientCredentials.IsEmpty() {
		sb.WriteString(low.GenerateHashString(o.ClientCredentials.Value))
		sb.WriteByte('|')
	}
	if !o.AuthorizationCode.IsEmpty() {
		sb.WriteString(low.GenerateHashString(o.AuthorizationCode.Value))
		sb.WriteByte('|')
	}
	if !o.Device.IsEmpty() {
		sb.WriteString(low.GenerateHashString(o.Device.Value))
		sb.WriteByte('|')
	}
	for _, ext := range low.HashExtensions(o.Extensions) {
		sb.WriteString(ext)
		sb.WriteByte('|')
	}
	return sha256.Sum256([]byte(sb.String()))
}

// OAuthFlow represents a low-level OpenAPI 3+ OAuthFlow object.
//   - https://spec.openapis.org/oas/v3.1.0#oauth-flow-object
type OAuthFlow struct {
	AuthorizationUrl low.NodeReference[string]
	TokenUrl         low.NodeReference[string]
	RefreshUrl       low.NodeReference[string]
	Scopes           low.NodeReference[*orderedmap.Map[low.KeyReference[string], low.ValueReference[string]]]
	Extensions       *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]]
	RootNode         *yaml.Node
	index            *index.SpecIndex
	context          context.Context
	*low.Reference
	low.NodeMap
}

// GetIndex returns the index.SpecIndex instance attached to the OAuthFlow object.
func (o *OAuthFlow) GetIndex() *index.SpecIndex {
	return o.index
}

// GetContext returns the context.Context instance used when building the OAuthFlow object.
func (o *OAuthFlow) GetContext() context.Context {
	return o.context
}

// GetExtensions returns all OAuthFlow extensions and satisfies the low.HasExtensions interface.
func (o *OAuthFlow) GetExtensions() *orderedmap.Map[low.KeyReference[string], low.ValueReference[*yaml.Node]] {
	return o.Extensions
}

// FindScope attempts to locate a scope using a specified name.
func (o *OAuthFlow) FindScope(scope string) *low.ValueReference[string] {
	return low.FindItemInOrderedMap[string](scope, o.Scopes.Value)
}

// FindExtension attempts to locate an extension with a specified key
func (o *OAuthFlow) FindExtension(ext string) *low.ValueReference[*yaml.Node] {
	return low.FindItemInOrderedMap(ext, o.Extensions)
}

// GetRootNode returns the root yaml node of the OAuthFlow object.
func (o *OAuthFlow) GetRootNode() *yaml.Node {
	return o.RootNode
}

// Build will extract extensions from the node.
func (o *OAuthFlow) Build(ctx context.Context, _, root *yaml.Node, idx *index.SpecIndex) error {
	o.Reference = new(low.Reference)
	o.Nodes = low.ExtractNodes(ctx, root)
	o.Extensions = low.ExtractExtensions(root)
	o.index = idx
	o.context = ctx
	low.ExtractExtensionNodes(ctx, o.Extensions, o.Nodes)

	if o.Scopes.Value != nil && o.Scopes.Value.Len() > 0 {
		for k := range o.Scopes.Value.KeysFromOldest() {
			o.Nodes.Store(k.KeyNode.Line, k.KeyNode)
		}
	}

	o.RootNode = root
	return nil
}

// Hash will return a consistent SHA256 Hash of the OAuthFlow object
func (o *OAuthFlow) Hash() [32]byte {
	// Use string builder pool
	sb := low.GetStringBuilder()
	defer low.PutStringBuilder(sb)

	if !o.AuthorizationUrl.IsEmpty() {
		sb.WriteString(o.AuthorizationUrl.Value)
		sb.WriteByte('|')
	}
	if !o.TokenUrl.IsEmpty() {
		sb.WriteString(o.TokenUrl.Value)
		sb.WriteByte('|')
	}
	if !o.RefreshUrl.IsEmpty() {
		sb.WriteString(o.RefreshUrl.Value)
		sb.WriteByte('|')
	}
	for k, v := range orderedmap.SortAlpha(o.Scopes.Value).FromOldest() {
		sb.WriteString(fmt.Sprintf("%s-%s", k.Value, sha256.Sum256([]byte(v.Value))))
		sb.WriteByte('|')
	}
	for _, ext := range low.HashExtensions(o.Extensions) {
		sb.WriteString(ext)
		sb.WriteByte('|')
	}
	return sha256.Sum256([]byte(sb.String()))
}
