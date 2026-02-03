// Copyright 2025 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package cache

import (
	"github.com/pb33f/libopenapi/datamodel/high/base"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// SchemaCacheEntry holds a compiled schema and its intermediate representations.
// This is stored in the cache to avoid re-rendering and re-compiling schemas on each request.
type SchemaCacheEntry struct {
	Schema          *base.Schema
	RenderedInline  []byte
	ReferenceSchema string // String version of RenderedInline
	RenderedJSON    []byte
	CompiledSchema  *jsonschema.Schema
}

// SchemaCache defines the interface for schema caching implementations.
// The key is a [32]byte hash of the schema (from schema.GoLow().Hash()).
type SchemaCache interface {
	Load(key [32]byte) (*SchemaCacheEntry, bool)
	Store(key [32]byte, value *SchemaCacheEntry)
	Range(f func(key [32]byte, value *SchemaCacheEntry) bool)
}
