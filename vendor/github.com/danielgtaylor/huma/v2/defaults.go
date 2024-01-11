package huma

import (
	"encoding/json"
	"io"

	"github.com/fxamacker/cbor/v2"
)

// DefaultJSONFormat is the default JSON formatter that can be set in the API's
// `Config.Formats` map. This is used by the `DefaultConfig` function.
//
//	config := huma.Config{}
//	config.Formats = map[string]huma.Format{
//		"application/json": huma.DefaultJSONFormat,
//		"json":             huma.DefaultJSONFormat,
//	}
var DefaultJSONFormat = Format{
	Marshal: func(w io.Writer, v any) error {
		return json.NewEncoder(w).Encode(v)
	},
	Unmarshal: json.Unmarshal,
}

var cborEncMode, _ = cbor.EncOptions{
	// Canonical enc opts
	Sort:          cbor.SortCanonical,
	ShortestFloat: cbor.ShortestFloat16,
	NaNConvert:    cbor.NaNConvert7e00,
	InfConvert:    cbor.InfConvertFloat16,
	IndefLength:   cbor.IndefLengthForbidden,
	// Time handling
	Time:    cbor.TimeUnixDynamic,
	TimeTag: cbor.EncTagRequired,
}.EncMode()

// DefaultCBORFormat is the default CBOR formatter that can be set in the API's
// `Config.Formats` map. This is used by the `DefaultConfig` function.
//
//	config := huma.Config{}
//	config.Formats = map[string]huma.Format{
//		"application/cbor": huma.DefaultCBORFormat,
//		"cbor":             huma.DefaultCBORFormat,
//	}
var DefaultCBORFormat = Format{
	Marshal: func(w io.Writer, v any) error {
		return cborEncMode.NewEncoder(w).Encode(v)
	},
	Unmarshal: cbor.Unmarshal,
}

// DefaultConfig returns a default configuration for a new API. It is a good
// starting point for creating your own configuration. It supports JSON and
// CBOR formats out of the box. The registry uses references for structs and
// a link transformer is included to add `$schema` fields and links into
// responses. The `/openapi.[json|yaml]`, `/docs`, and `/schemas` paths are
// set up to serve the OpenAPI spec, docs UI, and schemas respectively.
//
//	// Create and customize the config (if desired).
//	config := huma.DefaultConfig("My API", "1.0.0")
//
//	// Create the API using the config.
//	router := chi.NewMux()
//	api := humachi.New(router, config)
func DefaultConfig(title, version string) Config {
	schemaPrefix := "#/components/schemas/"
	schemasPath := "/schemas"

	registry := NewMapRegistry(schemaPrefix, DefaultSchemaNamer)

	linkTransformer := NewSchemaLinkTransformer(schemaPrefix, schemasPath)

	return Config{
		OpenAPI: &OpenAPI{
			OpenAPI: "3.1.0",
			Info: &Info{
				Title:   title,
				Version: version,
			},
			Components: &Components{
				Schemas: registry,
			},
			OnAddOperation: []AddOpFunc{
				linkTransformer.OnAddOperation,
			},
		},
		OpenAPIPath: "/openapi",
		DocsPath:    "/docs",
		SchemasPath: schemasPath,
		Formats: map[string]Format{
			"application/json": DefaultJSONFormat,
			"json":             DefaultJSONFormat,
			"application/cbor": DefaultCBORFormat,
			"cbor":             DefaultCBORFormat,
		},
		DefaultFormat: "application/json",
		Transformers: []Transformer{
			linkTransformer.Transform,
		},
	}
}
