![Huma Rest API Framework](https://user-images.githubusercontent.com/106826/78105564-51102780-73a6-11ea-99ff-84d6c1b3e8df.png)

[![HUMA Powered](https://img.shields.io/badge/Powered%20By-HUMA-f40273)](https://huma.rocks/) [![CI](https://github.com/danielgtaylor/huma/workflows/CI/badge.svg?branch=main)](https://github.com/danielgtaylor/huma/actions?query=workflow%3ACI+branch%3Amain++) [![codecov](https://codecov.io/gh/danielgtaylor/huma/branch/main/graph/badge.svg)](https://codecov.io/gh/danielgtaylor/huma) [![Docs](https://godoc.org/github.com/danielgtaylor/huma/v2?status.svg)](https://pkg.go.dev/github.com/danielgtaylor/huma/v2?tab=doc) [![Go Report Card](https://goreportcard.com/badge/github.com/danielgtaylor/huma/v2)](https://goreportcard.com/report/github.com/danielgtaylor/huma/v2)

- [What is huma?](#intro)
- [Install](#install)
- [Example](#example)
- [Documentation](#documentation)

<a name="intro"></a>
A modern, simple, fast & flexible micro framework for building HTTP REST/RPC APIs in Go backed by OpenAPI 3 and JSON Schema. Pronounced IPA: [/'hjuːmɑ/](https://en.wiktionary.org/wiki/Wiktionary:International_Phonetic_Alphabet). The goals of this project are to provide:

- Incremental adoption for teams with existing services
  - Bring your own router, middleware, and logging/metrics
  - Extensible OpenAPI & JSON Schema layer to document existing routes
- A modern REST or HTTP RPC API backend framework for Go developers
  - Described by [OpenAPI 3.1](https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.1.0.md) & [JSON Schema](https://json-schema.org/)
- Guard rails to prevent common mistakes
- Documentation that can't get out of date
- High-quality generated developer tooling

Features include:

- Declarative interface on top of your router of choice:
  - Operation & model documentation
  - Request params (path, query, or header)
  - Request body
  - Responses (including errors)
  - Response headers
- JSON Errors using [RFC7807](https://tools.ietf.org/html/rfc7807) and `application/problem+json` by default (but can be changed)
- Per-operation request size limits with sane defaults
- [Content negotiation](https://developer.mozilla.org/en-US/docs/Web/HTTP/Content_negotiation) between server and client
  - Support for JSON ([RFC 8259](https://tools.ietf.org/html/rfc8259)) and CBOR ([RFC 7049](https://tools.ietf.org/html/rfc7049)) content types via the `Accept` header with the default config.
- Conditional requests support, e.g. `If-Match` or `If-Unmodified-Since` header utilities.
- Optional automatic generation of `PATCH` operations that support:
  - [RFC 7386](https://www.rfc-editor.org/rfc/rfc7386) JSON Merge Patch
  - [RFC 6902](https://www.rfc-editor.org/rfc/rfc6902) JSON Patch
  - [Shorthand](https://github.com/danielgtaylor/shorthand) patches
- Annotated Go types for input and output models
  - Generates JSON Schema from Go types
  - Static typing for path/query/header params, bodies, response headers, etc.
  - Automatic input model validation & error handling
- Documentation generation using [Stoplight Elements](https://stoplight.io/open-source/elements)
- Optional CLI built-in, configured via arguments or environment variables
  - Set via e.g. `-p 8000`, `--port=8000`, or `SERVICE_PORT=8000`
  - Startup actions & graceful shutdown built-in
- Generates OpenAPI for access to a rich ecosystem of tools
  - Mocks with [API Sprout](https://github.com/danielgtaylor/apisprout) or [Prism](https://stoplight.io/open-source/prism)
  - SDKs with [OpenAPI Generator](https://github.com/OpenAPITools/openapi-generator) or [oapi-codegen](https://github.com/deepmap/oapi-codegen)
  - CLI with [Restish](https://rest.sh/)
  - And [plenty](https://openapi.tools/) [more](https://apis.guru/awesome-openapi3/category.html)
- Generates JSON Schema for each resource using optional `describedby` link relation headers as well as optional `$schema` properties in returned objects that integrate into editors for validation & completion.

This project was inspired by [FastAPI](https://fastapi.tiangolo.com/). Logo & branding designed by Kari Taylor.

# Install

Install via `go get`. Note that Go 1.20 or newer is required.

```sh
# After: go mod init ...
go get -u github.com/danielgtaylor/huma/v2
```

# Example

Here is a complete basic hello world example in Huma, that shows how to initialize a Huma app complete with CLI, declare a resource operation, and define its handler function.

```go
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
)

// Options for the CLI.
type Options struct {
	Port int `help:"Port to listen on" short:"p" default:"8888"`
}

// GreetingInput represents the greeting operation request.
type GreetingInput struct {
	Name string `path:"name" maxLength:"30" example:"world" doc:"Name to greet"`
}

// GreetingOutput represents the greeting operation response.
type GreetingOutput struct {
	Body struct {
		Message string `json:"message" example:"Hello, world!" doc:"Greeting message"`
	}
}

func main() {
	// Create a CLI app which takes a port option.
	cli := huma.NewCLI(func(hooks huma.Hooks, options *Options) {
		// Create a new router & API
		router := chi.NewMux()
		api := humachi.New(router, huma.DefaultConfig("My API", "1.0.0"))

		// Register GET /greeting/{name}
		huma.Register(api, huma.Operation{
			OperationID: "get-greeting",
			Summary:     "Get a greeting",
			Method:      http.MethodGet,
			Path:        "/greeting/{name}",
		}, func(ctx context.Context, input *GreetingInput) (*GreetingOutput, error) {
			resp := &GreetingOutput{}
			resp.Body.Message = fmt.Sprintf("Hello, %s!", input.Name)
			return resp, nil
		})

		// Tell the CLI how to start your router.
		hooks.OnStart(func() {
			http.ListenAndServe(fmt.Sprintf(":%d", options.Port), router)
		})
	})

	// Run the CLI. When passed no commands, it starts the server.
	cli.Run()
}
```

You can test it with `go run greet.go` (optionally pass `--port` to change the default) and make a sample request using [Restish](https://rest.sh/) (or `curl`):

```sh
# Get the message from the server
$ restish :8888/greeting/world
HTTP/1.1 200 OK
...
{
	$schema: "http://localhost:8888/schemas/GreetingOutputBody.json",
	message: "Hello, world!"
}
```

Even though the example is tiny you can also see some generated documentation at http://localhost:8888/docs. The generated OpenAPI is available at http://localhost:8888/openapi.json or http://localhost:8888/openapi.yaml.

# Documentation

See the [https://huma.rocks/](https://huma.rocks/) website for full documentation in a presentation that's easier to navigate and search then this README. You can find the source for the site in the `docs` directory of this repo.

Official Go package documentation can always be found at https://pkg.go.dev/github.com/danielgtaylor/huma/v2.
