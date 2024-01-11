package huma

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2/negotiation"
	"github.com/goccy/go-yaml"
)

var rxSchema = regexp.MustCompile(`#/components/schemas/([^"]+)`)

var ErrUnknownContentType = errors.New("unknown content type")

// Resolver runs a `Resolve` function after a request has been parsed, enabling
// you to run custom validation or other code that can modify the request and /
// or return errors.
type Resolver interface {
	Resolve(ctx Context) []error
}

// ResolverWithPath runs a `Resolve` function after a request has been parsed,
// enabling you to run custom validation or other code that can modify the
// request and / or return errors. The `prefix` is the path to the current
// location for errors, e.g. `body.foo[0].bar`.
type ResolverWithPath interface {
	Resolve(ctx Context, prefix *PathBuffer) []error
}

var resolverType = reflect.TypeOf((*Resolver)(nil)).Elem()
var resolverWithPathType = reflect.TypeOf((*ResolverWithPath)(nil)).Elem()

// Adapter is an interface that allows the API to be used with different HTTP
// routers and frameworks. It is designed to work with the standard library
// `http.Request` and `http.ResponseWriter` types as well as types like
// `gin.Context` or `fiber.Ctx` that provide both request and response
// functionality in one place, by using the `huma.Context` interface which
// abstracts away those router-specific differences.
//
// The handler function takes uses the context to get request information like
// path / query / header params, the input body, and provide response data
// like a status code, response headers, and a response body.
type Adapter interface {
	Handle(op *Operation, handler func(ctx Context))
	ServeHTTP(http.ResponseWriter, *http.Request)
}

// Context is the current request/response context. It provides a generic
// interface to get request information and write responses.
type Context interface {
	// Operation returns the OpenAPI operation that matched the request.
	Operation() *Operation

	// Context returns the underlying request context.
	Context() context.Context

	// Method returns the HTTP method for the request.
	Method() string

	// Host returns the HTTP host for the request.
	Host() string

	// URL returns the full URL for the request.
	URL() url.URL

	// Param returns the value for the given path parameter.
	Param(name string) string

	// Query returns the value for the given query parameter.
	Query(name string) string

	// Header returns the value for the given header.
	Header(name string) string

	// EachHeader iterates over all headers and calls the given callback with
	// the header name and value.
	EachHeader(cb func(name, value string))

	// BodyReader returns the request body reader.
	BodyReader() io.Reader

	// GetMultipartForm returns the parsed multipart form, if any.
	GetMultipartForm() (*multipart.Form, error)

	// SetReadDeadline sets the read deadline for the request body.
	SetReadDeadline(time.Time) error

	// SetStatus sets the HTTP status code for the response.
	SetStatus(code int)

	// SetHeader sets the given header to the given value, overwriting any
	// existing value. Use `AppendHeader` to append a value instead.
	SetHeader(name, value string)

	// AppendHeader appends the given value to the given header.
	AppendHeader(name, value string)

	// BodyWriter returns the response body writer.
	BodyWriter() io.Writer
}

// Transformer is a function that can modify a response body before it is
// serialized. The `status` is the HTTP status code for the response and `v` is
// the value to be serialized. The return value is the new value to be
// serialized or an error.
type Transformer func(ctx Context, status string, v any) (any, error)

// Config represents a configuration for a new API. See `huma.DefaultConfig()`
// as a starting point.
type Config struct {
	// OpenAPI spec for the API. You should set at least the `Info.Title` and
	// `Info.Version` fields.
	*OpenAPI

	// OpenAPIPath is the path to the OpenAPI spec without extension. If set
	// to `/openapi` it will allow clients to get `/openapi.json` or
	// `/openapi.yaml`, for example.
	OpenAPIPath string

	// DocsPath is the path to the API documentation. If set to `/docs` it will
	// allow clients to get `/docs` to view the documentation in a browser. If
	// you wish to provide your own documentation renderer, you can leave this
	// blank and attach it directly to the router or adapter.
	DocsPath string

	// SchemasPath is the path to the API schemas. If set to `/schemas` it will
	// allow clients to get `/schemas/{schema}` to view the schema in a browser
	// or for use in editors like VSCode to provide autocomplete & validation.
	SchemasPath string

	// Formats defines the supported request/response formats by content type or
	// extension (e.g. `json` for `application/my-format+json`).
	Formats map[string]Format

	// DefaultFormat specifies the default content type to use when the client
	// does not specify one. If unset, the default type will be randomly
	// chosen from the keys of `Formats`.
	DefaultFormat string

	// Transformers are a way to modify a response body before it is serialized.
	Transformers []Transformer
}

// API represents a Huma API wrapping a specific router.
type API interface {
	// Adapter returns the router adapter for this API, providing a generic
	// interface to get request information and write responses.
	Adapter() Adapter

	// OpenAPI returns the OpenAPI spec for this API. You may edit this spec
	// until the server starts.
	OpenAPI() *OpenAPI

	// Negotiate returns the selected content type given the client's `accept`
	// header and the server's supported content types. If the client does not
	// send an `accept` header, then JSON is used.
	Negotiate(accept string) (string, error)

	// Transform runs the API transformers on the given value. The `status` is
	// the key in the operation's `Responses` map that corresponds to the
	// response being sent (e.g. "200" for a 200 OK response).
	Transform(ctx Context, status string, v any) (any, error)

	// Marshal marshals the given value into the given writer. The
	// content type is used to determine which format to use. Use `Negotiate` to
	// get the content type from an accept header.
	Marshal(w io.Writer, contentType string, v any) error

	// Unmarshal unmarshals the given data into the given value. The content type
	Unmarshal(contentType string, data []byte, v any) error

	// UseMiddleware appends a middleware handler to the API middleware stack.
	//
	// The middleware stack for any API will execute before searching for a matching
	// route to a specific handler, which provides opportunity to respond early,
	// change the course of the request execution, or set request-scoped values for
	// the next Middleware.
	UseMiddleware(middlewares ...func(ctx Context, next func(Context)))

	// Middlewares returns a slice of middleware handler functions.
	Middlewares() Middlewares
}

// Format represents a request / response format. It is used to marshal and
// unmarshal data.
type Format struct {
	// Marshal a value to a given writer (e.g. response body).
	Marshal func(writer io.Writer, v any) error

	// Unmarshal a value into `v` from the given bytes (e.g. request body).
	Unmarshal func(data []byte, v any) error
}

type api struct {
	config       Config
	adapter      Adapter
	formats      map[string]Format
	formatKeys   []string
	transformers []Transformer
	middlewares  Middlewares
}

func (a *api) Adapter() Adapter {
	return a.adapter
}

func (a *api) OpenAPI() *OpenAPI {
	return a.config.OpenAPI
}

func (a *api) Unmarshal(contentType string, data []byte, v any) error {
	// Handle e.g. `application/json; charset=utf-8` or `my/format+json`
	start := strings.IndexRune(contentType, '+') + 1
	end := strings.IndexRune(contentType, ';')
	if end == -1 {
		end = len(contentType)
	}
	ct := contentType[start:end]
	if ct == "" {
		// Default to assume JSON since this is an API.
		ct = "application/json"
	}
	f, ok := a.formats[ct]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownContentType, contentType)
	}
	return f.Unmarshal(data, v)
}

func (a *api) Negotiate(accept string) (string, error) {
	ct := negotiation.SelectQValueFast(accept, a.formatKeys)
	if ct == "" && a.formatKeys != nil {
		ct = a.formatKeys[0]
	}
	if _, ok := a.formats[ct]; !ok {
		return ct, fmt.Errorf("unknown content type: %s", ct)
	}
	return ct, nil
}

func (a *api) Transform(ctx Context, status string, v any) (any, error) {
	var err error
	for _, t := range a.transformers {
		v, err = t(ctx, status, v)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func (a *api) Marshal(w io.Writer, ct string, v any) error {
	f, ok := a.formats[ct]
	if !ok {
		start := strings.IndexRune(ct, '+') + 1
		f, ok = a.formats[ct[start:]]
	}
	if !ok {
		return fmt.Errorf("unknown content type: %s", ct)
	}
	return f.Marshal(w, v)
}

func (a *api) UseMiddleware(middlewares ...func(ctx Context, next func(Context))) {
	a.middlewares = append(a.middlewares, middlewares...)
}

func (a *api) Middlewares() Middlewares {
	return a.middlewares
}

// NewAPI creates a new API with the given configuration and router adapter.
// You usually don't need to use this function directly, and can instead use
// the `New(...)` function provided by the adapter packages which call this
// function internally.
//
// When the API is created, this function will ensure a schema registry exists
// (or create a new map registry if not), will set a default format if not
// set, and will set up the handlers for the OpenAPI spec, documentation, and
// JSON schema routes if the paths are set in the config.
//
//	router := chi.NewMux()
//	adapter := humachi.NewAdapter(router)
//	config := huma.DefaultConfig("Example API", "1.0.0")
//	api := huma.NewAPI(config, adapter)
func NewAPI(config Config, a Adapter) API {
	newAPI := &api{
		config:       config,
		adapter:      a,
		formats:      map[string]Format{},
		transformers: config.Transformers,
	}

	if config.OpenAPI == nil {
		config.OpenAPI = &OpenAPI{}
	}

	if config.OpenAPI.OpenAPI == "" {
		config.OpenAPI.OpenAPI = "3.1.0"
	}

	if config.OpenAPI.Components == nil {
		config.OpenAPI.Components = &Components{}
	}

	if config.OpenAPI.Components.Schemas == nil {
		config.OpenAPI.Components.Schemas = NewMapRegistry("#/components/schemas/", DefaultSchemaNamer)
	}

	if config.DefaultFormat == "" && config.Formats["application/json"].Marshal != nil {
		config.DefaultFormat = "application/json"
	}
	if config.DefaultFormat != "" {
		newAPI.formatKeys = append(newAPI.formatKeys, config.DefaultFormat)
	}
	for k, v := range config.Formats {
		newAPI.formats[k] = v
		newAPI.formatKeys = append(newAPI.formatKeys, k)
	}

	if config.OpenAPIPath != "" {
		var specJSON []byte
		a.Handle(&Operation{
			Method: http.MethodGet,
			Path:   config.OpenAPIPath + ".json",
		}, func(ctx Context) {
			ctx.SetHeader("Content-Type", "application/vnd.oai.openapi+json")
			if specJSON == nil {
				specJSON, _ = json.Marshal(newAPI.OpenAPI())
			}
			ctx.BodyWriter().Write(specJSON)
		})
		var specYAML []byte
		a.Handle(&Operation{
			Method: http.MethodGet,
			Path:   config.OpenAPIPath + ".yaml",
		}, func(ctx Context) {
			ctx.SetHeader("Content-Type", "application/vnd.oai.openapi+yaml")
			if specYAML == nil {
				specYAML, _ = yaml.Marshal(newAPI.OpenAPI())
			}
			ctx.BodyWriter().Write(specYAML)
		})
	}

	if config.DocsPath != "" {
		a.Handle(&Operation{
			Method: http.MethodGet,
			Path:   config.DocsPath,
		}, func(ctx Context) {
			ctx.SetHeader("Content-Type", "text/html")
			ctx.BodyWriter().Write([]byte(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <title>Elements in HTML</title>
    <!-- Embed elements Elements via Web Component -->
    <script src="https://unpkg.com/@stoplight/elements/web-components.min.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/@stoplight/elements/styles.min.css">
  </head>
  <body>

    <elements-api
      apiDescriptionUrl="` + config.OpenAPIPath + `.yaml"
      router="hash"
      layout="sidebar"
    />

  </body>
</html>`))
		})
	}

	if config.SchemasPath != "" {
		a.Handle(&Operation{
			Method: http.MethodGet,
			Path:   config.SchemasPath + "/{schema}",
		}, func(ctx Context) {
			// Some routers dislike a path param+suffix, so we strip it here instead.
			schema := strings.TrimSuffix(ctx.Param("schema"), ".json")
			ctx.SetHeader("Content-Type", "application/json")
			b, _ := json.Marshal(config.OpenAPI.Components.Schemas.Map()[schema])
			b = rxSchema.ReplaceAll(b, []byte(config.SchemasPath+`/$1.json`))
			ctx.BodyWriter().Write(b)
		})
	}

	return newAPI
}
