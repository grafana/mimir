package huma

import (
	"fmt"
	"net/http"
	"time"

	"github.com/goccy/go-yaml"
)

// Contact information to get support for the API.
//
//	name: API Support
//	url: https://www.example.com/support
//	email: support@example.com
type Contact struct {
	// Name of the contact person/organization.
	Name string `yaml:"name,omitempty"`

	// URL pointing to the contact information.
	URL string `yaml:"url,omitempty"`

	// Email address of the contact person/organization.
	Email string `yaml:"email,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// License name & link for using the API.
//
//	name: Apache 2.0
//	identifier: Apache-2.0
type License struct {
	// Name of the license.
	Name string `yaml:"name"`

	// Identifier SPDX license expression for the API. This field is mutually
	// exclusive with the URL field.
	Identifier string `yaml:"identifier,omitempty"`

	// URL pointing to the license. This field is mutually exclusive with the
	// Identifier field.
	URL string `yaml:"url,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Info object that provides metadata about the API. The metadata MAY be used by
// the clients if needed, and MAY be presented in editing or documentation
// generation tools for convenience.
//
//	title: Sample Pet Store App
//	summary: A pet store manager.
//	description: This is a sample server for a pet store.
//	termsOfService: https://example.com/terms/
//	contact:
//	  name: API Support
//	  url: https://www.example.com/support
//	  email: support@example.com
//	license:
//	  name: Apache 2.0
//	  url: https://www.apache.org/licenses/LICENSE-2.0.html
//	version: 1.0.1
type Info struct {
	// Title of the API.
	Title string `yaml:"title"`

	// Description of the API. CommonMark syntax MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// TermsOfService URL for the API.
	TermsOfService string `yaml:"termsOfService,omitempty"`

	// Contact information to get support for the API.
	Contact *Contact `yaml:"contact,omitempty"`

	// License name & link for using the API.
	License *License `yaml:"license,omitempty"`

	// Version of the OpenAPI document (which is distinct from the OpenAPI Specification version or the API implementation version).
	Version string `yaml:"version"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// ServerVariable for server URL template substitution.
type ServerVariable struct {
	// Enumeration of string values to be used if the substitution options are from a limited set. The array MUST NOT be empty.
	Enum []string `yaml:"enum,omitempty"`

	// Default value to use for substitution, which SHALL be sent if an alternate value is not supplied.
	Default string `yaml:"default"`

	// Description for the server variable. CommonMark syntax MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Server URL, optionally with variables.
//
//	servers:
//	- url: https://development.gigantic-server.com/v1
//	  description: Development server
//	- url: https://staging.gigantic-server.com/v1
//	  description: Staging server
//	- url: https://api.gigantic-server.com/v1
//	  description: Production server
type Server struct {
	// URL to the target host. This URL supports Server Variables and MAY be relative, to indicate that the host location is relative to the location where the OpenAPI document is being served. Variable substitutions will be made when a variable is named in {brackets}.
	URL string `yaml:"url"`

	// Description of the host designated by the URL. CommonMark syntax MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// Variables map between a variable name and its value. The value is used for substitution in the server’s URL template.
	Variables map[string]*ServerVariable `yaml:"variables,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Example value of a request param or body or response header or body.
//
//	requestBody:
//	  content:
//	    'application/json':
//	      schema:
//	        $ref: '#/components/schemas/Address'
//	      examples:
//	        foo:
//	          summary: A foo example
//	          value: {"foo": "bar"}
//	        bar:
//	          summary: A bar example
//	          value: {"bar": "baz"}
type Example struct {
	// Ref is a reference to another example. This field is mutually exclusive
	// with the other fields.
	Ref string `yaml:"$ref,omitempty"`

	// Summary is a short summary of the example.
	Summary string `yaml:"summary,omitempty"`

	// Description is a long description of the example. CommonMark syntax MAY
	// be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// Value is an embedded literal example. The `value` field and `externalValue`
	// field are mutually exclusive. To represent examples of media types that
	// cannot naturally represented in JSON or YAML, use a string value to contain
	// the example, escaping where necessary.
	Value any `yaml:"value,omitempty"`

	// ExternalValue is a URI that points to the literal example. This provides
	// the capability to reference examples that cannot easily be included in JSON
	// or YAML documents. The `value` field and `externalValue` field are mutually
	// exclusive. See the rules for resolving Relative References.
	ExternalValue string `yaml:"externalValue,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Encoding is a single encoding definition applied to a single schema property.
//
//	requestBody:
//	  content:
//	    multipart/form-data:
//	      schema:
//	        type: object
//	        properties:
//	          id:
//	            # default is text/plain
//	            type: string
//	            format: uuid
//	          address:
//	            # default is application/json
//	            type: object
//	            properties: {}
//	          historyMetadata:
//	            # need to declare XML format!
//	            description: metadata in XML format
//	            type: object
//	            properties: {}
//	          profileImage: {}
//	      encoding:
//	        historyMetadata:
//	          # require XML Content-Type in utf-8 encoding
//	          contentType: application/xml; charset=utf-8
//	        profileImage:
//	          # only accept png/jpeg
//	          contentType: image/png, image/jpeg
//	          headers:
//	            X-Rate-Limit-Limit:
//	              description: The number of allowed requests in the current period
//	              schema:
//	                type: integer
type Encoding struct {
	// ContentType for encoding a specific property. Default value depends on the
	// property type: for object - application/json; for array – the default is
	// defined based on the inner type; for all other cases the default is
	// application/octet-stream. The value can be a specific media type (e.g.
	// application/json), a wildcard media type (e.g. image/*), or a
	// comma-separated list of the two types.
	ContentType string `yaml:"contentType,omitempty"`

	// Headers is a map allowing additional information to be provided as headers,
	// for example Content-Disposition. Content-Type is described separately and
	// SHALL be ignored in this section. This property SHALL be ignored if the
	// request body media type is not a multipart.
	Headers map[string]*Header `yaml:"headers,omitempty"`

	// Style describes how a specific property value will be serialized depending
	// on its type. See Parameter Object for details on the style property. The
	// behavior follows the same values as query parameters, including default
	// values. This property SHALL be ignored if the request body media type is
	// not application/x-www-form-urlencoded or multipart/form-data. If a value is
	// explicitly defined, then the value of contentType (implicit or explicit)
	// SHALL be ignored.
	Style string `yaml:"style,omitempty"`

	// Explode, when true, property values of type array or object generate
	// separate parameters for each value of the array, or key-value-pair of the
	// map. For other types of properties this property has no effect. When style
	// is form, the default value is true. For all other styles, the default value
	// is false. This property SHALL be ignored if the request body media type is
	// not application/x-www-form-urlencoded or multipart/form-data. If a value is
	// explicitly defined, then the value of contentType (implicit or explicit)
	// SHALL be ignored.
	Explode *bool `yaml:"explode,omitempty"`

	// AllowReserved determines whether the parameter value SHOULD allow reserved
	// characters, as defined by [RFC3986] :/?#[]@!$&'()*+,;= to be included
	// without percent-encoding. The default value is false. This property SHALL
	// be ignored if the request body media type is not
	// application/x-www-form-urlencoded or multipart/form-data. If a value is
	// explicitly defined, then the value of contentType (implicit or explicit)
	// SHALL be ignored.
	AllowReserved bool `yaml:"allowReserved,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// MediaType object provides schema and examples for the media type identified
// by its key.
//
//	application/json:
//	  schema:
//	    $ref: "#/components/schemas/Pet"
//	  examples:
//	    cat:
//	      summary: An example of a cat
//	      value:
//	        name: Fluffy
//	        petType: Cat
//	        color: White
//	        gender: male
//	        breed: Persian
type MediaType struct {
	// Schema defining the content of the request, response, or parameter.
	Schema *Schema `yaml:"schema,omitempty"`

	// Example of the media type. The example object SHOULD be in the correct
	// format as specified by the media type. The example field is mutually
	// exclusive of the examples field. Furthermore, if referencing a schema which
	// contains an example, the example value SHALL override the example provided
	// by the schema.
	Example any `yaml:"example,omitempty"`

	// Examples of the media type. Each example object SHOULD match the media type
	// and specified schema if present. The examples field is mutually exclusive
	// of the example field. Furthermore, if referencing a schema which contains
	// an example, the examples value SHALL override the example provided by the
	// schema.
	Examples map[string]*Example `yaml:"examples,omitempty"`

	// Encoding is a map between a property name and its encoding information. The
	// key, being the property name, MUST exist in the schema as a property. The
	// encoding object SHALL only apply to requestBody objects when the media type
	// is multipart or application/x-www-form-urlencoded.
	Encoding map[string]*Encoding `yaml:"encoding,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Param Describes a single operation parameter.
//
// A unique parameter is defined by a combination of a name and location.
//
//	name: username
//	in: path
//	description: username to fetch
//	required: true
//	schema:
//	  type: string
type Param struct {
	// Ref is a reference to another example. This field is mutually exclusive
	// with the other fields.
	Ref string `yaml:"$ref,omitempty"`

	// Name is REQUIRED. The name of the parameter. Parameter names are case
	// sensitive.
	//
	//   - If in is "path", the name field MUST correspond to a template expression
	//     occurring within the path field in the Paths Object. See Path Templating
	//     for further information.
	//
	//   - If in is "header" and the name field is "Accept", "Content-Type" or
	//     "Authorization", the parameter definition SHALL be ignored.
	//
	//   - For all other cases, the name corresponds to the parameter name used by
	//     the in property.
	Name string `yaml:"name,omitempty"`

	// In is REQUIRED. The location of the parameter. Possible values are "query",
	// "header", "path" or "cookie".
	In string `yaml:"in,omitempty"`

	// Description of the parameter. This could contain examples of use.
	// CommonMark syntax MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// Required determines whether this parameter is mandatory. If the parameter
	// location is "path", this property is REQUIRED and its value MUST be true.
	// Otherwise, the property MAY be included and its default value is false.
	Required bool `yaml:"required,omitempty"`

	// Deprecated specifies that a parameter is deprecated and SHOULD be
	// transitioned out of usage. Default value is false.
	Deprecated bool `yaml:"deprecated,omitempty"`

	// AllowEmptyValue sets the ability to pass empty-valued parameters. This is
	// valid only for query parameters and allows sending a parameter with an
	// empty value. Default value is false. If style is used, and if behavior is
	// n/a (cannot be serialized), the value of allowEmptyValue SHALL be ignored.
	// Use of this property is NOT RECOMMENDED, as it is likely to be removed in a
	// later revision.
	AllowEmptyValue bool `yaml:"allowEmptyValue,omitempty"`

	// Style describes how the parameter value will be serialized depending on the
	// type of the parameter value. Default values (based on value of in): for
	// query - form; for path - simple; for header - simple; for cookie - form.
	Style string `yaml:"style,omitempty"`

	// Explode, when true, makes parameter values of type array or object generate
	// separate parameters for each value of the array or key-value pair of the
	// map. For other types of parameters this property has no effect. When style
	// is form, the default value is true. For all other styles, the default value
	// is false.
	Explode *bool `yaml:"explode,omitempty"`

	// AllowReserved determines whether the parameter value SHOULD allow reserved
	// characters, as defined by [RFC3986] :/?#[]@!$&'()*+,;= to be included
	// without percent-encoding. This property only applies to parameters with an
	// in value of query. The default value is false.
	AllowReserved bool `yaml:"allowReserved,omitempty"`

	// Schema defining the type used for the parameter.
	Schema *Schema `yaml:"schema,omitempty"`

	// Example of the parameter’s potential value. The example SHOULD match the
	// specified schema and encoding properties if present. The example field is
	// mutually exclusive of the examples field. Furthermore, if referencing a
	// schema that contains an example, the example value SHALL override the
	// example provided by the schema. To represent examples of media types that
	// cannot naturally be represented in JSON or YAML, a string value can contain
	// the example with escaping where necessary.
	Example any `yaml:"example,omitempty"`

	// Examples of the parameter’s potential value. Each example SHOULD contain a
	// value in the correct format as specified in the parameter encoding. The
	// examples field is mutually exclusive of the example field. Furthermore, if
	// referencing a schema that contains an example, the examples value SHALL
	// override the example provided by the schema.
	Examples map[string]*Example `yaml:"examples,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Header object follows the structure of the Parameter Object with the
// following changes:
//
//   - name MUST NOT be specified, it is given in the corresponding headers map.
//
//   - in MUST NOT be specified, it is implicitly in header.
//
//   - All traits that are affected by the location MUST be applicable to a
//     location of header (for example, style).
//
// Example:
//
//	description: The number of allowed requests in the current period
//	schema:
//	  type: integer
type Header = Param

// RequestBody describes a single request body.
//
//	description: user to add to the system
//	content:
//	  'application/json':
//	    schema:
//	      $ref: '#/components/schemas/User'
//	    examples:
//	      user:
//	        summary: User Example
//	        externalValue: 'https://foo.bar/examples/user-example.json'
type RequestBody struct {
	// Ref is a reference to another example. This field is mutually exclusive
	// with the other fields.
	Ref string `yaml:"$ref,omitempty"`

	// Description of the request body. This could contain examples of use.
	// CommonMark syntax MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// Content is REQUIRED. The content of the request body. The key is a media
	// type or media type range and the value describes it. For requests that
	// match multiple keys, only the most specific key is applicable. e.g.
	// text/plain overrides text/*
	Content map[string]*MediaType `yaml:"content"`

	// Required Determines if the request body is required in the request.
	// Defaults to false.
	Required bool `yaml:"required,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Link object represents a possible design-time link for a response. The
// presence of a link does not guarantee the caller’s ability to successfully
// invoke it, rather it provides a known relationship and traversal mechanism
// between responses and other operations.
//
// Unlike dynamic links (i.e. links provided in the response payload), the OAS
// linking mechanism does not require link information in the runtime response.
//
// For computing links, and providing instructions to execute them, a runtime
// expression is used for accessing values in an operation and using them as
// parameters while invoking the linked operation.
//
//	paths:
//	  /users/{id}:
//	    parameters:
//	    - name: id
//	      in: path
//	      required: true
//	      description: the user identifier, as userId
//	      schema:
//	        type: string
//	    get:
//	      responses:
//	        '200':
//	          description: the user being returned
//	          content:
//	            application/json:
//	              schema:
//	                type: object
//	                properties:
//	                  uuid: # the unique user id
//	                    type: string
//	                    format: uuid
//	          links:
//	            address:
//	              # the target link operationId
//	              operationId: getUserAddress
//	              parameters:
//	                # get the `id` field from the request path parameter named `id`
//	                userId: $request.path.id
//	  # the path item of the linked operation
//	  /users/{userid}/address:
//	    parameters:
//	    - name: userid
//	      in: path
//	      required: true
//	      description: the user identifier, as userId
//	      schema:
//	        type: string
//	    # linked operation
//	    get:
//	      operationId: getUserAddress
//	      responses:
//	        '200':
//	          description: the user's address
type Link struct {
	// Ref is a reference to another example. This field is mutually exclusive
	// with the other fields.
	Ref string `yaml:"$ref,omitempty"`

	// OperationRef is a relative or absolute URI reference to an OAS operation.
	// This field is mutually exclusive of the operationId field, and MUST point
	// to an Operation Object. Relative operationRef values MAY be used to locate
	// an existing Operation Object in the OpenAPI definition. See the rules for
	// resolving Relative References.
	OperationRef string `yaml:"operationRef,omitempty"`

	// OperationID is the name of an existing, resolvable OAS operation, as
	// defined with a unique operationId. This field is mutually exclusive of the
	// operationRef field.
	OperationID string `yaml:"operationId,omitempty"`

	// Parameters is a map representing parameters to pass to an operation as
	// specified with operationId or identified via operationRef. The key is the
	// parameter name to be used, whereas the value can be a constant or an
	// expression to be evaluated and passed to the linked operation. The
	// parameter name can be qualified using the parameter location [{in}.]{name}
	// for operations that use the same parameter name in different locations
	// (e.g. path.id).
	Parameters map[string]any `yaml:"parameters,omitempty"`

	// RequestBody is a literal value or {expression} to use as a request body
	// when calling the target operation.
	RequestBody any `yaml:"requestBody,omitempty"`

	// Description of the link. CommonMark syntax MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// Server object to be used by the target operation.
	Server *Server `yaml:"server,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Response describes a single response from an API Operation, including
// design-time, static links to operations based on the response.
//
//	description: A complex object array response
//	content:
//	  application/json:
//	    schema:
//	      type: array
//	      items:
//	        $ref: '#/components/schemas/VeryComplexType'
type Response struct {
	// Ref is a reference to another example. This field is mutually exclusive
	// with the other fields.
	Ref string `yaml:"$ref,omitempty"`

	// Description is REQUIRED. A description of the response. CommonMark syntax
	// MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// Headers maps a header name to its definition. [RFC7230] states header names
	// are case insensitive. If a response header is defined with the name
	// "Content-Type", it SHALL be ignored.
	Headers map[string]*Param `yaml:"headers,omitempty"`

	// Content is a map containing descriptions of potential response payloads.
	// The key is a media type or media type range and the value describes it. For
	// responses that match multiple keys, only the most specific key is
	// applicable. e.g. text/plain overrides text/*
	Content map[string]*MediaType `yaml:"content,omitempty"`

	// Links is a map of operations links that can be followed from the response.
	// The key of the map is a short name for the link, following the naming
	// constraints of the names for Component Objects.
	Links map[string]*Link `yaml:"links,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Operation describes a single API operation on a path.
//
//	tags:
//	- pet
//	summary: Updates a pet in the store with form data
//	operationId: updatePetWithForm
//	parameters:
//	- name: petId
//	  in: path
//	  description: ID of pet that needs to be updated
//	  required: true
//	  schema:
//	    type: string
//	requestBody:
//	  content:
//	    'application/x-www-form-urlencoded':
//	      schema:
//	       type: object
//	       properties:
//	          name:
//	            description: Updated name of the pet
//	            type: string
//	          status:
//	            description: Updated status of the pet
//	            type: string
//	       required:
//	         - status
//	responses:
//	  '200':
//	    description: Pet updated.
//	    content:
//	      'application/json': {}
//	      'application/xml': {}
//	  '405':
//	    description: Method Not Allowed
//	    content:
//	      'application/json': {}
//	      'application/xml': {}
//	security:
//	- petstore_auth:
//	  - write:pets
//	  - read:pets
type Operation struct {
	// --- Huma-specific fields ---

	// Method is the HTTP method for this operation
	Method string `yaml:"-"`

	// Path is the URL path for this operation
	Path string `yaml:"-"`

	// DefaultStatus is the default HTTP status code for this operation. It will
	// be set to 200 or 204 if not specified, depending on whether the handler
	// returns a response body.
	DefaultStatus int `yaml:"-"`

	// MaxBodyBytes is the maximum number of bytes to read from the request
	// body. If not specified, the default is 1MB. Use -1 for unlimited. If
	// the limit is reached, then an HTTP 413 error is returned.
	MaxBodyBytes int64 `yaml:"-"`

	// BodyReadTimeout is the maximum amount of time to wait for the request
	// body to be read. If not specified, the default is 5 seconds. Use -1
	// for unlimited. If the timeout is reached, then an HTTP 408 error is
	// returned. This value supercedes the server's read timeout, and a value
	// of -1 can unset the server's timeout.
	BodyReadTimeout time.Duration `yaml:"-"`

	// Errors is a list of HTTP status codes that the handler may return. If
	// not specified, then a default error response is added to the OpenAPI.
	// This is a convenience for handlers that return a fixed set of errors
	// where you do not wish to provide each one as an OpenAPI response object.
	// Each error specified here is expanded into a response object with the
	// schema generated from the type returned by `huma.NewError()`.
	Errors []int `yaml:"-"`

	// SkipValidateParams disables validation of path, query, and header
	// parameters. This can speed up request processing if you want to handle
	// your own validation. Use with caution!
	SkipValidateParams bool `yaml:"-"`

	// SkipValidateBody disables validation of the request body. This can speed
	// up request processing if you want to handle your own validation. Use with
	// caution!
	SkipValidateBody bool `yaml:"-"`

	// Hidden will skip documenting this operation in the OpenAPI. This is
	// useful for operations that are not intended to be used by clients but
	// you'd still like the benefits of using Huma. Generally not recommended.
	Hidden bool `yaml:"-"`

	// Metadata is a map of arbitrary data that can be attached to the operation.
	// This can be used to store custom data, such as custom settings for
	// functions which generate operations.
	Metadata map[string]any `yaml:"-"`

	// --- OpenAPI fields ---

	// Tags is a list of tags for API documentation control. Tags can be used for
	// logical grouping of operations by resources or any other qualifier.
	Tags []string `yaml:"tags,omitempty"`

	// Summary is a short summary of what the operation does.
	Summary string `yaml:"summary,omitempty"`

	// Description is a verbose explanation of the operation behavior. CommonMark
	// syntax MAY be used for rich text representation.
	Description string `yaml:"description,omitempty"`

	// ExternalDocs describes additional external documentation for this
	// operation.
	ExternalDocs *ExternalDocs `yaml:"externalDocs,omitempty"`

	// OperationID is a unique string used to identify the operation. The id MUST
	// be unique among all operations described in the API. The operationId value
	// is case-sensitive. Tools and libraries MAY use the operationId to uniquely
	// identify an operation, therefore, it is RECOMMENDED to follow common
	// programming naming conventions.
	OperationID string `yaml:"operationId,omitempty"`

	// Parameters is a list of parameters that are applicable for this operation.
	// If a parameter is already defined at the Path Item, the new definition will
	// override it but can never remove it. The list MUST NOT include duplicated
	// parameters. A unique parameter is defined by a combination of a name and
	// location. The list can use the Reference Object to link to parameters that
	// are defined at the OpenAPI Object’s components/parameters.
	Parameters []*Param `yaml:"parameters,omitempty"`

	// RequestBody applicable for this operation. The requestBody is fully
	// supported in HTTP methods where the HTTP 1.1 specification [RFC7231] has
	// explicitly defined semantics for request bodies. In other cases where the
	// HTTP spec is vague (such as GET, HEAD and DELETE), requestBody is permitted
	// but does not have well-defined semantics and SHOULD be avoided if possible.
	RequestBody *RequestBody `yaml:"requestBody,omitempty"`

	// Responses is the list of possible responses as they are returned from
	// executing this operation.
	Responses map[string]*Response `yaml:"responses,omitempty"`

	// Callbacks is a map of possible out-of band callbacks related to the parent
	// operation. The key is a unique identifier for the Callback Object. Each
	// value in the map is a Callback Object that describes a request that may be
	// initiated by the API provider and the expected responses.
	Callbacks map[string]*PathItem `yaml:"callbacks,omitempty"`

	// Deprecated declares this operation to be deprecated. Consumers SHOULD
	// refrain from usage of the declared operation. Default value is false.
	Deprecated bool `yaml:"deprecated,omitempty"`

	// Security is a declaration of which security mechanisms can be used for this
	// operation. The list of values includes alternative security requirement
	// objects that can be used. Only one of the security requirement objects need
	// to be satisfied to authorize a request. To make security optional, an empty
	// security requirement ({}) can be included in the array. This definition
	// overrides any declared top-level security. To remove a top-level security
	// declaration, an empty array can be used.
	Security []map[string][]string `yaml:"security,omitempty"`

	// Servers is an alternative server array to service this operation. If an
	// alternative server object is specified at the Path Item Object or Root
	// level, it will be overridden by this value.
	Servers []*Server `yaml:"servers,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// PathItem describes the operations available on a single path. A Path Item MAY
// be empty, due to ACL constraints. The path itself is still exposed to the
// documentation viewer but they will not know which operations and parameters
// are available.
//
//	get:
//	  description: Returns pets based on ID
//	  summary: Find pets by ID
//	  operationId: getPetsById
//	  responses:
//	    '200':
//	      description: pet response
//	      content:
//	        '*/*' :
//	          schema:
//	            type: array
//	            items:
//	              $ref: '#/components/schemas/Pet'
//	    default:
//	      description: error payload
//	      content:
//	        'text/html':
//	          schema:
//	            $ref: '#/components/schemas/ErrorModel'
//	parameters:
//	- name: id
//	  in: path
//	  description: ID of pet to use
//	  required: true
//	  schema:
//	    type: array
//	    items:
//	      type: string
//	  style: simple
type PathItem struct {
	// Ref is a reference to another example. This field is mutually exclusive
	// with the other fields.
	Ref string `yaml:"$ref,omitempty"`

	// Summary is an optional, string summary, intended to apply to all operations
	// in this path.
	Summary string `yaml:"summary,omitempty"`

	// Description is an optional, string description, intended to apply to all
	// operations in this path. CommonMark syntax MAY be used for rich text
	// representation.
	Description string `yaml:"description,omitempty"`

	// Get is a definition of a GET operation on this path.
	Get *Operation `yaml:"get,omitempty"`

	// Put is a definition of a PUT operation on this path.
	Put *Operation `yaml:"put,omitempty"`

	// Post is a definition of a POST operation on this path.
	Post *Operation `yaml:"post,omitempty"`

	// Delete is a definition of a DELETE operation on this path.
	Delete *Operation `yaml:"delete,omitempty"`

	// Options is a definition of a OPTIONS operation on this path.
	Options *Operation `yaml:"options,omitempty"`

	// Head is a definition of a HEAD operation on this path.
	Head *Operation `yaml:"head,omitempty"`

	// Patch is a definition of a PATCH operation on this path.
	Patch *Operation `yaml:"patch,omitempty"`

	// Trace is a definition of a TRACE operation on this path.
	Trace *Operation `yaml:"trace,omitempty"`

	// Servers is an alternative server array to service all operations in this
	// path.
	Servers []*Server `yaml:"servers,omitempty"`

	// Parameters is a list of parameters that are applicable for all the
	// operations described under this path. These parameters can be overridden at
	// the operation level, but cannot be removed there. The list MUST NOT include
	// duplicated parameters. A unique parameter is defined by a combination of a
	// name and location. The list can use the Reference Object to link to
	// parameters that are defined at the OpenAPI Object’s components/parameters.
	Parameters []*Param `yaml:"parameters,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// OAuthFlow stores configuration details for a supported OAuth Flow.
//
//	type: oauth2
//	flows:
//	  implicit:
//	    authorizationUrl: https://example.com/api/oauth/dialog
//	    scopes:
//	      write:pets: modify pets in your account
//	      read:pets: read your pets
//	  authorizationCode:
//	    authorizationUrl: https://example.com/api/oauth/dialog
//	    tokenUrl: https://example.com/api/oauth/token
//	    scopes:
//	      write:pets: modify pets in your account
//	      read:pets: read your pets
type OAuthFlow struct {
	// AuthorizationURL is REQUIRED. The authorization URL to be used for this
	// flow. This MUST be in the form of a URL. The OAuth2 standard requires the
	// use of TLS.
	AuthorizationURL string `yaml:"authorizationUrl"`

	// TokenURL is REQUIRED. The token URL to be used for this flow. This MUST be
	// in the form of a URL. The OAuth2 standard requires the use of TLS.
	TokenURL string `yaml:"tokenUrl"`

	// RefreshURL is the URL to be used for obtaining refresh tokens. This MUST be
	// in the form of a URL. The OAuth2 standard requires the use of TLS.
	RefreshURL string `yaml:"refreshUrl,omitempty"`

	// Scopes are REQUIRED. The available scopes for the OAuth2 security scheme. A
	// map between the scope name and a short description for it. The map MAY be
	// empty.
	Scopes map[string]string `yaml:"scopes"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// OAuthFlows allows configuration of the supported OAuth Flows.
type OAuthFlows struct {
	// Implicit is the configuration for the OAuth Implicit flow.
	Implicit *OAuthFlow `yaml:"implicit,omitempty"`

	// Password is the configuration for the OAuth Resource Owner Password flow.
	Password *OAuthFlow `yaml:"password,omitempty"`

	// ClientCredentials is the configuration for the OAuth Client Credentials
	// flow. Previously called application in OpenAPI 2.0.
	ClientCredentials *OAuthFlow `yaml:"clientCredentials,omitempty"`

	// AuthorizationCode is the configuration for the OAuth Authorization Code
	// flow. Previously called accessCode in OpenAPI 2.0.
	AuthorizationCode *OAuthFlow `yaml:"authorizationCode,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// SecurityScheme defines a security scheme that can be used by the operations.
//
// Supported schemes are HTTP authentication, an API key (either as a header, a
// cookie parameter or as a query parameter), mutual TLS (use of a client
// certificate), OAuth2’s common flows (implicit, password, client credentials
// and authorization code) as defined in [RFC6749], and OpenID Connect
// Discovery. Please note that as of 2020, the implicit flow is about to be
// deprecated by OAuth 2.0 Security Best Current Practice. Recommended for most
// use case is Authorization Code Grant flow with PKCE.
//
//	type: http
//	scheme: bearer
//	bearerFormat: JWT
type SecurityScheme struct {
	// Type is REQUIRED. The type of the security scheme. Valid values are
	// "apiKey", "http", "mutualTLS", "oauth2", "openIdConnect".
	Type string `yaml:"type"`

	// Description for security scheme. CommonMark syntax MAY be used for rich
	// text representation.
	Description string `yaml:"description,omitempty"`

	// Name is REQUIRED. The name of the header, query or cookie parameter to be
	// used.
	Name string `yaml:"name,omitempty"`

	// In is REQUIRED. The location of the API key. Valid values are "query",
	// "header" or "cookie".
	In string `yaml:"in,omitempty"`

	// Scheme is REQUIRED. The name of the HTTP Authorization scheme to be used in
	// the Authorization header as defined in [RFC7235]. The values used SHOULD be
	// registered in the IANA Authentication Scheme registry.
	Scheme string `yaml:"scheme,omitempty"`

	// BearerFormat is a hint to the client to identify how the bearer token is
	// formatted. Bearer tokens are usually generated by an authorization server,
	// so this information is primarily for documentation purposes.
	BearerFormat string `yaml:"bearerFormat,omitempty"`

	// Flows is REQUIRED. An object containing configuration information for the
	// flow types supported.
	Flows *OAuthFlows `yaml:"flows,omitempty"`

	// OpenIDConnectURL is REQUIRED. OpenId Connect URL to discover OAuth2
	// configuration values. This MUST be in the form of a URL. The OpenID Connect
	// standard requires the use of TLS.
	OpenIDConnectURL string `yaml:"openIdConnectUrl,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Components holds a set of reusable objects for different aspects of the OAS.
// All objects defined within the components object will have no effect on the
// API unless they are explicitly referenced from properties outside the
// components object.
//
//	components:
//	  schemas:
//	    GeneralError:
//	      type: object
//	      properties:
//	        code:
//	          type: integer
//	          format: int32
//	        message:
//	          type: string
//	    Category:
//	      type: object
//	      properties:
//	        id:
//	          type: integer
//	          format: int64
//	        name:
//	          type: string
//	    Tag:
//	      type: object
//	      properties:
//	        id:
//	          type: integer
//	          format: int64
//	        name:
//	          type: string
//	  parameters:
//	    skipParam:
//	      name: skip
//	      in: query
//	      description: number of items to skip
//	      required: true
//	      schema:
//	        type: integer
//	        format: int32
//	    limitParam:
//	      name: limit
//	      in: query
//	      description: max records to return
//	      required: true
//	      schema:
//	        type: integer
//	        format: int32
//	  responses:
//	    NotFound:
//	      description: Entity not found.
//	    IllegalInput:
//	      description: Illegal input for operation.
//	    GeneralError:
//	      description: General Error
//	      content:
//	        application/json:
//	          schema:
//	            $ref: '#/components/schemas/GeneralError'
//	  securitySchemes:
//	    api_key:
//	      type: apiKey
//	      name: api_key
//	      in: header
//	    petstore_auth:
//	      type: oauth2
//	      flows:
//	        implicit:
//	          authorizationUrl: https://example.org/api/oauth/dialog
//	          scopes:
//	            write:pets: modify pets in your account
//	            read:pets: read your pets
type Components struct {
	// Schemas is an object to hold reusable Schema Objects.
	Schemas Registry `yaml:"schemas,omitempty"`

	// Responses is an object to hold reusable Response Objects.
	Responses map[string]*Response `yaml:"responses,omitempty"`

	// Parameters is an object to hold reusable Parameter Objects.
	Parameters map[string]*Param `yaml:"parameters,omitempty"`

	// Examples is an object to hold reusable Example Objects.
	Examples map[string]*Example `yaml:"examples,omitempty"`

	// RequestBodies is an object to hold reusable Request Body Objects.
	RequestBodies map[string]*RequestBody `yaml:"requestBodies,omitempty"`

	// Headers is an object to hold reusable Header Objects.
	Headers map[string]*Header `yaml:"headers,omitempty"`

	// SecuritySchemes is an object to hold reusable Security Scheme Objects.
	SecuritySchemes map[string]*SecurityScheme `yaml:"securitySchemes,omitempty"`

	// Links is an object to hold reusable Link Objects.
	Links map[string]*Link `yaml:"links,omitempty"`

	// Callbacks is an object to hold reusable Callback Objects.
	Callbacks map[string]*PathItem `yaml:"callbacks,omitempty"`

	// PathItems is an object to hold reusable Path Item Objects.
	PathItems map[string]*PathItem `yaml:"pathItems,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// ExternalDocs allows referencing an external resource for extended
// documentation.
//
//	description: Find more info here
//	url: https://example.com
type ExternalDocs struct {
	// Description of the target documentation. CommonMark syntax MAY be used for
	// rich text representation.
	Description string `yaml:"description,omitempty"`

	// URL is REQUIRED. The URL for the target documentation. Value MUST be in the
	// format of a URL.
	URL string `yaml:"url"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

// Tag adds metadata to a single tag that is used by the Operation Object. It is
// not mandatory to have a Tag Object per tag defined in the Operation Object
// instances.
type Tag struct {
	// Name is REQUIRED. The name of the tag.
	Name string `yaml:"name"`

	// Description for the tag. CommonMark syntax MAY be used for rich text
	// representation.
	Description string `yaml:"description,omitempty"`

	// ExternalDocs is additional external documentation for this tag.
	ExternalDocs *ExternalDocs `yaml:"externalDocs,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`
}

type AddOpFunc func(oapi *OpenAPI, op *Operation)

// OpenAPI is the root object of the OpenAPI document.
type OpenAPI struct { //nolint: musttag
	// OpenAPI is REQUIRED. This string MUST be the version number of the OpenAPI
	// Specification that the OpenAPI document uses. The openapi field SHOULD be
	// used by tooling to interpret the OpenAPI document. This is not related to
	// the API info.version string.
	OpenAPI string `yaml:"openapi"`

	// Info is REQUIRED. Provides metadata about the API. The metadata MAY be used
	// by tooling as required.
	Info *Info `yaml:"info"`

	// JSONSchemaDialect is he default value for the $schema keyword within Schema
	// Objects contained within this OAS document. This MUST be in the form of a
	// URI.
	JSONSchemaDialect string `yaml:"jsonSchemaDialect,omitempty"`

	// Servers is an array of Server Objects, which provide connectivity
	// information to a target server. If the servers property is not provided, or
	// is an empty array, the default value would be a Server Object with a url
	// value of /.
	Servers []*Server `yaml:"servers,omitempty"`

	// Paths are the available paths and operations for the API.
	Paths map[string]*PathItem `yaml:"paths,omitempty"`

	// Webhooks that MAY be received as part of this API and that the API consumer
	// MAY choose to implement. Closely related to the callbacks feature, this
	// section describes requests initiated other than by an API call, for example
	// by an out of band registration. The key name is a unique string to refer to
	// each webhook, while the (optionally referenced) Path Item Object describes
	// a request that may be initiated by the API provider and the expected
	// responses. An example is available.
	Webhooks map[string]*PathItem `yaml:"webhooks,omitempty"`

	// Components is an element to hold various schemas for the document.
	Components *Components `yaml:"components,omitempty"`

	// Security is a declaration of which security mechanisms can be used across
	// the API. The list of values includes alternative security requirement
	// objects that can be used. Only one of the security requirement objects need
	// to be satisfied to authorize a request. Individual operations can override
	// this definition. To make security optional, an empty security requirement
	// ({}) can be included in the array.
	Security []map[string][]string `yaml:"security,omitempty"`

	// Tags are a list of tags used by the document with additional metadata. The
	// order of the tags can be used to reflect on their order by the parsing
	// tools. Not all tags that are used by the Operation Object must be declared.
	// The tags that are not declared MAY be organized randomly or based on the
	// tools’ logic. Each tag name in the list MUST be unique.
	Tags []*Tag `yaml:"tags,omitempty"`

	// ExternalDocs is additional external documentation.
	ExternalDocs *ExternalDocs `yaml:"externalDocs,omitempty"`

	// Extensions (user-defined properties), if any. Values in this map will
	// be marshalled as siblings of the other properties above.
	Extensions map[string]any `yaml:",inline"`

	// OnAddOperation is called when an operation is added to the OpenAPI via
	// `AddOperation`. You may bypass this by directly writing to the `Paths`
	// map instead.
	OnAddOperation []AddOpFunc `yaml:"-"`
}

// AddOperation adds an operation to the OpenAPI. This is the preferred way to
// add operations to the OpenAPI, as it will ensure that the operation is
// properly added to the Paths map, and will call any registered OnAddOperation
// functions.
func (o *OpenAPI) AddOperation(op *Operation) {
	if o.Paths == nil {
		o.Paths = map[string]*PathItem{}
	}

	item := o.Paths[op.Path]
	if item == nil {
		item = &PathItem{}
		o.Paths[op.Path] = item
	}

	switch op.Method {
	case http.MethodGet:
		item.Get = op
	case http.MethodPost:
		item.Post = op
	case http.MethodPut:
		item.Put = op
	case http.MethodPatch:
		item.Patch = op
	case http.MethodDelete:
		item.Delete = op
	case http.MethodHead:
		item.Head = op
	case http.MethodOptions:
		item.Options = op
	case http.MethodTrace:
		item.Trace = op
	default:
		panic(fmt.Sprintf("unknown method %s", op.Method))
	}

	for _, f := range o.OnAddOperation {
		f(o, op)
	}
}

func (o *OpenAPI) MarshalJSON() ([]byte, error) {
	// JSON doesn't support the `,inline` field tag, so we go through the YAML
	// marshaller instead. It's not quite as fast, but this operation should
	// only happen once on server load.
	// Note: it does mean the individual structs above cannot be marshalled
	// directly to JSON - you must marshal the entire OpenAPI struct with the
	// exception of individual schemas.
	return yaml.MarshalWithOptions(o, yaml.JSON())
}
