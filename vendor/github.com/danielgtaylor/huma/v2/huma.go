// Package huma provides a framework for building REST APIs in Go. It is
// designed to be simple, fast, and easy to use. It is also designed to
// generate OpenAPI 3.1 specifications and JSON Schema documents
// describing the API and providing a quick & easy way to generate
// docs, mocks, SDKs, CLI clients, and more.
//
// https://huma.rocks/
package huma

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/exp/slices"
)

var errDeadlineUnsupported = fmt.Errorf("%w", http.ErrNotSupported)

var bodyCallbackType = reflect.TypeOf(func(Context) {})

// SetReadDeadline is a utility to set the read deadline on a response writer,
// if possible. If not, it will not incur any allocations (unlike the stdlib
// `http.ResponseController`). This is mostly a convenience function for
// adapters so they can be more efficient.
//
//	huma.SetReadDeadline(w, time.Now().Add(5*time.Second))
func SetReadDeadline(w http.ResponseWriter, deadline time.Time) error {
	for {
		switch t := w.(type) {
		case interface{ SetReadDeadline(time.Time) error }:
			return t.SetReadDeadline(deadline)
		case interface{ Unwrap() http.ResponseWriter }:
			w = t.Unwrap()
		default:
			return errDeadlineUnsupported
		}
	}
}

// StreamResponse is a response that streams data to the client. The body
// function will be called once the response headers have been written and
// the body writer is ready to be written to.
//
//	func handler(ctx context.Context, input *struct{}) (*huma.StreamResponse, error) {
//		return &huma.StreamResponse{
//			Body: func(ctx huma.Context) {
//				ctx.SetHeader("Content-Type", "text/my-type")
//
//				// Write some data to the stream.
//				writer := ctx.BodyWriter()
//				writer.Write([]byte("Hello "))
//
//				// Flush the stream to the client.
//				if f, ok := writer.(http.Flusher); ok {
//					f.Flush()
//				}
//
//				// Write some more...
//				writer.Write([]byte("world!"))
//			}
//		}
//	}
type StreamResponse struct {
	Body func(ctx Context)
}

type paramFieldInfo struct {
	Type       reflect.Type
	Name       string
	Loc        string
	Required   bool
	Default    string
	TimeFormat string
	Schema     *Schema
}

func findParams(registry Registry, op *Operation, t reflect.Type) *findResult[*paramFieldInfo] {
	return findInType(t, nil, func(f reflect.StructField, path []int) *paramFieldInfo {
		if f.Anonymous {
			return nil
		}

		if f.Type.Kind() == reflect.Pointer {
			// TODO: support pointers? The problem is that when we dynamically
			// create an instance of the input struct the `params.Every(...)`
			// call cannot set them as the value is `reflect.Invalid` unless
			// dynamically allocated, but we don't know when to allocate until
			// after the `Every` callback has run. Doable, but a bigger change.
			panic("pointers are not supported for path/query/header parameters")
		}

		pfi := &paramFieldInfo{
			Type:   f.Type,
			Schema: SchemaFromField(registry, f, ""),
		}

		var example any
		if e := f.Tag.Get("example"); e != "" {
			example = jsonTagValue(f, f.Type, f.Tag.Get("example"))
		}

		if def := f.Tag.Get("default"); def != "" {
			pfi.Default = def
		}

		var name string
		var explode *bool
		if p := f.Tag.Get("path"); p != "" {
			pfi.Loc = "path"
			name = p
			pfi.Required = true
		} else if q := f.Tag.Get("query"); q != "" {
			pfi.Loc = "query"
			name = q
			// If `in` is `query` then `explode` defaults to true. Parsing is *much*
			// easier if we use comma-separated values, so we disable explode.
			nope := false
			explode = &nope
		} else if h := f.Tag.Get("header"); h != "" {
			pfi.Loc = "header"
			name = h
		} else {
			return nil
		}

		// While discouraged, make it possible to make query/header params required.
		if r := f.Tag.Get("required"); r == "true" {
			pfi.Required = true
		}

		pfi.Name = name

		if f.Type == timeType {
			timeFormat := time.RFC3339Nano
			if pfi.Loc == "header" {
				timeFormat = http.TimeFormat
			}
			if f := f.Tag.Get("timeFormat"); f != "" {
				timeFormat = f
			}
			pfi.TimeFormat = timeFormat
		}

		if f.Tag.Get("hidden") == "" {
			// Document the parameter if not hidden.
			op.Parameters = append(op.Parameters, &Param{
				Name:     name,
				In:       pfi.Loc,
				Explode:  explode,
				Required: pfi.Required,
				Schema:   pfi.Schema,
				Example:  example,
			})
		}
		return pfi
	}, "Body")
}

func findResolvers(resolverType, t reflect.Type) *findResult[bool] {
	return findInType(t, func(t reflect.Type, path []int) bool {
		if reflect.PtrTo(t).Implements(resolverType) {
			return true
		}
		if reflect.PtrTo(t).Implements(resolverWithPathType) {
			return true
		}
		return false
	}, nil)
}

func findDefaults(t reflect.Type) *findResult[any] {
	return findInType(t, nil, func(sf reflect.StructField, i []int) any {
		if d := sf.Tag.Get("default"); d != "" {
			if sf.Type.Kind() == reflect.Pointer {
				panic("pointers cannot have default values")
			}
			return jsonTagValue(sf, sf.Type, d)
		}
		return nil
	})
}

type headerInfo struct {
	Field      reflect.StructField
	Name       string
	TimeFormat string
}

func findHeaders(t reflect.Type) *findResult[*headerInfo] {
	return findInType(t, nil, func(sf reflect.StructField, i []int) *headerInfo {
		header := sf.Tag.Get("header")
		if header == "" {
			header = sf.Name
		}
		timeFormat := ""
		if sf.Type == timeType {
			timeFormat = http.TimeFormat
			if f := sf.Tag.Get("timeFormat"); f != "" {
				timeFormat = f
			}
		}
		return &headerInfo{sf, header, timeFormat}
	}, "Status", "Body")
}

type findResultPath[T comparable] struct {
	Path  []int
	Value T
}

type findResult[T comparable] struct {
	Paths []findResultPath[T]
}

func (r *findResult[T]) every(current reflect.Value, path []int, v T, f func(reflect.Value, T)) {
	if current.Kind() == reflect.Invalid {
		// Indirect from below may have resulted in no value, for example
		// an optional field may have been omitted; just ignore it.
		return
	}

	if len(path) == 0 {
		f(current, v)
		return
	}

	switch current.Kind() {
	case reflect.Struct:
		r.every(reflect.Indirect(current.Field(path[0])), path[1:], v, f)
	case reflect.Slice:
		for j := 0; j < current.Len(); j++ {
			r.every(reflect.Indirect(current.Index(j)), path, v, f)
		}
	case reflect.Map:
		for _, k := range current.MapKeys() {
			r.every(reflect.Indirect(current.MapIndex(k)), path, v, f)
		}
	default:
		panic("unsupported")
	}
}

func (r *findResult[T]) Every(v reflect.Value, f func(reflect.Value, T)) {
	for i := range r.Paths {
		r.every(v, r.Paths[i].Path, r.Paths[i].Value, f)
	}
}

func jsonName(field reflect.StructField) string {
	name := strings.ToLower(field.Name)
	if jsonName := field.Tag.Get("json"); jsonName != "" {
		name = strings.Split(jsonName, ",")[0]
	}
	return name
}

func (r *findResult[T]) everyPB(current reflect.Value, path []int, pb *PathBuffer, v T, f func(reflect.Value, T)) {
	if current.Kind() == reflect.Invalid {
		// Indirect from below may have resulted in no value, for example
		// an optional field may have been omitted; just ignore it.
		return
	}
	switch current.Kind() {
	case reflect.Struct:
		if len(path) == 0 {
			f(current, v)
			return
		}
		field := current.Type().Field(path[0])
		pops := 0
		if !field.Anonymous {
			// The path name can come from one of four places: path parameter,
			// query parameter, header parameter, or body field.
			// TODO: pre-compute type/field names? Could save a few allocations.
			pops++
			if path := field.Tag.Get("path"); path != "" && pb.Len() == 0 {
				pb.Push("path")
				pb.Push(path)
				pops++
			} else if query := field.Tag.Get("query"); query != "" && pb.Len() == 0 {
				pb.Push("query")
				pb.Push(query)
				pops++
			} else if header := field.Tag.Get("header"); header != "" && pb.Len() == 0 {
				pb.Push("header")
				pb.Push(header)
				pops++
			} else {
				// The body is _always_ in a field called "Body", which turns into
				// `body` in the path buffer, so we don't need to push it separately
				// like the the params fields above.
				pb.Push(jsonName(field))
			}
		}
		r.everyPB(reflect.Indirect(current.Field(path[0])), path[1:], pb, v, f)
		for i := 0; i < pops; i++ {
			pb.Pop()
		}
	case reflect.Slice:
		for j := 0; j < current.Len(); j++ {
			pb.PushIndex(j)
			r.everyPB(reflect.Indirect(current.Index(j)), path, pb, v, f)
			pb.Pop()
		}
	case reflect.Map:
		for _, k := range current.MapKeys() {
			if k.Kind() == reflect.String {
				pb.Push(k.String())
			} else {
				pb.Push(fmt.Sprintf("%v", k.Interface()))
			}
			r.everyPB(reflect.Indirect(current.MapIndex(k)), path, pb, v, f)
			pb.Pop()
		}
	default:
		if len(path) == 0 {
			f(current, v)
			return
		}
		panic("unsupported")
	}
}

func (r *findResult[T]) EveryPB(pb *PathBuffer, v reflect.Value, f func(reflect.Value, T)) {
	for i := range r.Paths {
		pb.Reset()
		r.everyPB(v, r.Paths[i].Path, pb, r.Paths[i].Value, f)
	}
}

func findInType[T comparable](t reflect.Type, onType func(reflect.Type, []int) T, onField func(reflect.StructField, []int) T, ignore ...string) *findResult[T] {
	result := &findResult[T]{}
	_findInType(t, []int{}, result, onType, onField, ignore...)
	return result
}

func _findInType[T comparable](t reflect.Type, path []int, result *findResult[T], onType func(reflect.Type, []int) T, onField func(reflect.StructField, []int) T, ignore ...string) {
	t = deref(t)
	zero := reflect.Zero(reflect.TypeOf((*T)(nil)).Elem()).Interface()

	if onType != nil {
		if v := onType(t, path); v != zero {
			result.Paths = append(result.Paths, findResultPath[T]{path, v})
		}
	}

	switch t.Kind() {
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				continue
			}
			if slices.Contains(ignore, f.Name) {
				continue
			}
			fi := append([]int{}, path...)
			fi = append(fi, i)
			if onField != nil {
				if v := onField(f, fi); v != zero {
					result.Paths = append(result.Paths, findResultPath[T]{fi, v})
				}
			}
			_findInType(f.Type, fi, result, onType, onField, ignore...)
		}
	case reflect.Slice:
		_findInType(t.Elem(), path, result, onType, onField, ignore...)
	case reflect.Map:
		_findInType(t.Elem(), path, result, onType, onField, ignore...)
	}
}

func getHint(parent reflect.Type, name string, other string) string {
	if parent.Name() != "" {
		return parent.Name() + name
	} else {
		return other
	}
}

type validateDeps struct {
	pb  *PathBuffer
	res *ValidateResult
}

var validatePool = sync.Pool{
	New: func() any {
		return &validateDeps{
			pb:  &PathBuffer{buf: make([]byte, 0, 128)},
			res: &ValidateResult{},
		}
	},
}

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 128))
	},
}

// transformAndWrite is a utility function to transform and write a response.
// It is best-effort as the status code and headers may have already been sent.
func transformAndWrite(api API, ctx Context, status int, ct string, body any) {
	// Try to transform and then marshal/write the response.
	// Status code was already sent, so just log the error if something fails,
	// and do our best to stuff it into the body of the response.
	tval, terr := api.Transform(ctx, strconv.Itoa(status), body)
	if terr != nil {
		ctx.BodyWriter().Write([]byte("error transforming response"))
		panic(fmt.Sprintf("error transforming response %+v for %s %s %d: %s\n", tval, ctx.Operation().Method, ctx.Operation().Path, status, terr.Error()))
	}
	ctx.SetStatus(status)
	if merr := api.Marshal(ctx.BodyWriter(), ct, tval); merr != nil {
		ctx.BodyWriter().Write([]byte("error marshaling response"))
		panic(fmt.Sprintf("error marshaling response %+v for %s %s %d: %s\n", tval, ctx.Operation().Method, ctx.Operation().Path, status, merr.Error()))
	}
}

func parseArrElement[T any](values []string, parse func(string) (T, error)) ([]T, error) {
	result := make([]T, 0, len(values))

	for i := 0; i < len(values); i++ {
		v, err := parse(values[i])
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}

	return result, nil
}

// Register an operation handler for an API. The handler must be a function that
// takes a context and a pointer to the input struct and returns a pointer to the
// output struct and an error. The input struct must be a struct with fields
// for the request path/query/header parameters and/or body. The output struct
// must be a  struct with fields for the output headers and body of the
// operation, if any.
//
//	huma.Register(api, huma.Operation{
//		OperationID: "get-greeting",
//		Method:      http.MethodGet,
//		Path:        "/greeting/{name}",
//		Summary:     "Get a greeting",
//	}, func(ctx context.Context, input *GreetingInput) (*GreetingOutput, error) {
//		if input.Name == "bob" {
//			return nil, huma.Error404NotFound("no greeting for bob")
//		}
//		resp := &GreetingOutput{}
//		resp.MyHeader = "MyValue"
//		resp.Body.Message = fmt.Sprintf("Hello, %s!", input.Name)
//		return resp, nil
//	})
func Register[I, O any](api API, op Operation, handler func(context.Context, *I) (*O, error)) {
	oapi := api.OpenAPI()
	registry := oapi.Components.Schemas

	if op.Method == "" || op.Path == "" {
		panic("method and path must be specified in operation")
	}

	inputType := reflect.TypeOf((*I)(nil)).Elem()
	if inputType.Kind() != reflect.Struct {
		panic("input must be a struct")
	}
	inputParams := findParams(registry, &op, inputType)
	inputBodyIndex := -1
	if f, ok := inputType.FieldByName("Body"); ok {
		inputBodyIndex = f.Index[0]
		if op.RequestBody == nil {
			required := f.Type.Kind() != reflect.Ptr && f.Type.Kind() != reflect.Interface
			if f.Tag.Get("required") == "true" {
				required = true
			}

			contentType := "application/json"
			if c := f.Tag.Get("contentType"); c != "" {
				contentType = c
			}

			s := SchemaFromField(registry, f, getHint(inputType, f.Name, op.OperationID+"Request"))

			op.RequestBody = &RequestBody{
				Required: required,
				Content: map[string]*MediaType{
					contentType: {
						Schema: s,
					},
				},
			}
		}

		if op.BodyReadTimeout == 0 {
			// 5 second default
			op.BodyReadTimeout = 5 * time.Second
		}

		if op.MaxBodyBytes == 0 {
			// 1 MB default
			op.MaxBodyBytes = 1024 * 1024
		}
	}
	rawBodyIndex := -1
	if f, ok := inputType.FieldByName("RawBody"); ok {
		rawBodyIndex = f.Index[0]
		if op.RequestBody == nil {
			contentType := "application/octet-stream"
			if c := f.Tag.Get("contentType"); c != "" {
				contentType = c
			}

			op.RequestBody = &RequestBody{
				Required: true,
				Content: map[string]*MediaType{
					contentType: {
						Schema: &Schema{
							Type:   "string",
							Format: "binary",
						},
					},
				},
			}
		}
	}

	var inSchema *Schema
	if op.RequestBody != nil && op.RequestBody.Content != nil && op.RequestBody.Content["application/json"] != nil && op.RequestBody.Content["application/json"].Schema != nil {
		inSchema = op.RequestBody.Content["application/json"].Schema
	}

	resolvers := findResolvers(resolverType, inputType)
	defaults := findDefaults(inputType)

	if op.Responses == nil {
		op.Responses = map[string]*Response{}
	}
	outputType := reflect.TypeOf((*O)(nil)).Elem()
	if outputType.Kind() != reflect.Struct {
		panic("output must be a struct")
	}

	outStatusIndex := -1
	if f, ok := outputType.FieldByName("Status"); ok {
		outStatusIndex = f.Index[0]
		if f.Type.Kind() != reflect.Int {
			panic("status field must be an int")
		}
		// TODO: enum tag?
		// TODO: register each of the possible responses with the right model
		//       and headers down below.
	}
	outHeaders := findHeaders(outputType)
	outBodyIndex := -1
	outBodyFunc := false
	if f, ok := outputType.FieldByName("Body"); ok {
		outBodyIndex = f.Index[0]
		if f.Type.Kind() == reflect.Func {
			outBodyFunc = true

			if f.Type != bodyCallbackType {
				panic("body field must be a function with signature func(huma.Context)")
			}
		}
		status := op.DefaultStatus
		if status == 0 {
			status = http.StatusOK
		}
		statusStr := strconv.Itoa(status)
		if op.Responses[statusStr] == nil {
			op.Responses[statusStr] = &Response{}
		}
		if op.Responses[statusStr].Description == "" {
			op.Responses[statusStr].Description = http.StatusText(status)
		}
		if op.Responses[statusStr].Headers == nil {
			op.Responses[statusStr].Headers = map[string]*Param{}
		}
		if !outBodyFunc {
			outSchema := SchemaFromField(registry, f, getHint(outputType, f.Name, op.OperationID+"Response"))
			if op.Responses[statusStr].Content == nil {
				op.Responses[statusStr].Content = map[string]*MediaType{}
			}
			if _, ok := op.Responses[statusStr].Content["application/json"]; !ok {
				op.Responses[statusStr].Content["application/json"] = &MediaType{}
			}
			if op.Responses[statusStr].Content["application/json"].Schema == nil {
				op.Responses[statusStr].Content["application/json"].Schema = outSchema
			}
		}
	}
	if op.DefaultStatus == 0 {
		if outBodyIndex != -1 {
			op.DefaultStatus = http.StatusOK
		} else {
			op.DefaultStatus = http.StatusNoContent
		}
	}
	defaultStatusStr := strconv.Itoa(op.DefaultStatus)
	if op.Responses[defaultStatusStr] == nil {
		op.Responses[defaultStatusStr] = &Response{
			Description: http.StatusText(op.DefaultStatus),
		}
	}
	for _, entry := range outHeaders.Paths {
		// Document the header's name and type.
		if op.Responses[defaultStatusStr].Headers == nil {
			op.Responses[defaultStatusStr].Headers = map[string]*Param{}
		}
		v := entry.Value
		op.Responses[defaultStatusStr].Headers[v.Name] = &Header{
			// We need to generate the schema from the field to get validation info
			// like min/max and enums. Useful to let the client know possible values.
			Schema: SchemaFromField(registry, v.Field, getHint(outputType, v.Field.Name, op.OperationID+defaultStatusStr+v.Name)),
		}
	}

	if len(op.Errors) > 0 && (len(inputParams.Paths) > 0 || inputBodyIndex >= -1) {
		op.Errors = append(op.Errors, http.StatusUnprocessableEntity)
	}
	if len(op.Errors) > 0 {
		op.Errors = append(op.Errors, http.StatusInternalServerError)
	}

	exampleErr := NewError(0, "")
	errContentType := "application/json"
	if ctf, ok := exampleErr.(ContentTypeFilter); ok {
		errContentType = ctf.ContentType(errContentType)
	}
	errType := reflect.TypeOf(exampleErr)
	errSchema := registry.Schema(errType, true, getHint(errType, "", "Error"))
	for _, code := range op.Errors {
		op.Responses[strconv.Itoa(code)] = &Response{
			Description: http.StatusText(code),
			Content: map[string]*MediaType{
				errContentType: {
					Schema: errSchema,
				},
			},
		}
	}
	if len(op.Responses) <= 1 && len(op.Errors) == 0 {
		// No errors are defined, so set a default response.
		op.Responses["default"] = &Response{
			Description: "Error",
			Content: map[string]*MediaType{
				errContentType: {
					Schema: errSchema,
				},
			},
		}
	}

	if !op.Hidden {
		oapi.AddOperation(&op)
	}

	a := api.Adapter()

	a.Handle(&op, api.Middlewares().Handler(func(ctx Context) {
		var input I

		// Get the validation dependencies from the shared pool.
		deps := validatePool.Get().(*validateDeps)
		defer func() {
			deps.pb.Reset()
			deps.res.Reset()
			validatePool.Put(deps)
		}()
		pb := deps.pb
		res := deps.res

		errStatus := http.StatusUnprocessableEntity

		v := reflect.ValueOf(&input).Elem()
		inputParams.Every(v, func(f reflect.Value, p *paramFieldInfo) {
			var value string
			switch p.Loc {
			case "path":
				value = ctx.Param(p.Name)
			case "query":
				value = ctx.Query(p.Name)
			case "header":
				value = ctx.Header(p.Name)
			}

			pb.Reset()
			pb.Push(p.Loc)
			pb.Push(p.Name)

			if value == "" && p.Default != "" {
				value = p.Default
			}

			if p.Required && value == "" {
				// Path params are always required.
				res.Add(pb, "", "required "+p.Loc+" parameter is missing")
				return
			}

			if value != "" {
				var pv any

				switch p.Type.Kind() {
				case reflect.String:
					f.SetString(value)
					pv = value
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
					v, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						res.Add(pb, value, "invalid integer")
						return
					}
					f.SetInt(v)
					pv = v
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
					v, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						res.Add(pb, value, "invalid integer")
						return
					}
					f.SetUint(v)
					pv = v
				case reflect.Float32, reflect.Float64:
					v, err := strconv.ParseFloat(value, 64)
					if err != nil {
						res.Add(pb, value, "invalid float")
						return
					}
					f.SetFloat(v)
					pv = v
				case reflect.Bool:
					v, err := strconv.ParseBool(value)
					if err != nil {
						res.Add(pb, value, "invalid boolean")
						return
					}
					f.SetBool(v)
					pv = v
				default:
					if f.Type().Kind() == reflect.Slice {
						switch f.Type().Elem().Kind() {

						case reflect.String:
							values := strings.Split(value, ",")
							f.Set(reflect.ValueOf(values))
							pv = values

						case reflect.Int:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (int, error) {
								val, err := strconv.ParseInt(s, 10, strconv.IntSize)
								if err != nil {
									return 0, err
								}
								return int(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Int8:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (int8, error) {
								val, err := strconv.ParseInt(s, 10, 8)
								if err != nil {
									return 0, err
								}
								return int8(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Int16:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (int16, error) {
								val, err := strconv.ParseInt(s, 10, 16)
								if err != nil {
									return 0, err
								}
								return int16(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Int32:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (int32, error) {
								val, err := strconv.ParseInt(s, 10, 32)
								if err != nil {
									return 0, err
								}
								return int32(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Int64:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (int64, error) {
								val, err := strconv.ParseInt(s, 10, 64)
								if err != nil {
									return 0, err
								}
								return int64(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Uint:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (uint, error) {
								val, err := strconv.ParseUint(s, 10, strconv.IntSize)
								if err != nil {
									return 0, err
								}
								return uint(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Uint16:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (uint16, error) {
								val, err := strconv.ParseUint(s, 10, 16)
								if err != nil {
									return 0, err
								}
								return uint16(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Uint32:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (uint32, error) {
								val, err := strconv.ParseUint(s, 10, 32)
								if err != nil {
									return 0, err
								}
								return uint32(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Uint64:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (uint64, error) {
								val, err := strconv.ParseUint(s, 10, 64)
								if err != nil {
									return 0, err
								}
								return uint64(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid integer")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Float32:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (float32, error) {
								val, err := strconv.ParseFloat(s, 32)
								if err != nil {
									return 0, err
								}
								return float32(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid floating value")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs

						case reflect.Float64:
							values := strings.Split(value, ",")
							vs, err := parseArrElement(values, func(s string) (float64, error) {
								val, err := strconv.ParseFloat(s, 64)
								if err != nil {
									return 0, err
								}
								return float64(val), nil
							})
							if err != nil {
								res.Add(pb, value, "invalid floating value")
								return
							}
							f.Set(reflect.ValueOf(vs))
							pv = vs
						}
						break
					}

					// Special case: time.Time
					if f.Type() == timeType {
						t, err := time.Parse(p.TimeFormat, value)
						if err != nil {
							res.Add(pb, value, "invalid date/time for format "+p.TimeFormat)
							return
						}
						f.Set(reflect.ValueOf(t))
						pv = value
						break
					}

					panic("unsupported param type " + p.Type.String())
				}

				if !op.SkipValidateParams {
					Validate(oapi.Components.Schemas, p.Schema, pb, ModeWriteToServer, pv, res)
				}
			}
		})

		// Read input body if defined.
		if inputBodyIndex != -1 || rawBodyIndex != -1 {
			if op.BodyReadTimeout > 0 {
				ctx.SetReadDeadline(time.Now().Add(op.BodyReadTimeout))
			} else if op.BodyReadTimeout < 0 {
				// Disable any server-wide deadline.
				ctx.SetReadDeadline(time.Time{})
			}

			buf := bufPool.Get().(*bytes.Buffer)
			reader := ctx.BodyReader()
			if reader == nil {
				reader = bytes.NewReader(nil)
			}
			if closer, ok := reader.(io.Closer); ok {
				defer closer.Close()
			}
			if op.MaxBodyBytes > 0 {
				reader = io.LimitReader(reader, op.MaxBodyBytes)
			}
			count, err := io.Copy(buf, reader)
			if op.MaxBodyBytes > 0 {
				if count == op.MaxBodyBytes {
					buf.Reset()
					bufPool.Put(buf)
					WriteErr(api, ctx, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body is too large limit=%d bytes", op.MaxBodyBytes), res.Errors...)
					return
				}
			}
			if err != nil {
				buf.Reset()
				bufPool.Put(buf)

				if e, ok := err.(net.Error); ok && e.Timeout() {
					WriteErr(api, ctx, http.StatusRequestTimeout, "request body read timeout", res.Errors...)
					return
				}

				WriteErr(api, ctx, http.StatusInternalServerError, "cannot read request body", err)
				return
			}
			body := buf.Bytes()

			if rawBodyIndex != -1 {
				f := v.Field(rawBodyIndex)
				f.SetBytes(body)
			}

			if len(body) == 0 {
				if op.RequestBody != nil && op.RequestBody.Required {
					buf.Reset()
					bufPool.Put(buf)
					WriteErr(api, ctx, http.StatusBadRequest, "request body is required", res.Errors...)
					return
				}
			} else {
				parseErrCount := 0
				if inputBodyIndex != -1 && !op.SkipValidateBody {
					// Validate the input. First, parse the body into []any or map[string]any
					// or equivalent, which can be easily validated. Then, convert to the
					// expected struct type to call the handler.
					var parsed any
					if err := api.Unmarshal(ctx.Header("Content-Type"), body, &parsed); err != nil {
						errStatus = http.StatusBadRequest
						if errors.Is(err, ErrUnknownContentType) {
							errStatus = http.StatusUnsupportedMediaType
						}
						res.Errors = append(res.Errors, &ErrorDetail{
							Location: "body",
							Message:  err.Error(),
							Value:    body,
						})
						parseErrCount++
					} else {
						pb.Reset()
						pb.Push("body")
						count := len(res.Errors)
						Validate(oapi.Components.Schemas, inSchema, pb, ModeWriteToServer, parsed, res)
						parseErrCount = len(res.Errors) - count
						if parseErrCount > 0 {
							errStatus = http.StatusUnprocessableEntity
						}
					}
				}

				if inputBodyIndex != -1 {
					// We need to get the body into the correct type now that it has been
					// validated. Benchmarks on Go 1.20 show that using `json.Unmarshal` a
					// second time is faster than `mapstructure.Decode` or any of the other
					// common reflection-based approaches when using real-world medium-sized
					// JSON payloads with lots of strings.
					f := v.Field(inputBodyIndex)
					if err := api.Unmarshal(ctx.Header("Content-Type"), body, f.Addr().Interface()); err != nil {
						if parseErrCount == 0 {
							// Hmm, this should have worked... validator missed something?
							res.Errors = append(res.Errors, &ErrorDetail{
								Location: "body",
								Message:  err.Error(),
								Value:    string(body),
							})
						}
					} else {
						// Set defaults for any fields that were not in the input.
						defaults.Every(v, func(item reflect.Value, def any) {
							if item.IsZero() {
								item.Set(reflect.Indirect(reflect.ValueOf(def)))
							}
						})
					}
				}

				buf.Reset()
				bufPool.Put(buf)
			}
		}

		resolvers.EveryPB(pb, v, func(item reflect.Value, _ bool) {
			if resolver, ok := item.Addr().Interface().(Resolver); ok {
				if errs := resolver.Resolve(ctx); len(errs) > 0 {
					res.Errors = append(res.Errors, errs...)
				}
			} else if resolver, ok := item.Addr().Interface().(ResolverWithPath); ok {
				if errs := resolver.Resolve(ctx, pb); len(errs) > 0 {
					res.Errors = append(res.Errors, errs...)
				}
			} else {
				panic("matched resolver cannot be run, please file a bug")
			}
		})

		if len(res.Errors) > 0 {
			for i := len(res.Errors) - 1; i >= 0; i-- {
				// If there are errors, and they provide a status, then update the
				// response status code to match. Otherwise, use the default status
				// code is used. Since these run in order, the last error code wins.
				if s, ok := res.Errors[i].(StatusError); ok {
					errStatus = s.GetStatus()
					break
				}
			}
			WriteErr(api, ctx, errStatus, "validation failed", res.Errors...)
			return
		}

		output, err := handler(ctx.Context(), &input)
		if err != nil {
			status := http.StatusInternalServerError
			if se, ok := err.(StatusError); ok {
				status = se.GetStatus()
			} else {
				err = NewError(http.StatusInternalServerError, err.Error())
			}

			ct, _ := api.Negotiate(ctx.Header("Accept"))
			if ctf, ok := err.(ContentTypeFilter); ok {
				ct = ctf.ContentType(ct)
			}

			ctx.SetHeader("Content-Type", ct)
			transformAndWrite(api, ctx, status, ct, err)
			return
		}

		// Serialize output headers
		ct := ""
		vo := reflect.ValueOf(output).Elem()
		outHeaders.Every(vo, func(f reflect.Value, info *headerInfo) {
			switch f.Kind() {
			case reflect.String:
				if f.String() == "" {
					// Don't set empty headers.
					return
				}
				ctx.SetHeader(info.Name, f.String())
				if info.Name == "Content-Type" {
					ct = f.String()
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				ctx.SetHeader(info.Name, strconv.FormatInt(f.Int(), 10))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				ctx.SetHeader(info.Name, strconv.FormatUint(f.Uint(), 10))
			case reflect.Float32, reflect.Float64:
				ctx.SetHeader(info.Name, strconv.FormatFloat(f.Float(), 'f', -1, 64))
			case reflect.Bool:
				ctx.SetHeader(info.Name, strconv.FormatBool(f.Bool()))
			default:
				if f.Type() == timeType && !f.Interface().(time.Time).IsZero() {
					ctx.SetHeader(info.Name, f.Interface().(time.Time).Format(info.TimeFormat))
					return
				}

				ctx.SetHeader(info.Name, fmt.Sprintf("%v", f.Interface()))
			}
		})

		status := op.DefaultStatus
		if outStatusIndex != -1 {
			status = int(vo.Field(outStatusIndex).Int())
		}

		if outBodyIndex != -1 {
			// Serialize output body
			body := vo.Field(outBodyIndex).Interface()

			if outBodyFunc {
				body.(func(Context))(ctx)
				return
			}

			if b, ok := body.([]byte); ok {
				ctx.SetStatus(status)
				ctx.BodyWriter().Write(b)
				return
			}

			// Only write a content type if one wasn't already written by the
			// response headers handled above.
			if ct == "" {
				ct, err = api.Negotiate(ctx.Header("Accept"))
				if err != nil {
					WriteErr(api, ctx, http.StatusNotAcceptable, "unable to marshal response", err)
					return
				}
				if ctf, ok := body.(ContentTypeFilter); ok {
					ct = ctf.ContentType(ct)
				}

				ctx.SetHeader("Content-Type", ct)
			}

			transformAndWrite(api, ctx, status, ct, body)
		} else {
			ctx.SetStatus(status)
		}
	}))
}

// AutoRegister auto-detects operation registration methods and registers them
// with the given API. Any method named `Register...` will be called and
// passed the API as the only argument. Since registration happens at
// service startup, no errors are returned and methods should panic on error.
//
//	type ItemsHandler struct {}
//
//	func (s *ItemsHandler) RegisterListItems(api API) {
//		huma.Register(api, huma.Operation{
//			OperationID: "ListItems",
//			Method: http.MethodGet,
//			Path: "/items",
//		}, s.ListItems)
//	}
//
//	func main() {
//		router := chi.NewMux()
//		config := huma.DefaultConfig("My Service", "1.0.0")
//		api := huma.NewExampleAPI(router, config)
//
//		itemsHandler := &ItemsHandler{}
//		huma.AutoRegister(api, itemsHandler)
//	}
func AutoRegister(api API, server any) {
	args := []reflect.Value{reflect.ValueOf(server), reflect.ValueOf(api)}

	t := reflect.TypeOf(server)
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		if strings.HasPrefix(m.Name, "Register") && len(m.Name) > 8 {
			m.Func.Call(args)
		}
	}
}
