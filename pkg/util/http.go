// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/http.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/flagext"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"gopkg.in/yaml.v3"
)

// IsRequestBodyTooLarge returns true if the error is "http: request body too large".
func IsRequestBodyTooLarge(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http: request body too large")
}

// BasicAuth configures basic authentication for HTTP clients.
type BasicAuth struct {
	Username string         `yaml:"basic_auth_username"`
	Password flagext.Secret `yaml:"basic_auth_password"`
}

func (b *BasicAuth) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&b.Username, prefix+"basic-auth-username", "", "HTTP Basic authentication username. It overrides the username set in the URL (if any).")
	f.Var(&b.Password, prefix+"basic-auth-password", "HTTP Basic authentication password. It overrides the password set in the URL (if any).")
}

// IsEnabled returns false if basic authentication isn't enabled.
func (b BasicAuth) IsEnabled() bool {
	return b.Username != "" || b.Password.String() != ""
}

// WriteJSONResponse writes some JSON as a HTTP response.
func WriteJSONResponse(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")

	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We ignore errors here, because we cannot do anything about them.
	// Write will trigger sending Status code, so we cannot send a different status code afterwards.
	// Also this isn't internal error, but error communicating with client.
	_, _ = w.Write(data)
}

// WriteYAMLResponse writes some YAML as a HTTP response.
func WriteYAMLResponse(w http.ResponseWriter, v interface{}) {
	// There is not standardised content-type for YAML, text/plain ensures the
	// YAML is displayed in the browser instead of offered as a download
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	data, err := yaml.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We ignore errors here, because we cannot do anything about them.
	// Write will trigger sending Status code, so we cannot send a different status code afterwards.
	// Also this isn't internal error, but error communicating with client.
	_, _ = w.Write(data)
}

// WriteTextResponse sends message as text/plain response with 200 status code.
func WriteTextResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/plain")

	// Ignore inactionable errors.
	_, _ = w.Write([]byte(message))
}

// WriteHTMLResponse sends message as text/html response with 200 status code.
func WriteHTMLResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/html")

	// Ignore inactionable errors.
	_, _ = w.Write([]byte(message))
}

// RenderHTTPResponse either responds with JSON or a rendered HTML page using the passed in template
// by checking the Accepts header.
func RenderHTTPResponse(w http.ResponseWriter, v interface{}, t *template.Template, r *http.Request) {
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "application/json") {
		WriteJSONResponse(w, v)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	err := t.Execute(w, v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// StreamWriteYAMLResponse stream writes data as http response
func StreamWriteYAMLResponse(w http.ResponseWriter, iter chan interface{}, logger log.Logger) {
	w.Header().Set("Content-Type", "application/yaml")
	for v := range iter {
		data, err := yaml.Marshal(v)
		if err != nil {
			level.Error(logger).Log("msg", "yaml marshal failed", "err", err)
			continue
		}
		_, err = w.Write(data)
		if err != nil {
			level.Error(logger).Log("msg", "write http response failed", "err", err)
			return
		}
	}
}

// CompressionType for encoding and decoding requests and responses.
type CompressionType int

// Values for CompressionType
const (
	NoCompression CompressionType = iota
	RawSnappy
)

// ParseProtoReader parses a compressed proto from an io.Reader.
// You can pass in and receive back the decompression buffer for pooling, or pass in nil and ignore the return.
func ParseProtoReader(ctx context.Context, reader io.Reader, expectedSize, maxSize int, dst []byte, req proto.Message, compression CompressionType) ([]byte, error) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoRequest[start reading]"))
	}
	body, err := decompressRequest(dst, reader, expectedSize, maxSize, compression, sp)
	if err != nil {
		return nil, err
	}

	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoRequest[unmarshal]"), otlog.Int("size", len(body)))
	}

	// We re-implement proto.Unmarshal here as it calls XXX_Unmarshal first,
	// which we can't override without upsetting golint.
	req.Reset()
	if u, ok := req.(proto.Unmarshaler); ok {
		err = u.Unmarshal(body)
	} else {
		err = proto.NewBuffer(body).Unmarshal(req)
	}
	if err != nil {
		if sp != nil {
			sp.LogFields(otlog.Event("util.ParseProtoRequest[unmarshal done]"), otlog.Error(err))
		}

		return nil, err
	}

	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoRequest[unmarshal done]"))
	}

	return body, nil
}

type MsgSizeTooLargeErr struct {
	Actual, Limit int
}

func (e MsgSizeTooLargeErr) Error() string {
	return fmt.Sprintf("the request has been rejected because its size of %d bytes exceeds the limit of %d bytes", e.Actual, e.Limit)
}

// Needed for errors.Is to work properly.
func (e MsgSizeTooLargeErr) Is(err error) bool {
	_, ok1 := err.(MsgSizeTooLargeErr)
	_, ok2 := err.(*MsgSizeTooLargeErr)
	return ok1 || ok2
}

func decompressRequest(dst []byte, reader io.Reader, expectedSize, maxSize int, compression CompressionType, sp opentracing.Span) (body []byte, err error) {
	defer func() {
		if err != nil && len(body) > maxSize {
			err = MsgSizeTooLargeErr{Actual: len(body), Limit: maxSize}
		}
	}()
	if expectedSize > maxSize {
		return nil, MsgSizeTooLargeErr{Actual: expectedSize, Limit: maxSize}
	}
	buffer, ok := tryBufferFromReader(reader)
	if ok {
		body, err = decompressFromBuffer(dst, buffer, maxSize, compression, sp)
		return
	}
	body, err = decompressFromReader(dst, reader, expectedSize, maxSize, compression, sp)
	return
}

func decompressFromReader(dst []byte, reader io.Reader, expectedSize, maxSize int, compression CompressionType, sp opentracing.Span) ([]byte, error) {
	var (
		buf  bytes.Buffer
		body []byte
		err  error
	)
	if expectedSize > 0 {
		buf.Grow(expectedSize + bytes.MinRead) // extra space guarantees no reallocation
	}
	// Read from LimitReader with limit max+1. So if the underlying
	// reader is over limit, the result will be bigger than max.
	reader = io.LimitReader(reader, int64(maxSize)+1)
	switch compression {
	case NoCompression:
		_, err = buf.ReadFrom(reader)
		body = buf.Bytes()
	case RawSnappy:
		_, err = buf.ReadFrom(reader)
		if err != nil {
			return nil, err
		}
		body, err = decompressFromBuffer(dst, &buf, maxSize, RawSnappy, sp)
	}
	return body, err
}

func decompressFromBuffer(dst []byte, buffer *bytes.Buffer, maxSize int, compression CompressionType, sp opentracing.Span) ([]byte, error) {
	if len(buffer.Bytes()) > maxSize {
		return nil, MsgSizeTooLargeErr{Actual: len(buffer.Bytes()), Limit: maxSize}
	}
	switch compression {
	case NoCompression:
		return buffer.Bytes(), nil
	case RawSnappy:
		if sp != nil {
			sp.LogFields(otlog.Event("util.ParseProtoRequest[decompress]"),
				otlog.Int("size", len(buffer.Bytes())))
		}
		size, err := snappy.DecodedLen(buffer.Bytes())
		if err != nil {
			return nil, err
		}
		if size > maxSize {
			return nil, MsgSizeTooLargeErr{Actual: size, Limit: maxSize}
		}
		body, err := snappy.Decode(dst, buffer.Bytes())
		if err != nil {
			return nil, err
		}
		return body, nil
	}
	return nil, nil
}

// tryBufferFromReader attempts to cast the reader to a `*bytes.Buffer` this is possible when using httpgrpc.
// If it fails it will return nil and false.
func tryBufferFromReader(reader io.Reader) (*bytes.Buffer, bool) {
	if bufReader, ok := reader.(interface {
		BytesBuffer() *bytes.Buffer
	}); ok && bufReader != nil {
		return bufReader.BytesBuffer(), true
	}
	return nil, false
}

// SerializeProtoResponse serializes a protobuf response into an HTTP response.
func SerializeProtoResponse(w http.ResponseWriter, resp proto.Message, compression CompressionType) error {
	data, err := proto.Marshal(resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error marshaling proto response: %v", err)
	}

	switch compression {
	case NoCompression:
	case RawSnappy:
		data = snappy.Encode(nil, data)
	}

	if _, err := w.Write(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error sending proto response: %v", err)
	}
	return nil
}

// ParseRequestFormWithoutConsumingBody parsed and returns the request parameters (query string and/or request body)
// from the input http.Request. If the request has a Body, the request's Body is replaces so that it can be consumed again.
func ParseRequestFormWithoutConsumingBody(r *http.Request) (url.Values, error) {
	if r.Body == nil {
		if err := r.ParseForm(); err != nil {
			return nil, err
		}

		return r.Form, nil
	}

	// Close the original body reader. It's going to be replaced later in this function.
	origBody := r.Body
	defer func() { _ = origBody.Close() }()

	// Store the body contents, so we can read it multiple times.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))

	// Parse the request data.
	if err := r.ParseForm(); err != nil {
		return nil, err
	}

	// Store a copy of the params and restore the request state.
	// Restore the body, so it can be read again if it's used to forward the request through a roundtripper.
	// Restore the Form and PostForm, to avoid subtle bugs in middlewares, as they were set by ParseForm.
	params := copyValues(r.Form)
	r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	r.Form, r.PostForm = nil, nil

	return params, nil
}

func copyValues(src url.Values) url.Values {
	dst := make(url.Values, len(src))
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
	return dst
}
