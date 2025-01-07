// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/http.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"bytes"
	"compress/gzip"
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
	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
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
	Gzip
	Lz4
)

// ParseProtoReader parses a compressed proto from an io.Reader.
// You can pass in an optional RequestBuffers.
// If no error is returned, the returned actualSize is the size of the uncompressed proto.
func ParseProtoReader(ctx context.Context, reader io.Reader, expectedSize, maxSize int, buffers *RequestBuffers, req proto.Message, compression CompressionType) (actualSize int, err error) {
	sp := opentracing.SpanFromContext(ctx)
	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoReader[start reading]"))
	}
	body, err := decompressRequest(buffers, reader, expectedSize, maxSize, compression, sp)
	if err != nil {
		return 0, err
	}

	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoReader[unmarshal]"), otlog.Int("size", len(body)))
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
			sp.LogFields(otlog.Event("util.ParseProtoReader[unmarshal done]"), otlog.Error(err))
		}

		return 0, err
	}

	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoReader[unmarshal done]"))
	}

	return len(body), nil
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

func decompressRequest(buffers *RequestBuffers, reader io.Reader, expectedSize, maxSize int, compression CompressionType, sp opentracing.Span) ([]byte, error) {
	if expectedSize > maxSize {
		return nil, MsgSizeTooLargeErr{Actual: expectedSize, Limit: maxSize}
	}

	switch compression {
	case NoCompression, RawSnappy:
		buf, ok := tryBufferFromReader(reader)
		if ok {
			if compression == NoCompression {
				if buf.Len() > maxSize {
					return nil, MsgSizeTooLargeErr{Actual: buf.Len(), Limit: maxSize}
				}
				return buf.Bytes(), nil
			}

			return decompressSnappyFromBuffer(buffers, buf, maxSize, sp)
		}
	case Gzip:
		gzReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, errors.Wrap(err, "create gzip reader")
		}

		defer func() {
			_ = gzReader.Close()
		}()
		reader = gzReader
	case Lz4:
		reader = lz4.NewReader(reader)
	default:
		return nil, fmt.Errorf("unrecognized compression type %v", compression)
	}

	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoReader[decompress]"), otlog.Int("expectedSize", expectedSize))
	}

	// Limit at maxSize+1 so we can tell when the size is exceeded
	reader = io.LimitReader(reader, int64(maxSize)+1)

	sz := expectedSize
	if sz > 0 {
		// Extra space guarantees no reallocation
		sz += bytes.MinRead
	}
	buf := buffers.Get(sz)
	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoReader[started_reading]"))
	}
	if _, err := buf.ReadFrom(reader); err != nil {
		if compression == Gzip {
			return nil, errors.Wrap(err, "decompress gzip")
		}
		if compression == Lz4 {
			return nil, errors.Wrap(err, "decompress lz4")
		}
		return nil, errors.Wrap(err, "read body")
	}
	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoReader[finished_reading]"))
	}

	if compression == RawSnappy {
		return decompressSnappyFromBuffer(buffers, buf, maxSize, sp)
	}

	if buf.Len() > maxSize {
		return nil, MsgSizeTooLargeErr{Actual: -1, Limit: maxSize}
	}
	return buf.Bytes(), nil
}

func decompressSnappyFromBuffer(buffers *RequestBuffers, buffer *bytes.Buffer, maxSize int, sp opentracing.Span) ([]byte, error) {
	if sp != nil {
		sp.LogFields(otlog.Event("util.ParseProtoReader[decompressSnappy]"), otlog.Int("size", buffer.Len()))
	}

	size, err := snappy.DecodedLen(buffer.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "getting snappy decoded length")
	}
	if size > maxSize {
		return nil, MsgSizeTooLargeErr{Actual: size, Limit: maxSize}
	}

	decBuf := buffers.Get(size)
	// Snappy bases itself on the target buffer's length, not capacity
	decBufBytes := decBuf.Bytes()[0:size]

	decoded, err := snappy.Decode(decBufBytes, buffer.Bytes())
	if err != nil {
		return nil, errors.Wrap(err, "decompress snappy")
	}

	return decoded, nil
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

var snappyEncoding = snappyCheckAndEncode

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
		data, err = snappyEncoding(nil, data)
		if err != nil {
			err = errors.Wrap(err, "snappy encoding")
			break
		}
	case Gzip:
		var buf bytes.Buffer
		buf.Grow(len(data))
		wr := gzip.NewWriter(&buf)
		if _, err = wr.Write(data); err != nil {
			err = errors.Wrap(err, "write gzip")
			break
		}
		if err = wr.Close(); err != nil {
			err = errors.Wrap(err, "close gzip writer")
			break
		}
		data = buf.Bytes()
	default:
		err = fmt.Errorf("unrecognized compression format %v", compression)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}

	if _, err := w.Write(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return fmt.Errorf("error sending proto response: %v", err)
	}
	return nil
}

// ParseRequestFormWithoutConsumingBody parsed and returns the request parameters (query string and/or request body)
// from the input http.Request. If the request has a Body, the request's Body is replaced so that it can be consumed again.
// It does not check the req.Body size, so it is the caller's responsibility to ensure that the body is not too large.
func ParseRequestFormWithoutConsumingBody(r *http.Request) (url.Values, error) {
	if r.Body == nil {
		if err := r.ParseForm(); err != nil {
			return nil, err
		}

		return r.Form, nil
	}

	bodyBytes, err := ReadRequestBodyWithoutConsuming(r)
	if err != nil {
		return nil, err
	}

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

// ReadRequestBodyWithoutConsuming makes a copy of the request body bytes
// without consuming the body, so it can be read again later.
// If the request has no body, it returns nil without error.
// It does not check the req.Body size, so it is the caller's responsibility
// to ensure that the body is not too large.
func ReadRequestBodyWithoutConsuming(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
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

	return bodyBytes, nil
}

func copyValues(src url.Values) url.Values {
	dst := make(url.Values, len(src))
	for k, vs := range src {
		dst[k] = append([]string(nil), vs...)
	}
	return dst
}

// IsHTTPStatusCode returns true if the given code is a valid HTTP status code, or false otherwise.
func IsHTTPStatusCode(code codes.Code) bool {
	return int(code) >= 100 && int(code) < 600
}
func IsValidURL(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}

	return u.Scheme != "" && u.Host != ""
}

func snappyCheckAndEncode(dst []byte, data []byte) ([]byte, error) {
	if encodeLen := snappy.MaxEncodedLen(len(data)); encodeLen == -1 {
		return nil, fmt.Errorf("data too large to encode")
	}
	return snappy.Encode(dst, data), nil
}
