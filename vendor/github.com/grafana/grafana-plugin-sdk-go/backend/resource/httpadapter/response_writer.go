package httpadapter

import (
	"bytes"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// callResourceResponseWriter is an implementation of http.ResponseWriter that
// writes a backend.CallResourceResponse as Result().
type callResourceResponseWriter struct {
	stream backend.CallResourceResponseSender

	// Code is the HTTP response code set by WriteHeader.
	//
	// Note that if a Handler never calls WriteHeader or Write,
	// this might end up being 0, rather than the implicit
	// http.StatusOK. To get the implicit value, use the Result
	// method.
	Code int

	// HeaderMap contains the headers explicitly set by the Handler.
	// It is an internal detail.
	//
	// Deprecated: HeaderMap exists for historical compatibility
	// and should not be used. To access the headers returned by a handler,
	// use the Response.Header map as returned by the Result method.
	HeaderMap http.Header

	// Body is the buffer to which the Handler's Write calls are sent.
	// If nil, the Writes are silently discarded.
	Body *bytes.Buffer

	// Flushed is whether the Handler called Flush.
	Flushed bool

	wroteHeader     bool
	sentFirstStream bool
}

func newResponseWriter(stream backend.CallResourceResponseSender) *callResourceResponseWriter {
	return &callResourceResponseWriter{
		stream:    stream,
		HeaderMap: make(http.Header),
		Body:      new(bytes.Buffer),
		Code:      200,
	}
}

// Header implements http.ResponseWriter. It returns the response
// headers to mutate within a handler. To test the headers that were
// written after a handler completes, use the Result method and see
// the returned Response value's Header.
func (rw *callResourceResponseWriter) Header() http.Header {
	m := rw.HeaderMap
	if m == nil {
		m = make(http.Header)
		rw.HeaderMap = m
	}
	return m
}

// writeHeader writes a header if it was not written yet and
// detects Content-Type if needed.
//
// bytes or str are the beginning of the response body.
// We pass both to avoid unnecessarily generate garbage
// in rw.WriteString which was created for performance reasons.
// Non-nil bytes win.
func (rw *callResourceResponseWriter) writeHeader(b []byte, str string) {
	if rw.wroteHeader {
		return
	}
	if len(str) > 512 {
		str = str[:512]
	}

	m := rw.Header()

	_, hasType := m["Content-Type"]
	hasTE := m.Get("Transfer-Encoding") != ""
	if !hasType && !hasTE {
		if b == nil {
			b = []byte(str)
		}
		m.Set("Content-Type", http.DetectContentType(b))
	}

	rw.WriteHeader(200)
}

// Write implements http.ResponseWriter. The data in buf is written to
// rw.Body, if not nil.
func (rw *callResourceResponseWriter) Write(buf []byte) (int, error) {
	rw.writeHeader(buf, "")
	if rw.Body != nil {
		rw.Body.Write(buf)
	}
	return len(buf), nil
}

// WriteHeader implements http.ResponseWriter.
func (rw *callResourceResponseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}
	rw.Code = code
	rw.wroteHeader = true
	if rw.HeaderMap == nil {
		rw.HeaderMap = make(http.Header)
	}
}

// Flush implements http.Flusher.
func (rw *callResourceResponseWriter) Flush() {
	if !rw.wroteHeader {
		rw.WriteHeader(200)
	}

	resp := rw.getResponse()
	if resp != nil {
		if err := rw.stream.Send(resp); err != nil {
			log.DefaultLogger.Error("Failed to send resource response", "error", err)
		}
	}

	rw.Body = new(bytes.Buffer)
}

func (rw *callResourceResponseWriter) getResponse() *backend.CallResourceResponse {
	if !rw.sentFirstStream {
		res := &backend.CallResourceResponse{
			Status:  rw.Code,
			Headers: rw.Header().Clone(),
		}

		if rw.Body != nil {
			res.Body = rw.Body.Bytes()
		}

		rw.sentFirstStream = true
		return res
	}

	if rw.Body != nil && rw.Body.Len() > 0 {
		return &backend.CallResourceResponse{
			Body: rw.Body.Bytes(),
		}
	}

	return nil
}

func (rw *callResourceResponseWriter) close() {
	rw.Flush()
}
