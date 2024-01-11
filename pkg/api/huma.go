package api

import (
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/queryparam"
	"github.com/gorilla/mux"
)

type humaAdapter struct {
	api *API
}

var _ huma.Adapter = &humaAdapter{}

func NewHumaAPI(api *API, config huma.Config) huma.API {
	return huma.NewAPI(config, &humaAdapter{api: api})
}

func (h *humaAdapter) Handle(op *huma.Operation, handler func(huma.Context)) {
	h.api.server.HTTP.NewRoute().
		Path(op.Path).
		Methods(op.Method).
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler(&humaContext{op: op, w: w, r: r})
		})
}

func (h *humaAdapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.api.server.HTTP.ServeHTTP(w, r)
}

type humaContext struct {
	op *huma.Operation
	w  http.ResponseWriter
	r  *http.Request
}

var _ huma.Context = &humaContext{}

func (h *humaContext) Operation() *huma.Operation {
	return h.op
}

func (h *humaContext) Context() context.Context {
	return h.r.Context()
}

func (h *humaContext) Method() string {
	return h.r.Method
}

func (h *humaContext) Host() string {
	return h.r.Host
}

func (h *humaContext) URL() url.URL {
	return *h.r.URL
}

func (h *humaContext) Param(name string) string {
	return mux.Vars(h.r)[name]
}

func (h *humaContext) Query(name string) string {
	return queryparam.Get(h.r.URL.RawQuery, name)
}

func (h *humaContext) Header(name string) string {
	return h.r.Header.Get(name)
}

func (h *humaContext) EachHeader(cb func(name string, value string)) {
	for name, values := range h.r.Header {
		for _, value := range values {
			cb(name, value)
		}
	}
}

func (h *humaContext) BodyReader() io.Reader {
	return h.r.Body
}

func (h *humaContext) GetMultipartForm() (*multipart.Form, error) {
	err := h.r.ParseMultipartForm(8 * 1024)
	return h.r.MultipartForm, err
}

func (h *humaContext) SetReadDeadline(deadline time.Time) error {
	return huma.SetReadDeadline(h.w, deadline)
}

func (h *humaContext) SetStatus(code int) {
	h.w.WriteHeader(code)
}

func (h *humaContext) SetHeader(name, value string) {
	h.w.Header().Set(name, value)
}

func (h *humaContext) AppendHeader(name, value string) {
	h.w.Header().Add(name, value)
}

func (h *humaContext) BodyWriter() io.Writer {
	return h.w
}
