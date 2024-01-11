package humamux

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

type gmuxContext struct {
	op *huma.Operation
	r  *http.Request
	w  http.ResponseWriter
}

func (c *gmuxContext) Operation() *huma.Operation {
	return c.op
}

func (c *gmuxContext) Context() context.Context {
	return c.r.Context()
}

func (c *gmuxContext) Method() string {
	return c.r.Method
}

func (c *gmuxContext) Host() string {
	return c.r.Host
}

func (c *gmuxContext) URL() url.URL {
	return *c.r.URL
}

func (c *gmuxContext) Param(name string) string {
	return mux.Vars(c.r)[name]
}

func (c *gmuxContext) Query(name string) string {
	return queryparam.Get(c.r.URL.RawQuery, name)
}

func (c *gmuxContext) Header(name string) string {
	return c.r.Header.Get(name)
}

func (c *gmuxContext) EachHeader(cb func(name, value string)) {
	for name, values := range c.r.Header {
		for _, value := range values {
			cb(name, value)
		}
	}
}

func (c *gmuxContext) BodyReader() io.Reader {
	return c.r.Body
}

func (c *gmuxContext) GetMultipartForm() (*multipart.Form, error) {
	err := c.r.ParseMultipartForm(8 * 1024)
	return c.r.MultipartForm, err
}

func (c *gmuxContext) SetReadDeadline(deadline time.Time) error {
	return huma.SetReadDeadline(c.w, deadline)
}

func (c *gmuxContext) SetStatus(code int) {
	c.w.WriteHeader(code)
}

func (c *gmuxContext) AppendHeader(name string, value string) {
	c.w.Header().Add(name, value)
}

func (c *gmuxContext) SetHeader(name string, value string) {
	c.w.Header().Set(name, value)
}

func (c *gmuxContext) BodyWriter() io.Writer {
	return c.w
}

type gMux struct {
	router *mux.Router
}

func (a *gMux) Handle(op *huma.Operation, handler func(huma.Context)) {
	a.router.
		NewRoute().
		Path(op.Path).
		Methods(op.Method).
		HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler(&gmuxContext{op: op, r: r, w: w})
		})
}

func (a *gMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.router.ServeHTTP(w, r)
}

func New(r *mux.Router, config huma.Config) huma.API {
	return huma.NewAPI(config, &gMux{router: r})
}
