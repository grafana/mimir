// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"strings"

	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/model/labels"
	promql_parser "github.com/prometheus/prometheus/promql/parser"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
)

const maxBufferPoolSize = 1024 * 1024

var bufferPool = util.NewBucketedBufferPool(1e3, maxBufferPoolSize, 2)

type parser struct {
	processorConfig processorConfig

	requestHeaders bool
	requestRawBody bool
	decodePush     bool
	includeSamples bool

	responseHeaders    bool
	responseRawBody    bool
	responseDecodeBody bool

	tenantRegexpStr string
	tenantRegexp    *regexp.Regexp

	pathRegexpStr string
	pathRegexp    *regexp.Regexp

	ignorePathRegexpStr string
	ignorePathRegexp    *regexp.Regexp

	metricSelector string
	matchers       []*labels.Matcher
}

func (rp *parser) RegisterFlags(f *flag.FlagSet) {
	rp.processorConfig.RegisterFlags(f)

	f.StringVar(&rp.pathRegexpStr, "path", "", "Output only requests matching this URL path (regex).")
	f.StringVar(&rp.ignorePathRegexpStr, "ignore-path", "", "If not empty, and URL path matches this regex, request is ignored.")
	f.StringVar(&rp.tenantRegexpStr, "tenant", "", "Output only requests for this tenant (regex).")
	f.StringVar(&rp.metricSelector, "select", "", "If set, only output write requests that include series matching this selector. Used only when -request.decode-remote-write is enabled. (Only works for Prometheus requests)")

	f.BoolVar(&rp.requestHeaders, "request.headers", false, "Include request headers in the output")
	f.BoolVar(&rp.requestRawBody, "request.raw-body", false, "Include raw request body in the output")
	f.BoolVar(&rp.decodePush, "request.decode-remote-write", false, "Decode Prometheus or OTEL metrics requests. Only for POST requests that contain /push (Prometheus) or /metrics (OTEL) in the path")
	f.BoolVar(&rp.includeSamples, "request.samples", false, "Include samples in the output. Used only when -request.decode-remote-write is enabled. (Only works with Prometheus requests)")

	f.BoolVar(&rp.responseHeaders, "response.headers", false, "Include HTTP headers in the response")
	f.BoolVar(&rp.responseRawBody, "response.raw-body", false, "Include raw body in the response")
	f.BoolVar(&rp.responseDecodeBody, "response.decode-body", true, "Decode body (eg. gzip). If response body is JSON, it is included in json_body field.")
}

func (rp *parser) prepare() {
	if rp.ignorePathRegexpStr != "" {
		rp.ignorePathRegexp = regexp.MustCompile(rp.ignorePathRegexpStr)
	}
	if rp.pathRegexpStr != "" {
		rp.pathRegexp = regexp.MustCompile(rp.pathRegexpStr)
	}
	if rp.tenantRegexpStr != "" {
		rp.tenantRegexp = regexp.MustCompile(rp.tenantRegexpStr)
	}

	if rp.metricSelector != "" {
		var err error
		rp.matchers, err = promql_parser.ParseMetricSelector(rp.metricSelector)
		if err != nil {
			log.Fatalln("failed to parse matcher selector", "err", err)
		}

		for _, m := range rp.matchers {
			log.Println("using matcher:", m.String())
		}
	}
}

func (rp *parser) processHTTPRequest(req *http.Request, body []byte) *request {
	if rp.ignorePathRegexp != nil && rp.ignorePathRegexp.MatchString(req.URL.Path) {
		return &request{ignored: true}
	}

	if rp.pathRegexp != nil && !rp.pathRegexp.MatchString(req.URL.Path) {
		return &request{ignored: true}
	}

	tenant := req.Header.Get("X-Scope-OrgId")
	if tenant == "" {
		tenant, _, _ = req.BasicAuth()
	}
	if rp.tenantRegexp != nil && (tenant == "" || !rp.tenantRegexp.MatchString(tenant)) {
		return &request{ignored: true}
	}

	r := request{
		Method: req.Method,
		URL: requestURL{
			Path:  req.URL.Path,
			Query: req.URL.Query(),
		},

		Tenant: tenant,
	}

	if rp.requestRawBody {
		r.RawBody = string(body)
	}

	if ct, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type")); ct == "application/x-www-form-urlencoded" {
		vs, _ := url.ParseQuery(string(body))
		r.Form = vs
	}

	if rp.requestHeaders {
		r.Headers = req.Header
	}

	if rp.decodePush && req.Method == "POST" && strings.Contains(req.URL.Path, "/push") {
		var matched bool
		rb := util.NewRequestBuffers(bufferPool)
		r.cleanup = rb.CleanUp
		r.PushRequest, matched = rp.decodePushRequest(req, body, rp.matchers, rb)
		if !matched {
			r.ignored = true
		}
	}

	// Support POST to /otlp/v1/metrics
	if rp.decodePush && req.Method == "POST" && strings.Contains(req.URL.Path, "/metrics") {
		var matched bool
		r.PushRequest, matched = rp.decodeOTLPRequest(req, body)
		if !matched {
			r.ignored = true
		}
	}

	return &r
}

func (rp *parser) decodePushRequest(req *http.Request, body []byte, matchers []*labels.Matcher, buffers *util.RequestBuffers) (*pushRequest, bool) {
	res := &pushRequest{Version: req.Header.Get("X-Prometheus-Remote-Write-Version")}

	var wr mimirpb.WriteRequest
	if _, err := util.ParseProtoReader(context.Background(), bytes.NewReader(body), int(req.ContentLength), 100<<20, buffers, &wr, util.RawSnappy); err != nil {
		res.Error = fmt.Errorf("failed to decode decodePush request: %s", err).Error()
		return nil, true
	}

	// See if we find the matching series. If not, we ignore this request.
	if matchers != nil {
		matched := false
		for _, ts := range wr.Timeseries {
			lbls := mimirpb.FromLabelAdaptersToLabels(ts.Labels)
			if matches(lbls, matchers) {
				matched = true
				break
			}
		}

		if !matched {
			return nil, false
		}
	}

	for _, ts := range wr.Timeseries {
		t := timeseries{
			Labels: mimirpb.FromLabelAdaptersToLabels(ts.Labels),
		}

		if rp.includeSamples {
			t.Samples = ts.TimeSeries.Samples
			t.Exemplars = ts.TimeSeries.Exemplars
		} else {
			t.SamplesCount = len(ts.TimeSeries.Samples)
			t.ExemplarsCount = len(ts.TimeSeries.Exemplars)
		}

		res.TimeSeries = append(res.TimeSeries, t)
	}

	res.Metadata = wr.Metadata

	return res, true
}

func matches(lbls labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		val := lbls.Get(m.Name)
		if !m.Matches(val) {
			return false
		}
	}

	return true
}

func (rp *parser) processHTTPResponse(resp *http.Response, body []byte) *response {
	out := response{
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Proto:      resp.Proto,
	}

	const ce = "Content-Encoding"
	if rp.responseDecodeBody && resp.Header.Get(ce) == "gzip" {
		// decode body
		var err error
		var r *gzip.Reader
		r, err = gzip.NewReader(bytes.NewReader(body))
		if err == nil {
			var newBody []byte
			newBody, err = io.ReadAll(r)
			if err == nil {
				body = newBody
			}
		}

		if err != nil {
			out.Error = fmt.Sprintf("failed to gzip-decode response body: %s", err.Error())
		}
	}

	ct, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if ct == "application/json" && rp.responseDecodeBody {
		out.JSONBody = body
	}

	if ct == "text/plain" {
		out.TextBody = string(body)
	}

	if rp.responseHeaders {
		out.Headers = resp.Header
	}

	if rp.responseRawBody {
		out.RawBody = string(body)
	}

	return &out
}

const (
	pbContentType   = "application/x-protobuf"
	jsonContentType = "application/json"
)

func (rp *parser) decodeOTLPRequest(req *http.Request, body []byte) (*otlpPushRequest, bool) {
	res := &otlpPushRequest{}

	var decoderFunc func(buf []byte) (pmetricotlp.ExportRequest, error)

	contentType := req.Header.Get("Content-Type")
	switch contentType {
	case pbContentType:
		decoderFunc = func(buf []byte) (pmetricotlp.ExportRequest, error) {
			req := pmetricotlp.NewExportRequest()
			return req, req.UnmarshalProto(buf)
		}

	case jsonContentType:
		decoderFunc = func(buf []byte) (pmetricotlp.ExportRequest, error) {
			req := pmetricotlp.NewExportRequest()
			return req, req.UnmarshalJSON(buf)
		}

	default:
		res.Error = fmt.Sprintf("unsupported content type: %s", contentType)
		return nil, false
	}

	// Handle compression.
	contentEncoding := req.Header.Get("Content-Encoding")
	switch contentEncoding {
	case "gzip":
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			res.Error = fmt.Sprintf("failed to decode gzip request: %v", err)
			return nil, true
		}

		body, err = io.ReadAll(gr)
		if err != nil {
			res.Error = fmt.Sprintf("failed to decode gzip request: %v", err)
			return nil, true
		}

	case "":
		// No compression.

	default:
		res.Error = fmt.Sprintf("unsupported compression for otel: %s", contentEncoding)
		return nil, true
	}

	var err error
	res.ExportRequest, err = decoderFunc(body)
	if err != nil {
		res.Error = fmt.Sprintf("failed to decode request: %v", err)
		return nil, true
	}

	return res, true
}
