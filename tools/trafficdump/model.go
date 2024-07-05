// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type output struct {
	Client endpoint `json:"client"`
	Server endpoint `json:"server"`

	Request  *request  `json:"request,omitempty"`
	Response *response `json:"response,omitempty"`
}

type endpoint struct {
	Address string `json:"addr"`
	Port    string `json:"port"`
}

// Request described in JSON, for nice machine-readable output.
type request struct {
	ignored bool

	Error string `json:"error,omitempty"`

	Method  string      `json:"method"`
	URL     requestURL  `json:"url"`
	Headers http.Header `json:"headers,omitempty"`
	Tenant  string      `json:"tenant,omitempty"`

	Form    url.Values `json:"form,omitempty"`
	RawBody string     `json:"raw_body,omitempty"`

	PushRequest any `json:"push,omitempty"`

	cleanup func()
}

type requestURL struct {
	Path  string     `json:"path"`
	Query url.Values `json:"query,omitempty"`
}

type pushRequest struct {
	Version string `json:"version"`

	TimeSeries []timeseries              `json:"timeseries,omitempty"`
	Metadata   []*mimirpb.MetricMetadata `json:"metadata,omitempty"`

	Error string `json:"error,omitempty"`
}

type otlpPushRequest struct {
	NumDataPoints            int `json:"numDataPoints,omitempty"`
	NumExemplars             int `json:"numExemplars,omitempty"`
	NumGauges                int `json:"numGauges,omitempty"`
	NumSums                  int `json:"numSums,omitempty"`
	NumHistograms            int `json:"numHistograms,omitempty"`
	NumHistogramSamples      int `json:"numHistogramSamples,omitempty"`
	NumExponentialHistograms int `json:"numExponentialHistograms,omitempty"`
	NumSummaries             int `json:"numSummaries,omitempty"`

	Error string `json:"error,omitempty"`
}

type timeseries struct {
	Labels labels.Labels
	// Sorted by time, oldest sample first.
	Samples      []mimirpb.Sample
	SamplesCount int

	Exemplars      []mimirpb.Exemplar
	ExemplarsCount int
}

func (s timeseries) MarshalJSON() ([]byte, error) {
	buf := &bytes.Buffer{}
	buf.WriteString("{\"metric\": ")

	writeLabels(buf, s.Labels)

	buf.WriteString(", \"samples\":")
	if len(s.Samples) > 0 {
		buf.WriteString("[")
		sep := ""
		for _, ss := range s.Samples {
			buf.WriteString(sep)
			sampleWithLabels{ts: ss.TimestampMs, val: ss.Value}.marshalToBuffer(buf)
			sep = ","
		}
		buf.WriteString("]")
	} else {
		buf.WriteString(strconv.Itoa(s.SamplesCount))
	}

	buf.WriteString(", \"exemplars\":")
	if len(s.Exemplars) > 0 {
		buf.WriteString("[")
		sep := ""
		for _, ex := range s.Exemplars {
			buf.WriteString(sep)

			sampleWithLabels{
				lbls: mimirpb.FromLabelAdaptersToLabels(ex.Labels),
				ts:   ex.TimestampMs,
				val:  ex.Value,
			}.marshalToBuffer(buf)
			sep = ", "
		}
		buf.WriteString("]")
	} else {
		buf.WriteString(strconv.Itoa(s.ExemplarsCount))
	}

	buf.WriteString("}")

	return buf.Bytes(), nil
}

// Response described in JSON, for nice machine-readable output.
type response struct {
	Error string `json:"error,omitempty"`

	Proto      string `json:"proto"`
	Status     string `json:"status"`
	StatusCode int    `json:"status_code"`

	Headers http.Header `json:"headers,omitempty"`

	RawBody  string          `json:"raw_body,omitempty"`
	JSONBody json.RawMessage `json:"json_body,omitempty"`
	TextBody string          `json:"text_body,omitempty"`
}

func writeJSONString(b *bytes.Buffer, s string) {
	out, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	b.Write(out)
}

func writeLabels(b *bytes.Buffer, lbls labels.Labels) {
	b.WriteByte('{')
	i := 0
	lbls.Range(func(l labels.Label) {
		if i > 0 {
			b.WriteByte(',')
		}
		writeJSONString(b, l.Name)
		b.WriteByte(':')
		writeJSONString(b, l.Value)
		i++
	})
	b.WriteByte('}')
}

type sampleWithLabels struct {
	lbls labels.Labels
	ts   int64
	val  float64
}

func (s sampleWithLabels) marshalToBuffer(b *bytes.Buffer) {
	b.WriteString("[")
	if !s.lbls.IsEmpty() {
		writeLabels(b, s.lbls)
		b.WriteString(",")
	}

	b.WriteString(strconv.FormatFloat(float64(s.ts)/float64(time.Second/time.Millisecond), 'f', -1, 64))
	b.WriteString(",")
	writeJSONString(b, strconv.FormatFloat(float64(s.val), 'f', -1, 64))
	b.WriteString("]")
}
