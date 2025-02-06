// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/base64"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

type SeriesResponse struct {
	Series  *Series `json:"series,omitempty"`
	Warning string  `json:"warning,omitempty"`
	Stats   *Stats  `json:"stats,omitempty"`
}

type Stats struct {
	FetchedIndexBytes int `json:"fetched_index_bytes,omitempty"`
}

type Series struct {
	Labels []Label     `json:"labels"`
	Chunks []AggrChunk `json:"chunks"`
}

func (s Series) LabelSet() labels.Labels {
	builder := labels.NewScratchBuilder(len(s.Labels))
	for _, label := range s.Labels {
		builder.Add(label.Name(), label.Value())
	}
	return builder.Labels()
}

type Label struct {
	EncodedName  string `json:"name"`
	EncodedValue string `json:"value"`
}

func (l Label) Name() string {
	name, err := base64.StdEncoding.DecodeString(l.EncodedName)
	if err != nil {
		panic(err)
	}

	return string(name)
}

func (l Label) Value() string {
	value, err := base64.StdEncoding.DecodeString(l.EncodedValue)
	if err != nil {
		panic(err)
	}

	return string(value)
}

type AggrChunk struct {
	MinTimeMs string `json:"minTime"`
	MaxTimeMs string `json:"maxTime"`
	Raw       Chunk  `json:"raw"`
}

type Chunk struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

func (c AggrChunk) StartTimestamp() int64 {
	value, err := strconv.ParseInt(c.MinTimeMs, 10, 64)
	if err != nil {
		panic(err)
	}

	return value
}

func (c AggrChunk) EndTimestamp() int64 {
	value, err := strconv.ParseInt(c.MaxTimeMs, 10, 64)
	if err != nil {
		panic(err)
	}

	return value
}

func (c AggrChunk) StartTime() time.Time {
	return time.UnixMilli(c.StartTimestamp()).UTC()
}

func (c AggrChunk) EndTime() time.Time {
	return time.UnixMilli(c.EndTimestamp()).UTC()
}

func (c AggrChunk) EncodedChunk() chunk.EncodedChunk {
	data, err := base64.StdEncoding.DecodeString(c.Raw.Data)
	if err != nil {
		panic(err)
	}

	var encoding chunk.Encoding
	switch c.Raw.Type {
	case "", "Chunk_XOR":
		encoding = chunk.PrometheusXorChunk
	case "Chunk_Histogram":
		encoding = chunk.PrometheusHistogramChunk
	case "Chunk_FloatHistogram":
		encoding = chunk.PrometheusFloatHistogramChunk
	default:
		panic("unknown chunk encoding: " + c.Raw.Type)
	}

	dataChunk, err := chunk.NewForEncoding(encoding)
	if err != nil {
		panic(err)
	}

	err = dataChunk.UnmarshalFromBuf(data)
	if err != nil {
		panic(err)
	}

	return dataChunk
}
