// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/base64"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

type QueryStreamResponse struct {
	Chunkseries []QueryStreamChunkseries `json:"chunkseries"`
}

type QueryStreamChunkseries struct {
	Labels []Label `json:"labels"`
	Chunks []Chunk `json:"chunks"`
}

func (c QueryStreamChunkseries) LabelSet() labels.Labels {
	builder := labels.NewScratchBuilder(len(c.Labels))
	for _, label := range c.Labels {
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

type Chunk struct {
	StartTimestampMs string `json:"startTimestampMs"`
	EndTimestampMs   string `json:"endTimestampMs"`
	Encoding         int    `json:"encoding"`
	EncodedData      string `json:"data"`
}

func (c Chunk) StartTimestamp() int64 {
	value, err := strconv.ParseInt(c.StartTimestampMs, 10, 64)
	if err != nil {
		panic(err)
	}

	return value
}

func (c Chunk) EndTimestamp() int64 {
	value, err := strconv.ParseInt(c.EndTimestampMs, 10, 64)
	if err != nil {
		panic(err)
	}

	return value
}

func (c Chunk) StartTime() time.Time {
	return time.UnixMilli(c.StartTimestamp()).UTC()
}

func (c Chunk) EndTime() time.Time {
	return time.UnixMilli(c.EndTimestamp()).UTC()
}

func (c Chunk) EncodedChunk() chunk.EncodedChunk {
	data, err := base64.StdEncoding.DecodeString(c.EncodedData)
	if err != nil {
		panic(err)
	}

	dataChunk, err := chunk.NewForEncoding(chunk.Encoding(c.Encoding))
	if err != nil {
		panic(err)
	}

	err = dataChunk.UnmarshalFromBuf(data)
	if err != nil {
		panic(err)
	}

	return dataChunk
}
