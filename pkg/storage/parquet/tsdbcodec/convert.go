package tsdbcodec

import (
	"math"
	"runtime"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
)

var DefaultConvertOpts = TSDBConvertOpts{
	name:              "block",
	rowGroupSize:      1e6,
	colDuration:       time.Hour * 8,
	numRowGroups:      math.MaxInt32,
	sortedLabels:      []string{labels.MetricName},
	bloomfilterLabels: []string{labels.MetricName},
	pageBufferSize:    parquet.DefaultPageBufferSize,
	writeBufferSize:   parquet.DefaultWriteBufferSize,
	columnPageBuffers: parquet.DefaultWriterConfig().ColumnPageBuffers,
	concurrency:       runtime.GOMAXPROCS(0),
}

type TSDBConvertOpts struct {
	numRowGroups      int
	rowGroupSize      int
	colDuration       time.Duration
	name              string
	sortedLabels      []string
	bloomfilterLabels []string
	pageBufferSize    int
	writeBufferSize   int
	columnPageBuffers parquet.BufferPool
	concurrency       int
}

type TSDBConvertOption func(*TSDBConvertOpts)

func WithSortBy(labels ...string) TSDBConvertOption {
	return func(opts *TSDBConvertOpts) {
		opts.sortedLabels = labels
	}
}

func WithColDuration(d time.Duration) TSDBConvertOption {
	return func(opts *TSDBConvertOpts) {
		opts.colDuration = d
	}
}

func WithWriteBufferSize(s int) TSDBConvertOption {
	return func(opts *TSDBConvertOpts) {
		opts.writeBufferSize = s
	}
}

func WithPageBufferSize(s int) TSDBConvertOption {
	return func(opts *TSDBConvertOpts) {
		opts.pageBufferSize = s
	}
}

func WithName(name string) TSDBConvertOption {
	return func(opts *TSDBConvertOpts) {
		opts.name = name
	}
}

func WithConcurrency(concurrency int) TSDBConvertOption {
	return func(opts *TSDBConvertOpts) {
		opts.concurrency = concurrency
	}
}

func WithColumnPageBuffers(buffers parquet.BufferPool) TSDBConvertOption {
	return func(opts *TSDBConvertOpts) {
		opts.columnPageBuffers = buffers
	}
}
