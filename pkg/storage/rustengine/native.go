// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package rustengine

/*
#cgo LDFLAGS: -L${SRCDIR}/native/target/release -lmimir_engine
#cgo linux LDFLAGS: -ldl -lpthread -lm
#cgo darwin LDFLAGS: -framework Security -framework CoreFoundation
#include "native/engine.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type Label struct {
	Name  string
	Value string
}
type Sample struct {
	Timestamp int64
	Kind      uint8
	Bits      uint64
	Data      string
}
type Exemplar struct {
	Timestamp int64
	Bits      uint64
	Labels    []Label
}
type Series struct {
	Ref       uint64
	Labels    []Label
	Samples   map[int64]Sample
	Exemplars []Exemplar
}

// FloatSeries borrows a decoded request only until Call returns.
type FloatSeries struct {
	Index   int
	Labels  []mimirpb.LabelAdapter
	Samples []mimirpb.Sample
}
type SampleError struct {
	Timestamp int64
	Error     string
}
type FloatResult struct {
	Index    int
	Ref      uint64
	Accepted int
	Errors   []SampleError
}
type QueryChunk struct {
	Mint, Maxt int64
	Data       []byte
	Samples    []Sample
}

type Request struct {
	Unordered    bool
	Cursor       uint64
	Sorted       bool
	Limit        int
	Batch        []FloatSeries
	Op           string
	Ref          uint64
	Labels       []Label
	Sample       *Sample
	Exemplar     *Exemplar
	RejectOOO    bool
	Window       int64
	MaxExemplars int64
	Refs         []uint64
	UpTo         int64
	Mint         int64
	Maxt         int64
	Name         string
	Values       []string
	IncludeCold  bool
	Filter       bool
}
type Response struct {
	Cursor    uint64
	Done      bool
	Chunks    []QueryChunk
	Count     uint64
	Mint      int64
	Maxt      int64
	MinValid  int64
	Bytes     map[string]uint64
	Sample    *Sample
	Results   []FloatResult
	Leftovers []int
	Active    bool
	Refs      []uint64
	Values    []string
	Error     string
	Ref       uint64
	Series    []Series
}

// Client requires caller serialization so transactions cannot interleave with reads or close.
type Client struct {
	mu      sync.Mutex
	syncMu  sync.Mutex
	handle  unsafe.Pointer
	request []byte
}

func Open(path string) (*Client, error) {
	bytes := []byte(path)
	if len(bytes) == 0 {
		return nil, errors.New("empty Rust engine path")
	}
	var failure C.mimir_engine_buffer
	handle := C.mimir_engine_open((*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(bytes))), C.size_t(len(bytes)), &failure)
	runtime.KeepAlive(bytes)
	if handle == nil {
		defer C.mimir_engine_free(failure)
		return nil, errors.New(C.GoStringN((*C.char)(unsafe.Pointer(failure.data)), C.int(failure.len)))
	}
	return &Client{handle: handle}, nil
}
func (c *Client) Call(req Request) (Response, error) {
	if req.Op == "sync" {
		return Response{}, c.Sync()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return Response{}, errors.New("Rust engine is closed")
	}
	bytes := encodeRequest(c.request, req)
	if cap(bytes) <= 1<<20 {
		c.request = bytes
	} else {
		c.request = nil
	}
	out := C.mimir_engine_call(c.handle, (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(bytes))), C.size_t(len(bytes)))
	runtime.KeepAlive(bytes)
	defer C.mimir_engine_free(out)
	// Decode while Rust owns the buffer; no foreign pointer escapes this call.
	var response Response
	if uint64(out.len) > uint64(^uint(0)>>1) {
		return response, errors.New("Rust response exceeds address space")
	}
	response, err := decodeResponse(unsafe.Slice((*byte)(unsafe.Pointer(out.data)), int(out.len)))
	if err != nil {
		return response, fmt.Errorf("decode Rust engine response: %w", err)
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}
func (c *Client) IndexEpoch(includeCold bool) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return 0, errors.New("Rust engine is closed")
	}
	var cold C.uint8_t
	if includeCold {
		cold = 1
	}
	var generation C.uint64_t
	if C.mimir_engine_index_epoch(c.handle, cold, &generation) == 0 {
		return 0, errors.New("engine requires reopen after WAL failure")
	}
	return uint64(generation), nil
}

func (c *Client) Sync() error {
	// Kafka snapshots its commit offset before Sync. Flush that prefix under
	// mu, then let ingestion and reads continue during the durability barrier.
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.mu.Lock()
	if c.handle == nil {
		c.mu.Unlock()
		return errors.New("Rust engine is closed")
	}
	var failure C.mimir_engine_buffer
	file := C.mimir_engine_prepare_sync(c.handle, &failure)
	c.mu.Unlock()
	defer C.mimir_engine_free(failure)
	if failure.len != 0 {
		return errors.New(C.GoStringN((*C.char)(unsafe.Pointer(failure.data)), C.int(failure.len)))
	}
	if file == nil {
		return nil
	}
	failure = C.mimir_engine_sync_file(file)
	defer C.mimir_engine_free(failure)
	if failure.len != 0 {
		c.mu.Lock()
		C.mimir_engine_fail_sync(c.handle)
		c.mu.Unlock()
		return errors.New(C.GoStringN((*C.char)(unsafe.Pointer(failure.data)), C.int(failure.len)))
	}
	return nil
}

func (c *Client) Close() {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle != nil {
		C.mimir_engine_close(c.handle)
		c.handle = nil
		c.request = nil
	}
}
