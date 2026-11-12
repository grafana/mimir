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
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
type Sample struct {
	Timestamp int64  `json:"t"`
	Kind      uint8  `json:"kind"`
	Bits      uint64 `json:"bits"`
	Data      string `json:"data"`
}
type Exemplar struct {
	Timestamp int64   `json:"t"`
	Bits      uint64  `json:"bits"`
	Labels    []Label `json:"labels"`
}
type Series struct {
	Ref       uint64           `json:"ref"`
	Labels    []Label          `json:"labels"`
	Samples   map[int64]Sample `json:"samples"`
	Exemplars []Exemplar       `json:"exemplars"`
}
type FloatSeries struct {
	Index   int      `json:"index"`
	Labels  []Label  `json:"labels"`
	Samples []Sample `json:"samples"`
}
type SampleError struct {
	Timestamp int64  `json:"timestamp"`
	Error     string `json:"error"`
}
type FloatResult struct {
	Index    int           `json:"index"`
	Ref      uint64        `json:"ref"`
	Accepted int           `json:"accepted"`
	Errors   []SampleError `json:"errors"`
}
type Request struct {
	Limit        int           `json:"limit"`
	Batch        []FloatSeries `json:"batch,omitempty"`
	Op           string        `json:"op"`
	Ref          uint64        `json:"ref"`
	Labels       []Label       `json:"labels,omitempty"`
	Sample       *Sample       `json:"sample,omitempty"`
	Exemplar     *Exemplar     `json:"exemplar,omitempty"`
	RejectOOO    bool          `json:"reject_ooo"`
	Window       int64         `json:"window"`
	MaxExemplars int64         `json:"max_exemplars"`
	Refs         []uint64      `json:"refs,omitempty"`
	UpTo         int64         `json:"up_to"`
	Mint         int64         `json:"mint"`
	Maxt         int64         `json:"maxt"`
	Name         string        `json:"name"`
	Values       []string      `json:"values,omitempty"`
	IncludeCold  bool          `json:"include_cold"`
	Filter       bool          `json:"filter"`
}
type Response struct {
	Count     uint64            `json:"count"`
	Mint      int64             `json:"mint"`
	Maxt      int64             `json:"maxt"`
	MinValid  int64             `json:"min_valid"`
	Bytes     map[string]uint64 `json:"bytes"`
	Sample    *Sample           `json:"sample"`
	Results   []FloatResult     `json:"results"`
	Leftovers []int             `json:"leftovers"`
	Active    bool              `json:"active"`
	Refs      []uint64          `json:"refs"`
	Values    []string          `json:"values"`
	Error     string            `json:"error"`
	Ref       uint64            `json:"ref"`
	Series    []Series          `json:"series"`
}

// Client requires caller serialization so transactions cannot interleave with reads or close.
type Client struct {
	mu     sync.Mutex
	handle unsafe.Pointer
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == nil {
		return Response{}, errors.New("Rust engine is closed")
	}
	bytes, err := json.Marshal(req)
	if err != nil {
		return Response{}, err
	}
	out := C.mimir_engine_call(c.handle, (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(bytes))), C.size_t(len(bytes)))
	runtime.KeepAlive(bytes)
	defer C.mimir_engine_free(out)
	// Decode while Rust owns the buffer; no foreign pointer escapes this call.
	var response Response
	if uint64(out.len) > uint64(^uint(0)>>1) {
		return response, errors.New("Rust response exceeds address space")
	}
	if err = json.Unmarshal(unsafe.Slice((*byte)(unsafe.Pointer(out.data)), int(out.len)), &response); err != nil {
		return response, fmt.Errorf("decode Rust engine response: %w", err)
	}
	if response.Error != "" {
		return response, errors.New(response.Error)
	}
	return response, nil
}
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle != nil {
		C.mimir_engine_close(c.handle)
		c.handle = nil
	}
}
