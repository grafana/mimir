// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"sync"
)

var (
	queryStreamResponseDataPool = sync.Pool{}
)

// ChunksCount returns the number of chunks in response.
func (m *QueryStreamResponse) ChunksCount() int {
	if len(m.Chunkseries) == 0 {
		return 0
	}

	count := 0
	for _, entry := range m.Chunkseries {
		count += len(entry.Chunks)
	}
	return count
}

// ChunksSize returns the size of all chunks in the response.
func (m *QueryStreamResponse) ChunksSize() int {
	if len(m.Chunkseries) == 0 {
		return 0
	}

	size := 0
	for _, entry := range m.Chunkseries {
		for _, chunk := range entry.Chunks {
			size += chunk.Size()
		}
	}
	return size
}

func SendQueryStream(stream Ingester_QueryStreamServer, m *QueryStreamResponse) error {
	res := &reuseResponse{res: m}

	err := sendWithContextErrChecking(stream.Context(), func() error {
		return stream.SendMsg(res)
	})

	// TODO This is unsafe! The marshalled payload is enqueued in transpont control flow component (http2 transport) and
	// it's still retained after SendMsg() is returned.
	queryStreamResponseDataPool.Put(res.data[:0])
	return err
}

type reuseResponse struct {
	res  *QueryStreamResponse
	data []byte
}

func (r *reuseResponse) Marshal() (dAtA []byte, err error) {
	size := r.res.Size()

	dataFromPool := queryStreamResponseDataPool.Get()
	if dataFromPool == nil {
		dAtA = make([]byte, size)
	} else {
		dAtA = dataFromPool.([]byte)
		if cap(dAtA) < size {
			dAtA = make([]byte, size)
		}
	}
	r.data = dAtA

	n, err := r.res.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (r *reuseResponse) Unmarshal(dAtA []byte) error {
	return r.res.Unmarshal(dAtA)
}

func (r *reuseResponse) Reset() {
	r.res.Reset()
}

func (r *reuseResponse) String() string {
	return r.res.String()
}

func (r *reuseResponse) ProtoMessage() {
	r.res.ProtoMessage()
}
