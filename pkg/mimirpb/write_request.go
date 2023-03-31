// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"sync"
)

var reusableWriteRequestsPool = sync.Pool{
	New: func() any { return new(ReusableWriteRequest) },
}

func NewReusableWriteRequest(request WriteRequest) *ReusableWriteRequest {
	wr := reusableWriteRequestsPool.Get().(*ReusableWriteRequest)
	wr.WriteRequest = request
	return wr
}

type ReusableWriteRequest struct {
	WriteRequest

	bytes []byte
}

// Marshal reuses the internal buffer to marshal the request.
// It is not thread-safe.
func (wr *ReusableWriteRequest) Marshal() ([]byte, error) {
	size := wr.Size()
	if cap(wr.bytes) < size {
		wr.bytes = make([]byte, size)
	}

	n, err := wr.MarshalToSizedBuffer(wr.bytes[:size])
	if err != nil {
		return nil, err
	}

	return wr.bytes[:n], nil
}

func (wr *ReusableWriteRequest) Release() {
	reusableWriteRequestsPool.Put(wr)
}
