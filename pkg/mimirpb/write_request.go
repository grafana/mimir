// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"sync"

	"go.uber.org/atomic"
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

	bytes atomic.Pointer[[]byte]
}

func (wr *ReusableWriteRequest) Marshal() ([]byte, error) {
	var b []byte
	bp := wr.bytes.Load()
	if bp != nil {
		b = *bp
	} else {
		bp = new([]byte)
	}

	size := wr.Size()
	if cap(b) < size {
		b = make([]byte, size)
		*bp = b
	}
	wr.bytes.Store(bp)

	n, err := wr.MarshalToSizedBuffer(b[:size])
	if err != nil {
		return nil, err
	}

	return b[:n], nil
}

func (wr *ReusableWriteRequest) Release() {
	reusableWriteRequestsPool.Put(wr)
}
