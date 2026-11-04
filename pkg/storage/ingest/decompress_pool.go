// SPDX-License-Identifier: AGPL-3.0-only
package ingest

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type decompressBufferPool struct{ buffers sync.Pool }

func (p *decompressBufferPool) GetDecompressBytes(_ []byte, _ kgo.CompressionCodecType) []byte {
	if buffer := p.buffers.Get(); buffer != nil {
		return *buffer.(*[]byte)
	}
	return nil
}
func (p *decompressBufferPool) PutDecompressBytes(buffer []byte) {
	// Large backfills must not keep their peak allocation in every fetcher.
	if cap(buffer) > 8<<20 {
		return
	}
	buffer = buffer[:0]
	p.buffers.Put(&buffer)
}
func recycleRecords(records []*kgo.Record) {
	for _, record := range records {
		record.Recycle()
	}
}
