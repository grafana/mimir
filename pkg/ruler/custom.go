// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"google.golang.org/grpc/mem"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var _ mimirpb.BufferHolder = &RulesResponse{}

func (m *RulesResponse) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *RulesResponse) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}
