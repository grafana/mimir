// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"google.golang.org/grpc/mem"

	"github.com/grafana/mimir/pkg/mimirpb"
)

var _ mimirpb.UnmarshalerV2 = &PrometheusResponse{}

func (m *PrometheusResponse) SetBuffer(buf mem.Buffer) {
	m.buffer = buf
}

func (m *PrometheusResponse) FreeBuffer() {
	if m.buffer != nil {
		m.buffer.Free()
		m.buffer = nil
	}
}
