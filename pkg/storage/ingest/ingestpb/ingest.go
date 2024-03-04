// SPDX-License-Identifier: AGPL-3.0-only

package ingestpb

import (
	"fmt"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func (m *Segment) ClearUnmarshalData() {
	for _, piece := range m.Pieces {
		if piece.ObsoleteWriteRequest != nil {
			piece.ObsoleteWriteRequest.ClearTimeseriesUnmarshalData()
		}
	}
}

func (m *Piece) WriteRequest() (*mimirpb.WriteRequest, error) {
	if m.ObsoleteWriteRequest != nil {
		return m.ObsoleteWriteRequest, nil
	}

	if m.Data == nil {
		return nil, fmt.Errorf("no data")
	}

	wr := &mimirpb.PreallocWriteRequest{}
	if err := wr.Unmarshal(m.Data); err != nil {
		return nil, err
	}
	return &wr.WriteRequest, nil
}
