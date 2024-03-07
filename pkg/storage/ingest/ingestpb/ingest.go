// SPDX-License-Identifier: AGPL-3.0-only

package ingestpb

import (
	"fmt"

	"github.com/golang/snappy"
	"github.com/pkg/errors"

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

	// Decode data.
	var decodedData []byte
	if m.SnappyEncoded {
		var err error
		decodedData, err = snappy.Decode(nil, m.Data)
		if err != nil {
			return nil, errors.Wrap(err, "failed to snappy decode data")
		}
	} else {
		decodedData = m.Data
	}

	wr := &mimirpb.PreallocWriteRequest{}
	if err := wr.Unmarshal(decodedData); err != nil {
		return nil, err
	}
	return &wr.WriteRequest, nil
}
