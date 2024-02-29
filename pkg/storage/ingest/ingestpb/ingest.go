// SPDX-License-Identifier: AGPL-3.0-only

package ingestpb

func (m *Segment) ClearUnmarshalData() {
	for _, piece := range m.Pieces {
		piece.WriteRequests.ClearTimeseriesUnmarshalData()
	}
}
