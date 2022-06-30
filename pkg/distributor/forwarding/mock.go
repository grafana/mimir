// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"context"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type mockForwarder struct {
	ingest bool

	// Optional callback to run in place of the actual forwarding request.
	forwardReqCallback func()
}

func NewMockForwarder(ingest bool, forwardReqCallback func()) Forwarder {
	return &mockForwarder{
		ingest:             ingest,
		forwardReqCallback: forwardReqCallback,
	}
}

func (m *mockForwarder) Forward(ctx context.Context, forwardingRules validation.ForwardingRules, ts []mimirpb.PreallocTimeseries) (TimeseriesCounts, []mimirpb.PreallocTimeseries, chan error) {
	errCh := make(chan error)

	go func() {
		defer close(errCh)

		if m.forwardReqCallback != nil {
			m.forwardReqCallback()
		}
	}()

	var notIngestedCounts TimeseriesCounts

	if m.ingest {
		return notIngestedCounts, ts, errCh
	}

	for _, t := range ts {
		notIngestedCounts.SampleCount += len(t.Samples)
		notIngestedCounts.ExemplarCount += len(t.Exemplars)
	}

	return notIngestedCounts, nil, errCh
}

func (m *mockForwarder) Stop() {}
