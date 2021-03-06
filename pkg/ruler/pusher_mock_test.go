// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/pusher_mock_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type pusherMock struct {
	mock.Mock
}

func newPusherMock() *pusherMock {
	return &pusherMock{}
}

func (m *pusherMock) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	args := m.Called(ctx, req)
	return args.Get(0).(*mimirpb.WriteResponse), args.Error(1)
}

func (m *pusherMock) MockPush(res *mimirpb.WriteResponse, err error) {
	m.On("Push", mock.Anything, mock.Anything).Return(res, err)
}
