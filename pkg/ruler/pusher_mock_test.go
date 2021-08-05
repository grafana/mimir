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
