// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex_mock_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"context"

	"github.com/stretchr/testify/mock"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type IngesterServerMock struct {
	mock.Mock
}

func (m *IngesterServerMock) Push(ctx context.Context, r *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*mimirpb.WriteResponse), args.Error(1)
}

func (m *IngesterServerMock) QueryStream(r *QueryRequest, s Ingester_QueryStreamServer) error {
	args := m.Called(r, s)
	return args.Error(0)
}

func (m *IngesterServerMock) QueryExemplars(ctx context.Context, r *ExemplarQueryRequest) (*ExemplarQueryResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*ExemplarQueryResponse), args.Error(1)
}

func (m *IngesterServerMock) LabelValues(ctx context.Context, r *LabelValuesRequest) (*LabelValuesResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*LabelValuesResponse), args.Error(1)
}

func (m *IngesterServerMock) LabelNames(ctx context.Context, r *LabelNamesRequest) (*LabelNamesResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*LabelNamesResponse), args.Error(1)
}

func (m *IngesterServerMock) UserStats(ctx context.Context, r *UserStatsRequest) (*UserStatsResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*UserStatsResponse), args.Error(1)
}

func (m *IngesterServerMock) AllUserStats(ctx context.Context, r *UserStatsRequest) (*UsersStatsResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*UsersStatsResponse), args.Error(1)
}

func (m *IngesterServerMock) MetricsForLabelMatchers(ctx context.Context, r *MetricsForLabelMatchersRequest) (*MetricsForLabelMatchersResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*MetricsForLabelMatchersResponse), args.Error(1)
}

func (m *IngesterServerMock) MetricsMetadata(ctx context.Context, r *MetricsMetadataRequest) (*MetricsMetadataResponse, error) {
	args := m.Called(ctx, r)
	return args.Get(0).(*MetricsMetadataResponse), args.Error(1)
}

func (m *IngesterServerMock) LabelNamesAndValues(req *LabelNamesAndValuesRequest, srv Ingester_LabelNamesAndValuesServer) error {
	args := m.Called(req, srv)
	return args.Error(0)
}

func (m *IngesterServerMock) LabelValuesCardinality(req *LabelValuesCardinalityRequest, srv Ingester_LabelValuesCardinalityServer) error {
	args := m.Called(req, srv)
	return args.Error(0)
}

func (m *IngesterServerMock) ActiveSeries(req *ActiveSeriesRequest, srv Ingester_ActiveSeriesServer) error {
	args := m.Called(req, srv)
	return args.Error(0)
}

func (m *IngesterServerMock) ResourceAttributes(req *ResourceAttributesRequest, srv Ingester_ResourceAttributesServer) error {
	args := m.Called(req, srv)
	return args.Error(0)
}
