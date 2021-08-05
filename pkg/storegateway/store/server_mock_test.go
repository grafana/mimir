// Included-from-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/proxy_test.go
// Included-from-license: Apache-2.0
// Included-from-copyright: The Thanos Authors.

package store

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

// storeSeriesServer is test gRPC storeAPI series server.
// TODO(bwplotka): Make this part of some common library. We copy and paste this also in pkg/store.
type storeSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer

	ctx context.Context

	SeriesSet []*storepb.Series
	Warnings  []string
	HintsSet  []*types.Any

	Size int64
}

func newStoreSeriesServer(ctx context.Context) *storeSeriesServer {
	return &storeSeriesServer{ctx: ctx}
}

func (s *storeSeriesServer) Send(r *storepb.SeriesResponse) error {
	s.Size += int64(r.Size())

	if r.GetWarning() != "" {
		s.Warnings = append(s.Warnings, r.GetWarning())
		return nil
	}

	if r.GetSeries() != nil {
		s.SeriesSet = append(s.SeriesSet, r.GetSeries())
		return nil
	}

	if r.GetHints() != nil {
		s.HintsSet = append(s.HintsSet, r.GetHints())
		return nil
	}

	// Unsupported field, skip.
	return nil
}

func (s *storeSeriesServer) Context() context.Context {
	return s.ctx
}
