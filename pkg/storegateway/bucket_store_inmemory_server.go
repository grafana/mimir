// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_store_inmemory_server.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"context"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/storegateway/hintspb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// bucketStoreSeriesServer is an fake in-memory gRPC server used to
// call Thanos BucketStore.Series() without having to go through the
// gRPC networking stack.
type bucketStoreSeriesServer struct {
	// This field just exist to pseudo-implement the unused methods of the interface.
	storepb.Store_SeriesServer

	ctx context.Context

	SeriesSet []*storepb.Series
	Warnings  storage.Warnings
	Hints     hintspb.SeriesResponseHints
}

func newBucketStoreSeriesServer(ctx context.Context) *bucketStoreSeriesServer {
	return &bucketStoreSeriesServer{ctx: ctx}
}

func (s *bucketStoreSeriesServer) Send(r *storepb.SeriesResponse) error {
	if r.GetWarning() != "" {
		s.Warnings = append(s.Warnings, errors.New(r.GetWarning()))
	}

	if rawHints := r.GetHints(); rawHints != nil {
		// We expect only 1 hints entry so we just keep 1.
		if err := types.UnmarshalAny(rawHints, &s.Hints); err != nil {
			return errors.Wrap(err, "failed to unmarshal series hints")
		}
	}

	if recvSeries := r.GetSeries(); recvSeries != nil {
		// Thanos uses a pool for the chunks and may use other pools in the future.
		// Given we need to retain the reference after the pooled slices are recycled,
		// we need to do a copy here. We prefer to stay on the safest side at this stage
		// so we do a marshal+unmarshal to copy the whole series.
		recvSeriesData, err := recvSeries.Marshal()
		if err != nil {
			return errors.Wrap(err, "marshal received series")
		}

		copiedSeries := &storepb.Series{}
		if err = copiedSeries.Unmarshal(recvSeriesData); err != nil {
			return errors.Wrap(err, "unmarshal received series")
		}

		s.SeriesSet = append(s.SeriesSet, copiedSeries)
	}

	return nil
}

func (s *bucketStoreSeriesServer) Context() context.Context {
	return s.ctx
}
