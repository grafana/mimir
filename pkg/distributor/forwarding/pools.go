// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"sync"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// pools is the collection of pools which the forwarding uses when building remote_write requests.
type pools struct {
	protobuf sync.Pool
	request  sync.Pool

	// Mockable for testing.
	getProtobuf func() *[]byte
	putProtobuf func(*[]byte)
	getReq      func() *request
	putReq      func(*request)
	getTs       func() *mimirpb.TimeSeries
	putTs       func(*mimirpb.TimeSeries)
	getTsSlice  func() []mimirpb.PreallocTimeseries
	putTsSlice  func([]mimirpb.PreallocTimeseries)
}

func newPools() *pools {
	p := &pools{
		protobuf: sync.Pool{New: func() interface{} { return &[]byte{} }},
		request:  sync.Pool{New: func() interface{} { return &request{} }},
	}

	p.getProtobuf = getter[*[]byte](&p.protobuf)
	p.putProtobuf = putter[*[]byte](&p.protobuf)
	p.getReq = getter[*request](&p.request)
	p.putReq = putter[*request](&p.request)
	p.getTs = mimirpb.TimeseriesFromPool
	p.putTs = mimirpb.ReuseTimeseries
	p.getTsSlice = mimirpb.PreallocTimeseriesSliceFromPool
	p.putTsSlice = mimirpb.ReuseSlice

	return p
}

func getter[T any](pool *sync.Pool) func() T {
	return func() T {
		return pool.Get().(T)
	}
}

func putter[T any](pool *sync.Pool) func(T) {
	return func(val T) {
		pool.Put(val)
	}
}
