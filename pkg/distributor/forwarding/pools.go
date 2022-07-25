// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"sync"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// pools is the collection of pools which the forwarding uses when building remote_write requests.
// Even though protobuf and snappy are both pools of []byte we keep them separate because the slices
// which they contain are likely to have very different sizes.
type pools struct {
	protobuf sync.Pool
	snappy   sync.Pool
	request  sync.Pool

	// Mockable for testing.
	getProtobuf func() *[]byte
	putProtobuf func(*[]byte)
	getSnappy   func() *[]byte
	putSnappy   func(*[]byte)
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
		snappy:   sync.Pool{New: func() interface{} { return &[]byte{} }},
		request:  sync.Pool{New: func() interface{} { return &request{} }},
	}

	p.getProtobuf = getProtobuf(&p.protobuf)
	p.putProtobuf = putProtobuf(&p.protobuf)
	p.getSnappy = getSnappy(&p.snappy)
	p.putSnappy = putSnappy(&p.snappy)
	p.getReq = getReq(&p.request)
	p.putReq = putReq(&p.request)
	p.getTs = mimirpb.TimeseriesFromPool
	p.putTs = mimirpb.ReuseTimeseries
	p.getTsSlice = mimirpb.PreallocTimeseriesSliceFromPool
	p.putTsSlice = mimirpb.ReuseSlice

	return p
}

func getProtobuf(pool *sync.Pool) func() *[]byte {
	return func() *[]byte {
		return pool.Get().(*[]byte)
	}
}

func putProtobuf(pool *sync.Pool) func(*[]byte) {
	return func(protobuf *[]byte) {
		pool.Put(protobuf)
	}
}

func getSnappy(pool *sync.Pool) func() *[]byte {
	return func() *[]byte {
		return pool.Get().(*[]byte)
	}
}

func putSnappy(pool *sync.Pool) func(*[]byte) {
	return func(snappy *[]byte) {
		pool.Put(snappy)
	}
}

func getReq(pool *sync.Pool) func() *request {
	return func() *request {
		return pool.Get().(*request)
	}
}

func putReq(pool *sync.Pool) func(*request) {
	return func(req *request) {
		pool.Put(req)
	}
}
