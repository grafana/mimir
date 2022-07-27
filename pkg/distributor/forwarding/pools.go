// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"bytes"
	"sync"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// pools is the collection of pools which the forwarding uses when building remote_write requests.
// Even though protobuf and snappy are both pools of []byte we keep them separate because the slices
// which they contain are likely to have very different sizes.
type pools struct {
	protobuf    sync.Pool
	snappy      sync.Pool
	request     sync.Pool
	bytesReader sync.Pool
	tsByTargets sync.Pool

	// Mockable for testing.
	getProtobuf    func() *[]byte
	putProtobuf    func(*[]byte)
	getSnappy      func() *[]byte
	putSnappy      func(*[]byte)
	getReq         func() *request
	putReq         func(*request)
	getBytesReader func() *bytes.Reader
	putBytesReader func(*bytes.Reader)
	getTsByTargets func() tsByTargets
	putTsByTargets func(tsByTargets)
	getTs          func() *mimirpb.TimeSeries
	putTs          func(*mimirpb.TimeSeries)
	getTsSlice     func() []mimirpb.PreallocTimeseries
	putTsSlice     func([]mimirpb.PreallocTimeseries)
}

func newPools() *pools {
	p := &pools{
		protobuf:    sync.Pool{New: func() interface{} { return &[]byte{} }},
		snappy:      sync.Pool{New: func() interface{} { return &[]byte{} }},
		request:     sync.Pool{New: func() interface{} { return &request{} }},
		bytesReader: sync.Pool{New: func() interface{} { return bytes.NewReader(nil) }},
		tsByTargets: sync.Pool{New: func() interface{} { return make(tsByTargets) }},
	}

	p.getProtobuf = getter[*[]byte](&p.protobuf)
	p.putProtobuf = putter[*[]byte](&p.protobuf)
	p.getSnappy = getter[*[]byte](&p.snappy)
	p.putSnappy = putter[*[]byte](&p.snappy)
	p.getReq = getter[*request](&p.request)
	p.putReq = putter[*request](&p.request)
	p.getBytesReader = getter[*bytes.Reader](&p.bytesReader)
	p.putBytesReader = putter[*bytes.Reader](&p.bytesReader)
	p.getTsByTargets = getter[tsByTargets](&p.tsByTargets)
	p.putTsByTargets = cleanTsByTargets(putter[tsByTargets](&p.tsByTargets))
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

func cleanTsByTargets(putter func(val tsByTargets)) func(tsByTargets) {
	return func(val tsByTargets) {
		for key := range val {
			delete(val, key)
		}
		putter(val)
	}
}
