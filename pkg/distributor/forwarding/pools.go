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

	p.getProtobuf = getProtobuf(&p.protobuf)
	p.putProtobuf = putProtobuf(&p.protobuf)
	p.getSnappy = getSnappy(&p.snappy)
	p.putSnappy = putSnappy(&p.snappy)
	p.getReq = getReq(&p.request)
	p.putReq = putReq(&p.request)
	p.getBytesReader = getBytesReader(&p.bytesReader)
	p.putBytesReader = putBytesReader(&p.bytesReader)
	p.getTsByTargets = getTsByTargets(&p.tsByTargets)
	p.putTsByTargets = putTsByTargets(&p.tsByTargets)
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

func getBytesReader(pool *sync.Pool) func() *bytes.Reader {
	return func() *bytes.Reader {
		return pool.Get().(*bytes.Reader)
	}
}

func putBytesReader(pool *sync.Pool) func(*bytes.Reader) {
	return func(bytesReader *bytes.Reader) {
		pool.Put(bytesReader)
	}
}

func getTsByTargets(pool *sync.Pool) func() tsByTargets {
	return func() tsByTargets {
		return pool.Get().(tsByTargets)
	}
}

func putTsByTargets(pool *sync.Pool) func(tsByTargets) {
	return func(tsByTargets tsByTargets) {
		for key := range tsByTargets {
			delete(tsByTargets, key)
		}
		pool.Put(tsByTargets)
	}
}
