// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/json_helpers.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

import (
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

func init() {
	jsoniter.RegisterTypeDecoderFunc("labels.Labels", decodeLabels)
	jsoniter.RegisterTypeEncoderFunc("labels.Labels", encodeLabels, labelsIsEmpty)
	jsoniter.RegisterTypeDecoderFunc("model.Time", decodeModelTime)
	jsoniter.RegisterTypeEncoderFunc("model.Time", encodeModelTime, modelTimeIsEmpty)
}

// Override Prometheus' labels.Labels decoder which goes via a map
func decodeLabels(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	labelsPtr := (*labels.Labels)(ptr)
	builder := labels.NewScratchBuilder(10)
	iter.ReadMapCB(func(iter *jsoniter.Iterator, key string) bool {
		value := iter.ReadString()
		builder.Add(key, value)
		return true
	})
	// Labels are always sorted, but earlier Mimir using a map would
	// output in any order so we have to sort on read in
	builder.Sort()
	*labelsPtr = builder.Labels()
}

// Override Prometheus' labels.Labels encoder which goes via a map
func encodeLabels(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	labelsPtr := (*labels.Labels)(ptr)
	stream.WriteObjectStart()
	i := 0
	labelsPtr.Range(func(v labels.Label) {
		if i != 0 {
			stream.WriteMore()
		}
		i++
		stream.WriteString(v.Name)
		stream.WriteRaw(`:`)
		stream.WriteString(v.Value)
	})
	stream.WriteObjectEnd()
}

func labelsIsEmpty(ptr unsafe.Pointer) bool {
	labelsPtr := (*labels.Labels)(ptr)
	return labelsPtr.IsEmpty()
}

// Decode via jsoniter's float64 routine is faster than getting the string data and decoding as two integers
func decodeModelTime(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	pt := (*model.Time)(ptr)
	f := iter.ReadFloat64()
	*pt = model.Time(int64(f * 1000))
}

// Write out the timestamp as an int divided by 1000. This is ~3x faster than converting to a float.
// Adapted from https://github.com/prometheus/prometheus/blob/cc39021b2bb6f829c7a626e4bdce2f338d1b76db/web/api/v1/api.go#L829
func encodeModelTime(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	pt := (*model.Time)(ptr)
	t := int64(*pt)
	if t < 0 {
		stream.WriteRaw(`-`)
		t = -t
	}
	stream.WriteInt64(t / 1000)
	fraction := t % 1000
	if fraction != 0 {
		stream.WriteRaw(`.`)
		if fraction < 100 {
			stream.WriteRaw(`0`)
		}
		if fraction < 10 {
			stream.WriteRaw(`0`)
		}
		stream.WriteInt64(fraction)
	}
}

func modelTimeIsEmpty(ptr unsafe.Pointer) bool {
	return false
}
