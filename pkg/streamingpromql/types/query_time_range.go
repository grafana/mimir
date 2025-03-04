package types

import (
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/model/timestamp"
)

func init() {
	jsoniter.RegisterTypeEncoderFunc("types.QueryTimeRange", encodeQueryTimeRange, func(_ unsafe.Pointer) bool { return false })
	jsoniter.RegisterTypeDecoderFunc("types.QueryTimeRange", decodeQueryTimeRange)
}

func encodeQueryTimeRange(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	timeRange := (*QueryTimeRange)(ptr)

	stream.WriteObjectStart()

	if timeRange.IntervalMilliseconds == 1 && timeRange.StepCount == 1 {
		stream.WriteObjectField("at")
		stream.WriteInt64(timeRange.StartT)
	} else {
		stream.WriteObjectField("start")
		stream.WriteInt64(timeRange.StartT)
		stream.WriteMore()
		stream.WriteObjectField("end")
		stream.WriteInt64(timeRange.EndT)
		stream.WriteMore()
		stream.WriteObjectField("interval")
		stream.WriteInt64(timeRange.IntervalMilliseconds)
	}

	stream.WriteObjectEnd()
}

func decodeQueryTimeRange(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	timeRange := (*QueryTimeRange)(ptr)

	isInstant := false
	start := int64(0)
	end := int64(0)
	interval := int64(0)

	iter.ReadObjectCB(func(iter *jsoniter.Iterator, key string) bool {
		switch key {
		case "at":
			isInstant = true
			start = iter.ReadInt64()
		case "start":
			start = iter.ReadInt64()
		case "end":
			end = iter.ReadInt64()
		case "interval":
			interval = iter.ReadInt64()
		default:
			// Unknown field, skip.
			iter.Skip()
		}

		return true
	})

	if isInstant && (end != 0 || interval != 0) {
		iter.ReportError("decodeQueryTimeRange", "field for both instant and range query time range present")
		return
	}

	if isInstant {
		*timeRange = NewInstantQueryTimeRange(timestamp.Time(start))
	} else {
		*timeRange = NewRangeQueryTimeRange(timestamp.Time(start), timestamp.Time(end), time.Duration(interval)*time.Millisecond)
	}
}
