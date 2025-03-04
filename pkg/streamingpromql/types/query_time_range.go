package types

import (
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/prometheus/model/timestamp"
)

type QueryTimeRange struct {
	StartT               int64 // Start timestamp, in milliseconds since Unix epoch.
	EndT                 int64 // End timestamp, in milliseconds since Unix epoch.
	IntervalMilliseconds int64 // Range query interval, or 1 for instant queries. Note that this is deliberately different to parser.EvalStmt.Interval for instant queries (where it is 0) to simplify some loop conditions.

	StepCount int // 1 for instant queries.
}

func NewInstantQueryTimeRange(t time.Time) QueryTimeRange {
	ts := timestamp.FromTime(t)

	return QueryTimeRange{
		StartT:               ts,
		EndT:                 ts,
		IntervalMilliseconds: 1,
		StepCount:            1,
	}
}

func NewRangeQueryTimeRange(start time.Time, end time.Time, interval time.Duration) QueryTimeRange {
	startT := timestamp.FromTime(start)
	endT := timestamp.FromTime(end)
	IntervalMilliseconds := interval.Milliseconds()

	return QueryTimeRange{
		StartT:               startT,
		EndT:                 endT,
		IntervalMilliseconds: IntervalMilliseconds,
		StepCount:            int((endT-startT)/IntervalMilliseconds) + 1,
	}
}

// PointIndex returns the index in the QueryTimeRange that the timestamp, t, falls on.
// t must be in line with IntervalMs (ie the step).
func (q *QueryTimeRange) PointIndex(t int64) int64 {
	return (t - q.StartT) / q.IntervalMilliseconds
}

// IndexTime returns the timestamp that the point index, p, falls on.
// p must be less than StepCount
func (q *QueryTimeRange) IndexTime(p int64) int64 {
	return q.StartT + p*q.IntervalMilliseconds
}

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
