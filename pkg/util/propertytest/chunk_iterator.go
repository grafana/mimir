package propertytest

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type IteratorValue struct {
	It         chunkenc.Iterator
	Mint, Maxt int64         // inclusive
	Values     []interface{} // expected values
}

// RequireIteratorProperties checks properties of the iterator
func RequireIteratorProperties(t *testing.T, generateIterator func(r *rand.Rand) IteratorValue) {
	failedCheck := "n/a"

	isMonotone := func(iv IteratorValue) bool {
		var lastTs int64
		it := iv.It
		for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
			ts := it.AtT()
			if ts < lastTs {
				return false
			}
			lastTs = ts
		}
		return true
	}
	err := quick.Check(isMonotone, &quick.Config{
		Values: func(values []reflect.Value, r *rand.Rand) {
			values[0] = reflect.ValueOf(generateIterator(r))
		}},
	)
	require.NoError(t, err, "Iterator is monotone")

	matchesExpectedValues := func(iv IteratorValue) bool {
		it := iv.It
		count := 0
		for valType := it.Next(); valType != chunkenc.ValNone; valType = it.Next() {
			expectedValue := iv.Values[count]
			count++
			switch s := expectedValue.(type) {
			case mimirpb.Sample:
				if chunkenc.ValFloat != valType {
					failedCheck = fmt.Sprintf("value %v not a float", count)
					return false
				}
				ts, v := it.At()
				if s.TimestampMs != ts || s.Value != v {
					return false
				}
			case mimirpb.Histogram:
				if s.IsFloatHistogram() {
					if chunkenc.ValFloatHistogram != valType {
						failedCheck = fmt.Sprintf("value %v not a float histogram", count)
						return false
					}
					ts, h := it.AtFloatHistogram()
					if s.Timestamp != ts || h == nil {
						return false
					}
				} else {
					if chunkenc.ValHistogram != valType {
						failedCheck = fmt.Sprintf("value %v not a histogram", count)
						return false
					}
					ts, h := it.AtHistogram()
					if s.Timestamp != ts || h == nil {
						return false
					}
				}
			default:
				failedCheck = fmt.Sprintf("expected value type %T not supported", expectedValue)
				return false
			}
		}
		if count != len(iv.Values) {
			failedCheck = fmt.Sprintf("count=%v should have been %v", count, len(iv.Values))
			return false
		}
		return true
	}

	failedCheck = "n/a"
	err = quick.Check(matchesExpectedValues, &quick.Config{
		Values: func(values []reflect.Value, r *rand.Rand) {
			values[0] = reflect.ValueOf(generateIterator(r))
		}},
	)
	require.NoError(t, err, fmt.Sprintf("Iterator has the correct values failed: %s", failedCheck))
}
