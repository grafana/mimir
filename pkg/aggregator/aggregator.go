package aggregator

import (
	"math"
	"time"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// userAggregations contains the aggregation state for all users.
// Note that it and all its sub-structs are not thread-safe, to gain parallelism we partition the Kafka topic.
type userAggregations struct {
	interval int64
	delay    int64
	byUser   map[string]*aggregations
}

// newUserAggregations creates a new userAggregations object, following is a description of the parameters.
//
// interval:
// The interval at which to generate an aggregated sample.
// Note that the aggregation isn't triggered by a timer, it's triggered by newer samples arriving.
// The interval parameter determines the spacing between aggregated samples, but it doesn't guarantee that an aggregated
// sample will be generated every interval.
//
// delay:
// The delay to wait before generating an aggregated sample based on a raw sample,
// this is the tolerance for samples to arrive late.
func newUserAggregations(interval, delay time.Duration) userAggregations {
	return userAggregations{
		interval: interval.Milliseconds(),
		delay:    delay.Milliseconds(),
		byUser:   map[string]*aggregations{},
	}
}

// ingest ingests a sample for aggregation.
// It might return an aggregated sample if this ingestion has resulted in one, otherwise it will return a sample with a NaN value.
func (u *userAggregations) ingest(user, aggregatedLabels, rawLabels string, sample mimirpb.Sample) mimirpb.Sample {
	aggs, ok := u.byUser[user]
	if !ok {
		aggs = newAggregations()
		u.byUser[user] = aggs
	}

	aggSample := aggs.ingest(u.interval, u.delay, sample, aggregatedLabels, rawLabels)

	return aggSample
}

// aggregations represents the aggregation state for a single user.
type aggregations struct {
	// aggregations is keyed by the labelset of the aggregated series that we're generating.
	aggregations map[string]*aggregation
}

func newAggregations() *aggregations {
	return &aggregations{
		aggregations: map[string]*aggregation{},
	}
}

func (a *aggregations) ingest(interval, delay int64, sample mimirpb.Sample, aggregatedLabels, rawLabels string) mimirpb.Sample {
	agg, ok := a.aggregations[aggregatedLabels]
	if !ok {
		agg = newAggregation()
		a.aggregations[aggregatedLabels] = agg
	}

	aggSample := agg.ingest(interval, delay, sample, rawLabels)

	return aggSample
}

// aggregation is the state of one aggregated series (not an aggregated metrics).
type aggregation struct {
	// rawSeries is keyed by the labelset of the raw series that we're aggregating.
	rawSeries map[string]*timeBuckets

	// lastTimestamp is the last timestamp for which an aggregation has been generated.
	lastTimestamp int64
	// lastValue is the value of the last aggregation which has been generated.
	lastValue float64
}

func newAggregation() *aggregation {
	return &aggregation{
		rawSeries: map[string]*timeBuckets{},
		lastValue: math.NaN(),
	}
}

func (a *aggregation) ingest(interval, delay int64, sample mimirpb.Sample, rawLabels string) (aggSample mimirpb.Sample) {
	aggregationTs := getAggregationTs(sample.TimestampMs, interval)
	if aggregationTs <= a.lastTimestamp {
		// Aggregated sample which would be generated from the sample with the given timestamp has already been generated.
		// Dropping the sample.
		return
	}

	rawSeries, ok := a.rawSeries[rawLabels]
	if !ok {
		rawSeries = newTimeBuckets(bucketCount(interval, delay))
		a.rawSeries[rawLabels] = rawSeries
	}
	rawSeries.ingest(sample, interval)

	lastEligbleTs := getAggregationTs(sample.TimestampMs-delay-interval, interval)
	if lastEligbleTs > a.lastTimestamp {
		aggSample = a.aggregateTo(lastEligbleTs, interval)
	} else {
		aggSample = mimirpb.Sample{
			TimestampMs: lastEligbleTs,
			Value:       math.NaN(),
		}
	}

	return
}

func (a *aggregation) aggregateTo(toTs, interval int64) mimirpb.Sample {
	increasesSum := math.NaN()
	highestTs := int64(0)
	for key, rawSeries := range a.rawSeries {
		ts, increase := rawSeries.increaseToTs(toTs)
		a.rawSeries[key] = rawSeries

		if !math.IsNaN(increase) {
			if math.IsNaN(increasesSum) {
				increasesSum = 0
			}

			increasesSum += increase
		}

		if ts > highestTs {
			highestTs = ts
		}
	}

	var returnValue float64
	if math.IsNaN(increasesSum) {
		returnValue = math.NaN()
	} else {
		if math.IsNaN(a.lastValue) {
			a.lastValue = increasesSum
		} else {
			a.lastValue += increasesSum
		}
		returnValue = a.lastValue
	}

	if highestTs > 0 {
		a.lastTimestamp = getAggregationTs(highestTs, interval)
	} else {
		a.lastTimestamp = getAggregationTs(toTs, interval)
	}

	return mimirpb.Sample{
		TimestampMs: a.lastTimestamp,
		Value:       returnValue,
	}
}

// timeBuckets is a fixed size ring buffer of time buckets for which we're generating aggregations.
// Its size is determined by the aggregation interval and aggregation delay, it is calculated like:
// ceil( ( <aggregation delay> + <aggregation interval> ) / <aggregation interval> )
type timeBuckets struct {
	current int // Pointer to the current time bucket.
	buckets []timeBucket
}

func newTimeBuckets(bucketCount int) *timeBuckets {
	tb := &timeBuckets{
		buckets: make([]timeBucket, bucketCount),
	}
	for i := range tb.buckets {
		tb.buckets[i].sample.Value = math.NaN()
	}
	return tb
}

func bucketCount(interval, delay int64) int {
	totalRetention := delay + 2*interval
	totalRetention += interval - (delay % interval) // round up to next multiple of interval
	return int(totalRetention / interval)
}

// getAggregationTs takes the timestamp of a sample and the configured interval,
// it returns the timestamp to assign to the aggregated sample with the given timestamp.
func getAggregationTs(timestamp, interval int64) int64 {
	// the timestamp of the aggregated sample is the last millisecond of the interval bucket.
	return timestamp/interval*interval + interval - 1
}

func (t *timeBuckets) ingest(sample mimirpb.Sample, interval int64) {
	sampleBucketTs := getAggregationTs(sample.TimestampMs, interval)
	currentBucketTs := getAggregationTs(t.buckets[t.current].sample.TimestampMs, interval)
	if currentBucketTs == sampleBucketTs {
		// The sample is in the current bucket, we can just replace that bucket.
		t.buckets[t.current].sample = sample
		return
	}
	if currentBucketTs < sampleBucketTs {
		// The sample is in a future bucket, we give it a new bucket.
		t.current = incWrapped(t.current, len(t.buckets))
		t.buckets[t.current].sample = sample
		return
	}
}

func (t *timeBuckets) increaseToTs(toTs int64) (int64, float64) {
	increase := math.NaN()
	ts := int64(0)

	lowerBound, upperBound := -1, -1
	for i := 0; i < len(t.buckets); i++ {
		bucket := subtractWrapped(t.current, len(t.buckets), i)
		if upperBound < 0 {
			// Looking for the upper bound.
			if t.buckets[bucket].sample.TimestampMs != 0 && t.buckets[bucket].sample.TimestampMs <= toTs {
				upperBound = bucket
				ts = t.buckets[bucket].sample.TimestampMs
			}
		} else {
			// looking for the lower bound.
			if t.buckets[bucket].sample.TimestampMs != 0 && t.buckets[bucket].sample.TimestampMs <= t.buckets[upperBound].sample.TimestampMs {
				lowerBound = bucket
				break
			}
		}
	}

	if upperBound >= 0 {
		if lowerBound >= 0 {
			if t.buckets[upperBound].sample.Value > t.buckets[lowerBound].sample.Value {
				increase = t.buckets[upperBound].sample.Value - t.buckets[lowerBound].sample.Value
			} else {
				// If the upper bound is smaller than the lower bound, the counter has been reset.
				increase = t.buckets[upperBound].sample.Value
			}
			t.buckets[lowerBound].sample.Value = math.NaN()
			t.buckets[lowerBound].sample.TimestampMs = 0
		} else {
			// We have only the upper bound, we can't calculate the increase, taking the absolute value of the upper bound as increase.
			increase = t.buckets[upperBound].sample.Value
		}
	}

	return ts, increase
}

// subtractWrapped subtracts the integer <sub> from the integer <i>, it wraps around at 0 going back to <size>.
func subtractWrapped(i, size, sub int) int {
	return (i + size - sub) % size
}

// incWrapped increases the integer <i> by one, if <i> is <size> - 1 it returns 0 (wrapping around).
func incWrapped(i, size int) int {
	if i == size-1 {
		return 0
	}

	return i + 1
}

// time bucket represents one interval of time for which we're generating an aggregation,
// the time bucket begins and ends at multiples of the aggregation interval,
// its time range is inclusive - exclusive.
// Its timestamp and value properties are always set to the values of the most recent sample of this bucket.
type timeBucket struct {
	sample mimirpb.Sample
}
