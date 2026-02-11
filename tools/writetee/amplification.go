// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"

	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	amplifiedLabelName = "__amplified__"
)

// isRW2Error checks if an error is caused by trying to unmarshal RW 2.0 data
func isRW2Error(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Remote Write 2.0") || strings.Contains(errStr, "Symbols")
}

// hashSeriesLabels creates a deterministic hash from RW 1.0 series labels.
// This ensures the same series always gets the same hash value for consistent sampling.
func hashSeriesLabels(labels []mimirpb.LabelAdapter) uint64 {
	h := fnv.New64a()
	for _, label := range labels {
		// Write label name and value to hash
		h.Write([]byte(label.Name))
		h.Write([]byte{0}) // separator
		h.Write([]byte(label.Value))
		h.Write([]byte{0}) // separator
	}
	return h.Sum64()
}

// hashSeriesLabelsRW2 creates a deterministic hash from RW 2.0 series label references.
// This ensures the same series always gets the same hash value for consistent sampling.
func hashSeriesLabelsRW2(labelRefs []uint32, symbols []string) uint64 {
	h := fnv.New64a()
	for _, ref := range labelRefs {
		// Write the symbol string to hash
		if int(ref) < len(symbols) {
			h.Write([]byte(symbols[ref]))
			h.Write([]byte{0}) // separator
		}
	}
	return h.Sum64()
}

// shouldKeepSeries deterministically decides if a series should be kept based on its hash.
// For amplification factor f (where 0 < f < 1), a series is kept if hash % 1000 < f * 1000.
// This ensures consistent sampling - the same series will always be kept or excluded.
func shouldKeepSeries(hash uint64, amplificationFactor float64) bool {
	// Use modulo 10000 for fine-grained precision (0.01% granularity)
	threshold := uint64(amplificationFactor * 10000)
	return (hash % 10000) < threshold
}

// AmplificationTracker tracks RW 1.0 vs RW 2.0 series counts for observability.
// No longer performs dynamic adjustment - both RW 1.0 and RW 2.0 use the target amplification factor.
type AmplificationTracker struct {
	mu             sync.RWMutex
	totalRW1Series int64
	totalRW2Series int64
	lastReset      time.Time
	resetInterval  time.Duration
}

// NewAmplificationTracker creates a new tracker.
// The tracker resets its counters every hour to keep stats fresh.
func NewAmplificationTracker() *AmplificationTracker {
	return &AmplificationTracker{
		lastReset:     time.Now(),
		resetInterval: time.Hour,
	}
}

// RecordRW1Series records RW 1.0 series count.
func (t *AmplificationTracker) RecordRW1Series(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checkAndReset()
	t.totalRW1Series += int64(count)
}

// RecordRW2Series records RW 2.0 series count.
func (t *AmplificationTracker) RecordRW2Series(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checkAndReset()
	t.totalRW2Series += int64(count)
}

// checkAndReset resets counters if the reset interval has elapsed.
// Must be called with mu locked.
func (t *AmplificationTracker) checkAndReset() {
	if time.Since(t.lastReset) >= t.resetInterval {
		t.totalRW1Series = 0
		t.totalRW2Series = 0
		t.lastReset = time.Now()
	}
}

// GetStats returns current statistics.
func (t *AmplificationTracker) GetStats() (rw1Series, rw2Series int64, rw2Ratio float64) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	total := t.totalRW1Series + t.totalRW2Series
	var ratio float64
	if total > 0 {
		ratio = float64(t.totalRW2Series) / float64(total)
	}

	return t.totalRW1Series, t.totalRW2Series, ratio
}

// AmplificationResult contains the result of amplification along with metadata.
type AmplificationResult struct {
	Body                 []byte
	OriginalSeriesCount  int
	AmplifiedSeriesCount int
	IsRW2                bool
	WasAmplified         bool
}

// AmplifyWriteRequest takes a Prometheus remote write request body and amplifies or samples it
// based on the amplification factor.
// The amplification factor determines how to transform the time series:
//   - factor = 1.0: no change (returns original)
//   - factor > 1.0: amplification (duplication)
//   - factor = 2.0: each time series is duplicated once (2x total)
//   - factor = 1.5: each time series gets 1 full copy + deterministically selected 50% get a 2nd copy
//   - factor = 3.5: each time series gets 3 full copies + deterministically selected 50% get a 4th copy
//   - factor < 1.0: sampling (reduction)
//   - factor = 0.1: approximately 10% of series are kept (deterministic selection)
//   - factor = 0.5: approximately 50% of series are kept (deterministic selection)
//
// Amplified copies (factor > 1.0) get an additional label: __amplified__="<replica_num>"
// Sampled series (factor < 1.0) do not get additional labels.
// The body is expected to be Snappy-compressed Prometheus remote write protobuf.
//
// Selection (both sampling and fractional amplification) is deterministic and based on a hash
// of the series labels, ensuring the same series are consistently selected across requests.
//
// Both RW 1.0 and RW 2.0 requests are processed using the same amplification factor.
// RW 2.0 requests are kept in native RW 2.0 format (avoiding memory expansion).
// If tracker is provided, series counts are recorded for observability.
// Returns AmplificationResult with metadata about what was done.
func AmplifyWriteRequest(body []byte, amplificationFactor float64, tracker *AmplificationTracker) (AmplificationResult, error) {
	if amplificationFactor == 1.0 {
		return AmplificationResult{
			Body:                 body,
			OriginalSeriesCount:  0, // Unknown without unmarshaling
			AmplifiedSeriesCount: 0,
			IsRW2:                false,
			WasAmplified:         false,
		}, nil
	}

	// Decompress the snappy-compressed body
	decompressed, err := snappy.Decode(nil, body)
	if err != nil {
		return AmplificationResult{}, fmt.Errorf("failed to decompress write request: %w", err)
	}

	// Try unmarshaling as RW 1.0 first
	var req mimirpb.WriteRequest
	err = proto.Unmarshal(decompressed, &req)

	// If RW 1.0 unmarshal fails with RW 2.0 error, amplify RW 2.0 in native format
	if err != nil {
		// Check if this is a RW 2.0 request by looking for the specific error message
		if !isRW2Error(err) {
			return AmplificationResult{}, fmt.Errorf("failed to unmarshal write request: %w", err)
		}

		// Unmarshal as RW 2.0 data in native format using our custom unmarshal
		// This keeps data in native RW 2.0 format (symbol table + uint32 refs)
		// avoiding the 12.5x memory expansion of converting to RW 1.0
		rw2Req, err := mimirpb.UnmarshalWriteRequestRW2Native(decompressed)
		if err != nil {
			return AmplificationResult{}, fmt.Errorf("failed to unmarshal RW 2.0 write request: %w", err)
		}

		originalSeriesCount := len(rw2Req.Timeseries)

		// Record RW 2.0 series for observability
		if tracker != nil && originalSeriesCount > 0 {
			tracker.RecordRW2Series(originalSeriesCount)
		}

		// Handle sampling (factor < 1.0) for RW 2.0
		// Use deterministic hashing to ensure the same series are always sampled
		if amplificationFactor < 1.0 {
			sampledSeries := make([]mimirpb.TimeSeriesRW2, 0, int(float64(len(rw2Req.Timeseries))*amplificationFactor))
			for _, ts := range rw2Req.Timeseries {
				hash := hashSeriesLabelsRW2(ts.LabelsRefs, rw2Req.Symbols)
				if shouldKeepSeries(hash, amplificationFactor) {
					sampledSeries = append(sampledSeries, ts)
				}
			}

			rw2Req.Timeseries = sampledSeries

			// Marshal back to protobuf
			sampledProto, err := proto.Marshal(rw2Req)
			if err != nil {
				return AmplificationResult{}, fmt.Errorf("failed to marshal sampled RW 2.0 write request: %w", err)
			}

			// Compress back with snappy
			sampledBody := snappy.Encode(nil, sampledProto)

			return AmplificationResult{
				Body:                 sampledBody,
				OriginalSeriesCount:  originalSeriesCount,
				AmplifiedSeriesCount: len(sampledSeries) - originalSeriesCount, // Negative for sampling
				IsRW2:                true,
				WasAmplified:         true,
			}, nil
		}

		// Amplify RW 2.0 data in native format (avoids 12.5x memory expansion)
		amplifiedRW2 := amplifyRW2Request(rw2Req, amplificationFactor)

		// Marshal back to protobuf
		amplifiedProto, err := proto.Marshal(&amplifiedRW2)
		if err != nil {
			return AmplificationResult{}, fmt.Errorf("failed to marshal amplified RW 2.0 write request: %w", err)
		}

		// Compress back with snappy
		amplifiedBody := snappy.Encode(nil, amplifiedProto)

		return AmplificationResult{
			Body:                 amplifiedBody,
			OriginalSeriesCount:  originalSeriesCount,
			AmplifiedSeriesCount: len(amplifiedRW2.Timeseries) - originalSeriesCount,
			IsRW2:                true,
			WasAmplified:         true,
		}, nil
	}

	// RW 1.0 path
	originalSeriesCount := len(req.Timeseries)

	// Record RW 1.0 series for observability
	if tracker != nil {
		tracker.RecordRW1Series(originalSeriesCount)
	}

	// Handle sampling (factor < 1.0) - deterministically keep a subset of series
	// Use deterministic hashing to ensure the same series are always sampled
	if amplificationFactor < 1.0 {
		sampledSeries := make([]mimirpb.PreallocTimeseries, 0, int(float64(len(req.Timeseries))*amplificationFactor))
		for _, ts := range req.Timeseries {
			hash := hashSeriesLabels(ts.Labels)
			if shouldKeepSeries(hash, amplificationFactor) {
				sampledSeries = append(sampledSeries, ts)
			}
		}

		req.Timeseries = sampledSeries

		// Marshal back to protobuf
		sampledProto, err := proto.Marshal(&req)
		if err != nil {
			return AmplificationResult{}, fmt.Errorf("failed to marshal sampled write request: %w", err)
		}

		// Compress back with snappy
		sampledBody := snappy.Encode(nil, sampledProto)

		return AmplificationResult{
			Body:                 sampledBody,
			OriginalSeriesCount:  originalSeriesCount,
			AmplifiedSeriesCount: len(sampledSeries) - originalSeriesCount, // Negative for sampling
			IsRW2:                false,
			WasAmplified:         true,
		}, nil
	}

	// Calculate copies based on amplification factor (factor > 1.0)
	fullCopies := int(math.Floor(amplificationFactor))
	fractionalPart := amplificationFactor - float64(fullCopies)

	// RW 1.0: amplify using Timeseries
	amplifiedSeries := make([]mimirpb.PreallocTimeseries, 0, len(req.Timeseries)*fullCopies)

	for _, ts := range req.Timeseries {
		// Keep the original
		amplifiedSeries = append(amplifiedSeries, ts)

		// Create full copies
		for i := 1; i < fullCopies; i++ {
			amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, i))
		}

		// Handle fractional part deterministically using hash
		// This ensures the same series always get the fractional copy
		if fractionalPart > 0 {
			hash := hashSeriesLabels(ts.Labels)
			if shouldKeepSeries(hash, fractionalPart) {
				amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, fullCopies))
			}
		}
	}

	// Replace the time series in the request
	req.Timeseries = amplifiedSeries

	// Marshal back to protobuf
	amplifiedProto, err := proto.Marshal(&req)
	if err != nil {
		return AmplificationResult{}, fmt.Errorf("failed to marshal amplified write request: %w", err)
	}

	// Compress back with snappy
	amplifiedBody := snappy.Encode(nil, amplifiedProto)

	return AmplificationResult{
		Body:                 amplifiedBody,
		OriginalSeriesCount:  originalSeriesCount,
		AmplifiedSeriesCount: len(amplifiedSeries) - originalSeriesCount,
		IsRW2:                false,
		WasAmplified:         true,
	}, nil
}

// amplifyRW2Request amplifies a RW 2.0 request in native format.
// This avoids the 12.5x memory expansion of converting RW 2.0 → RW 1.0.
// Instead, we just add a few strings to the symbol table and duplicate uint32 label ref arrays.
func amplifyRW2Request(req *mimirpb.WriteRequestRW2, amplificationFactor float64) mimirpb.WriteRequestRW2 {
	fullCopies := int(math.Floor(amplificationFactor))
	fractionalPart := amplificationFactor - float64(fullCopies)

	// Build the amplified symbol table
	// Start with original symbols, then add __amplified__ and replica numbers
	amplifiedSymbols := make([]string, len(req.Symbols))
	copy(amplifiedSymbols, req.Symbols)

	// Add amplification label name symbol
	amplifiedLabelSymbolRef := uint32(len(amplifiedSymbols))
	amplifiedSymbols = append(amplifiedSymbols, amplifiedLabelName)

	// Add replica number symbols (1, 2, 3, ...)
	replicaSymbolRefs := make([]uint32, fullCopies)
	for i := 1; i < fullCopies; i++ {
		replicaSymbolRefs[i] = uint32(len(amplifiedSymbols))
		amplifiedSymbols = append(amplifiedSymbols, fmt.Sprintf("%d", i))
	}

	// Handle fractional part - might need one more replica symbol
	var fractionalReplicaRef uint32
	if fractionalPart > 0 {
		fractionalReplicaRef = uint32(len(amplifiedSymbols))
		amplifiedSymbols = append(amplifiedSymbols, fmt.Sprintf("%d", fullCopies))
	}

	// Amplify time series
	amplifiedSeries := make([]mimirpb.TimeSeriesRW2, 0, len(req.Timeseries)*fullCopies)

	for _, ts := range req.Timeseries {
		// Keep the original (no amplification label)
		amplifiedSeries = append(amplifiedSeries, ts)

		// Create full copies with amplification label
		for i := 1; i < fullCopies; i++ {
			amplifiedSeries = append(amplifiedSeries, amplifyTimeSeriesRW2(&ts, amplifiedLabelSymbolRef, replicaSymbolRefs[i]))
		}

		// Handle fractional part deterministically using hash
		// This ensures the same series always get the fractional copy
		if fractionalPart > 0 {
			hash := hashSeriesLabelsRW2(ts.LabelsRefs, req.Symbols)
			if shouldKeepSeries(hash, fractionalPart) {
				amplifiedSeries = append(amplifiedSeries, amplifyTimeSeriesRW2(&ts, amplifiedLabelSymbolRef, fractionalReplicaRef))
			}
		}
	}

	return mimirpb.WriteRequestRW2{
		Symbols:    amplifiedSymbols,
		Timeseries: amplifiedSeries,
	}
}

// amplifyTimeSeriesRW2 creates a copy of an RW 2.0 time series with the amplified label added.
// This operates on uint32 label references, not actual label strings.
func amplifyTimeSeriesRW2(original *mimirpb.TimeSeriesRW2, labelNameRef, labelValueRef uint32) mimirpb.TimeSeriesRW2 {
	// Copy the time series
	ts := mimirpb.TimeSeriesRW2{
		// Copy label refs and append amplification label refs
		LabelsRefs: make([]uint32, len(original.LabelsRefs)+2),
		// Copy samples
		Samples: make([]mimirpb.Sample, len(original.Samples)),
		// Copy exemplars
		Exemplars: make([]mimirpb.ExemplarRW2, len(original.Exemplars)),
		// Copy histograms
		Histograms: make([]mimirpb.Histogram, len(original.Histograms)),
		// Copy metadata
		Metadata:         original.Metadata,
		CreatedTimestamp: original.CreatedTimestamp,
	}

	// Copy label refs
	copy(ts.LabelsRefs, original.LabelsRefs)
	// Add amplification label refs
	ts.LabelsRefs[len(original.LabelsRefs)] = labelNameRef
	ts.LabelsRefs[len(original.LabelsRefs)+1] = labelValueRef

	// Deep copy samples
	copy(ts.Samples, original.Samples)

	// Deep copy exemplars
	for i := range original.Exemplars {
		ts.Exemplars[i] = mimirpb.ExemplarRW2{
			LabelsRefs: append([]uint32(nil), original.Exemplars[i].LabelsRefs...),
			Value:      original.Exemplars[i].Value,
			Timestamp:  original.Exemplars[i].Timestamp,
		}
	}

	// Deep copy histograms
	copy(ts.Histograms, original.Histograms)

	return ts
}

// amplifyTimeSeries creates a copy of a time series with the amplified label added.
// The replicaNum is included in the label value to ensure uniqueness.
func amplifyTimeSeries(original *mimirpb.PreallocTimeseries, replicaNum int) mimirpb.PreallocTimeseries {
	// Create a copy
	ts := mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			// Copy samples
			Samples: make([]mimirpb.Sample, len(original.Samples)),
			// Copy exemplars if present
			Exemplars: make([]mimirpb.Exemplar, len(original.Exemplars)),
			// Copy histograms if present
			Histograms: make([]mimirpb.Histogram, len(original.Histograms)),
		},
	}

	// Deep copy samples
	copy(ts.Samples, original.Samples)

	// Deep copy exemplars
	copy(ts.Exemplars, original.Exemplars)

	// Deep copy histograms
	copy(ts.Histograms, original.Histograms)

	// Copy and add amplified label
	// The original labels need to be copied as new strings to avoid unsafe mutations
	ts.Labels = make([]mimirpb.LabelAdapter, 0, len(original.Labels)+1)
	for _, label := range original.Labels {
		ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{
			Name:  string(label.Name),
			Value: string(label.Value),
		})
	}

	// Add the amplified label with replica number
	ts.Labels = append(ts.Labels, mimirpb.LabelAdapter{
		Name:  amplifiedLabelName,
		Value: fmt.Sprintf("%d", replicaNum),
	})

	return ts
}
