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
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/mimirpb"
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
	// Body is the snappy-compressed protobuf body (amplified or sampled).
	Body []byte
	// OriginalSeriesCount is the number of series in the original request.
	OriginalSeriesCount int
	// AmplifiedSeriesCount is the net change in series count (positive for amplification, negative for sampling).
	AmplifiedSeriesCount int
	// IsRW2 indicates whether the request was in Remote Write 2.0 format.
	IsRW2 bool
	// WasAmplified indicates whether any transformation was applied.
	WasAmplified bool
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
// The original series is considered replica 1 and keeps its original label values.
// Additional copies (replicas 2, 3, ...) have all label values (except __name__) suffixed with
// _amp{N}, where N is the replica number. This increases cardinality across all label dimensions.
// Sampled series (factor < 1.0) do not get suffixes.
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
		// Keep the original (considered replica 1, no suffix)
		amplifiedSeries = append(amplifiedSeries, ts)

		// Skip amplification for series with only __name__ label (nothing to suffix)
		if hasOnlyNameLabel(ts.Labels) {
			continue
		}

		// Create full copies with suffixed label values (replicas 2, 3, ...)
		for i := 1; i < fullCopies; i++ {
			amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, i+1))
		}

		// Handle fractional part deterministically using hash
		// This ensures the same series always get the fractional copy
		if fractionalPart > 0 {
			hash := hashSeriesLabels(ts.Labels)
			if shouldKeepSeries(hash, fractionalPart) {
				amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, fullCopies+1))
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
// All label values (except __name__) are suffixed with _amp{N} for replicas 2, 3, etc.
// The original series is considered replica 1 and keeps its original label values.
func amplifyRW2Request(req *mimirpb.WriteRequestRW2, amplificationFactor float64) mimirpb.WriteRequestRW2 {
	fullCopies := int(math.Floor(amplificationFactor))
	fractionalPart := amplificationFactor - float64(fullCopies)

	// Find __name__ symbol ref (to identify which values to exclude from suffixing)
	nameSymbolRef := findSymbolRef(req.Symbols, model.MetricNameLabel)

	// Collect value refs that need suffixing (exclude values of __name__ labels)
	numSymbols := len(req.Symbols)
	valueRefsToSuffix, valueRefCount := collectValueRefsToSuffix(req.Timeseries, nameSymbolRef, numSymbols)

	// Determine the last replica number needed
	lastReplica := fullCopies
	if fractionalPart > 0 {
		lastReplica++
	}
	numReplicas := lastReplica - 1 // Number of additional replicas (excluding original)

	// Build the amplified symbol table
	// Start with original symbols, then add suffixed versions for replicas 2, 3, etc.
	amplifiedSymbols := make([]string, numSymbols, numSymbols+valueRefCount*numReplicas)
	copy(amplifiedSymbols, req.Symbols)

	// 2D slice: suffixedValueRefs[replicaIdx][originalRef] = newRef
	// replicaIdx 0 = replica 2, replicaIdx 1 = replica 3, etc.
	suffixedValueRefs := make([][]uint32, numReplicas)
	for i := range suffixedValueRefs {
		suffixedValueRefs[i] = make([]uint32, numSymbols)
	}

	// Create suffixed symbols for each value ref that needs suffixing
	for valueRef := 0; valueRef < numSymbols; valueRef++ {
		if !valueRefsToSuffix[valueRef] {
			continue
		}
		originalValue := req.Symbols[valueRef]
		for replicaIdx := 0; replicaIdx < numReplicas; replicaIdx++ {
			replica := replicaIdx + 2 // replica 2, 3, ...
			newRef := uint32(len(amplifiedSymbols))
			amplifiedSymbols = append(amplifiedSymbols, originalValue+"_amp"+fmt.Sprint(replica))
			suffixedValueRefs[replicaIdx][valueRef] = newRef
		}
	}

	// Amplify time series
	amplifiedSeries := make([]mimirpb.TimeSeriesRW2, 0, len(req.Timeseries)*fullCopies)

	for i := range req.Timeseries {
		ts := &req.Timeseries[i]
		// Keep the original (replica 1, no suffixing)
		amplifiedSeries = append(amplifiedSeries, *ts)

		// Skip amplification for series with only __name__ label (nothing to suffix)
		if !hasLabelsToSuffix(ts.LabelsRefs, nameSymbolRef) {
			continue
		}

		// Create full copies with suffixed label values (replicas 2, 3, ...)
		for replicaIdx := 0; replicaIdx < fullCopies-1; replicaIdx++ {
			amplifiedSeries = append(amplifiedSeries, amplifyTimeSeriesRW2(ts, nameSymbolRef, suffixedValueRefs[replicaIdx]))
		}

		// Handle fractional part deterministically using hash
		// This ensures the same series always get the fractional copy
		if fractionalPart > 0 {
			hash := hashSeriesLabelsRW2(ts.LabelsRefs, req.Symbols)
			if shouldKeepSeries(hash, fractionalPart) {
				amplifiedSeries = append(amplifiedSeries, amplifyTimeSeriesRW2(ts, nameSymbolRef, suffixedValueRefs[numReplicas-1]))
			}
		}
	}

	return mimirpb.WriteRequestRW2{
		Symbols:    amplifiedSymbols,
		Timeseries: amplifiedSeries,
	}
}

// findSymbolRef finds the index of a symbol in the symbol table.
// Returns math.MaxUint32 if not found.
func findSymbolRef(symbols []string, target string) uint32 {
	for i, s := range symbols {
		if s == target {
			return uint32(i)
		}
	}
	return math.MaxUint32 // Not found
}

// hasLabelsToSuffix returns true if the RW2 series has any labels besides __name__.
// Series with only __name__ (or no labels) cannot be amplified.
func hasLabelsToSuffix(labelRefs []uint32, nameSymbolRef uint32) bool {
	// Iterate over label name/value pairs
	for i := 0; i+1 < len(labelRefs); i += 2 {
		labelNameRef := labelRefs[i]
		if labelNameRef != nameSymbolRef {
			return true // Found a label that is not __name__
		}
	}
	return false
}

// collectValueRefsToSuffix collects all value refs that need suffixing.
// It excludes values of __name__ labels (metric names should not be suffixed).
// Returns a bool slice where valueRefs[ref] = true if ref needs suffixing,
// and the count of unique value refs that need suffixing.
func collectValueRefsToSuffix(timeseries []mimirpb.TimeSeriesRW2, nameSymbolRef uint32, numSymbols int) ([]bool, int) {
	valueRefs := make([]bool, numSymbols)
	count := 0
	for _, ts := range timeseries {
		for i := 0; i+1 < len(ts.LabelsRefs); i += 2 {
			labelNameRef := ts.LabelsRefs[i]
			labelValueRef := ts.LabelsRefs[i+1]
			if labelNameRef != nameSymbolRef && int(labelValueRef) < numSymbols {
				if !valueRefs[labelValueRef] {
					count++
				}
				valueRefs[labelValueRef] = true
			}
		}
	}
	return valueRefs, count
}

// amplifyTimeSeriesRW2 creates a copy of an RW 2.0 time series with label values suffixed.
// This operates on uint32 label references, not actual label strings.
// For each label (except __name__), the value ref is replaced with its suffixed version.
// The suffixedValueRefs slice maps original ref -> new ref for this replica.
func amplifyTimeSeriesRW2(original *mimirpb.TimeSeriesRW2, nameSymbolRef uint32, suffixedValueRefs []uint32) mimirpb.TimeSeriesRW2 {
	// Create the time series with shared references for immutable data
	ts := mimirpb.TimeSeriesRW2{
		LabelsRefs:       make([]uint32, len(original.LabelsRefs)),
		Samples:          original.Samples,    // Share reference - samples are immutable
		Exemplars:        original.Exemplars,  // Share reference - exemplars are immutable
		Histograms:       original.Histograms, // Share reference - histograms are immutable
		Metadata:         original.Metadata,
		CreatedTimestamp: original.CreatedTimestamp,
	}

	// Copy and transform label refs.
	// No re-sorting needed - label names unchanged.
	for i := 0; i+1 < len(original.LabelsRefs); i += 2 {
		labelNameRef := original.LabelsRefs[i]
		labelValueRef := original.LabelsRefs[i+1]

		ts.LabelsRefs[i] = labelNameRef // Name unchanged

		if labelNameRef == nameSymbolRef {
			// Keep __name__ value unchanged
			ts.LabelsRefs[i+1] = labelValueRef
		} else if int(labelValueRef) < len(suffixedValueRefs) && suffixedValueRefs[labelValueRef] != 0 {
			// Use suffixed value ref
			ts.LabelsRefs[i+1] = suffixedValueRefs[labelValueRef]
		} else {
			// Keep original
			ts.LabelsRefs[i+1] = labelValueRef
		}
	}

	return ts
}

// hasOnlyNameLabel returns true if the series has only the __name__ label.
// Such series cannot be amplified (no label values to suffix).
func hasOnlyNameLabel(labels []mimirpb.LabelAdapter) bool {
	if len(labels) == 0 {
		return true // No labels at all, nothing to suffix
	}
	if len(labels) == 1 && labels[0].Name == model.MetricNameLabel {
		return true
	}
	return false
}

// amplifyTimeSeries creates a copy of a time series with all label values suffixed.
// The replicaNum is appended as _amp{N} to all label values except __name__.
// The original series is considered replica 1 (no suffix), so replicaNum should be >= 2.
// This increases cardinality across all label dimensions.
func amplifyTimeSeries(original *mimirpb.PreallocTimeseries, replicaNum int) mimirpb.PreallocTimeseries {
	suffix := fmt.Sprintf("_amp%d", replicaNum)

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

	// Copy labels, adding suffix to values (except __name__ value).
	// The original labels need to be copied as new strings to avoid unsafe mutations.
	// No re-sorting needed - label names unchanged.
	ts.Labels = make([]mimirpb.LabelAdapter, len(original.Labels))
	for i, label := range original.Labels {
		name := string(label.Name)
		value := string(label.Value)
		if name != model.MetricNameLabel {
			value = value + suffix
		}
		ts.Labels[i] = mimirpb.LabelAdapter{
			Name:  name,
			Value: value,
		}
	}

	return ts
}
