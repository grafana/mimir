// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
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

// SamplingResult contains the result of sampling along with metadata.
type SamplingResult struct {
	Body                []byte
	OriginalSeriesCount int
	SampledSeriesCount  int
}

// SampleWriteRequest samples series from a write request based on the sampling factor.
// The sampling factor determines how to filter the time series:
//   - factor = 1.0: no change (returns original)
//   - factor < 1.0: sampling (reduction)
//   - factor = 0.1: approximately 10% of series are kept (deterministic selection)
//   - factor = 0.5: approximately 50% of series are kept (deterministic selection)
//
// Selection is deterministic and based on a hash of the series labels,
// ensuring the same series are consistently selected across requests.
//
// The body is expected to be Snappy-compressed Prometheus remote write protobuf.
// Both RW 1.0 and RW 2.0 requests are processed using the same sampling factor.
// RW 2.0 requests are kept in native RW 2.0 format (avoiding memory expansion).
// If tracker is provided, series counts are recorded for observability.
//
// For amplification (factor > 1.0), use AmplifyRequestBody to send multiple separate requests.
func SampleWriteRequest(body []byte, amplificationFactor float64, tracker *AmplificationTracker) (SamplingResult, error) {
	if amplificationFactor == 1.0 {
		return SamplingResult{
			Body:                body,
			OriginalSeriesCount: 0, // Unknown without unmarshaling
			SampledSeriesCount:  0,
		}, nil
	}

	// Only handle sampling (factor < 1.0). For amplification (factor > 1.0),
	// use AmplifyRequestBody to send multiple separate requests.
	if amplificationFactor > 1.0 {
		return SamplingResult{}, fmt.Errorf("SampleWriteRequest only handles sampling (factor <= 1.0), got %.2f", amplificationFactor)
	}

	// Decompress the snappy-compressed body
	decompressed, err := snappy.Decode(nil, body)
	if err != nil {
		return SamplingResult{}, fmt.Errorf("failed to decompress write request: %w", err)
	}

	// Try unmarshaling as RW 1.0 first
	var req mimirpb.WriteRequest
	err = proto.Unmarshal(decompressed, &req)

	// If RW 1.0 unmarshal fails with RW 2.0 error, amplify RW 2.0 in native format
	if err != nil {
		// Check if this is a RW 2.0 request by looking for the specific error message
		if !isRW2Error(err) {
			return SamplingResult{}, fmt.Errorf("failed to unmarshal write request: %w", err)
		}

		// Unmarshal as RW 2.0 data in native format using our custom unmarshal
		// This keeps data in native RW 2.0 format (symbol table + uint32 refs)
		// avoiding the 12.5x memory expansion of converting to RW 1.0
		rw2Req, err := mimirpb.UnmarshalWriteRequestRW2Native(decompressed)
		if err != nil {
			return SamplingResult{}, fmt.Errorf("failed to unmarshal RW 2.0 write request: %w", err)
		}

		originalSeriesCount := len(rw2Req.Timeseries)

		// Record RW 2.0 series for observability
		if tracker != nil && originalSeriesCount > 0 {
			tracker.RecordRW2Series(originalSeriesCount)
		}

		// Sample series deterministically based on hash
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
			return SamplingResult{}, fmt.Errorf("failed to marshal sampled RW 2.0 write request: %w", err)
		}

		// Compress back with snappy
		sampledBody := snappy.Encode(nil, sampledProto)

		return SamplingResult{
			Body:                sampledBody,
			OriginalSeriesCount: originalSeriesCount,
			SampledSeriesCount:  len(sampledSeries),
		}, nil
	}

	// RW 1.0 path - sample series deterministically
	originalSeriesCount := len(req.Timeseries)

	// Record RW 1.0 series for observability
	if tracker != nil {
		tracker.RecordRW1Series(originalSeriesCount)
	}

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
		return SamplingResult{}, fmt.Errorf("failed to marshal sampled write request: %w", err)
	}

	// Compress back with snappy
	sampledBody := snappy.Encode(nil, sampledProto)

	return SamplingResult{
		Body:                sampledBody,
		OriginalSeriesCount: originalSeriesCount,
		SampledSeriesCount:  len(sampledSeries),
	}, nil
}

// AmplifyRequestBody creates multiple amplified replicas from a single request body.
// It decompresses and unmarshals once, then creates replicas startReplica through endReplica (inclusive).
// Each replica has all label values (except __name__) suffixed with _ampN for uniqueness.
func AmplifyRequestBody(body []byte, startReplica, endReplica int) ([][]byte, error) {
	if startReplica < 1 || endReplica < startReplica {
		return nil, fmt.Errorf("invalid replica range: %d to %d", startReplica, endReplica)
	}

	numReplicas := endReplica - startReplica + 1
	bodies := make([][]byte, 0, numReplicas)

	// Decompress once
	decompressed, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress write request: %w", err)
	}

	// Try unmarshaling as RW 1.0 first
	var req mimirpb.WriteRequest
	err = proto.Unmarshal(decompressed, &req)

	// Check if this is RW 2.0
	if err != nil && isRW2Error(err) {
		// RW 2.0 path: unmarshal once, create all replicas
		rw2Req, err := mimirpb.UnmarshalWriteRequestRW2Native(decompressed)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal RW 2.0 write request: %w", err)
		}

		// Create each replica
		for replicaNum := startReplica; replicaNum <= endReplica; replicaNum++ {
			var suffixedRW2 mimirpb.WriteRequestRW2
			if replicaNum == 1 {
				// Replica 1 is the original - no suffix needed, just marshal as-is
				suffixedRW2 = mimirpb.WriteRequestRW2{
					Symbols:    rw2Req.Symbols,
					Timeseries: rw2Req.Timeseries,
				}
			} else {
				suffixedRW2 = applySuffixToRW2Request(rw2Req, replicaNum)
			}

			marshaled, err := proto.Marshal(&suffixedRW2)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal RW 2.0 replica %d: %w", replicaNum, err)
			}
			compressed := snappy.Encode(nil, marshaled)
			bodies = append(bodies, compressed)
		}

		return bodies, nil
	} else if err != nil {
		// Unmarshal error that's not RW2
		return nil, fmt.Errorf("failed to unmarshal write request: %w", err)
	}

	// RW 1.0 path: unmarshal once, create all replicas
	for replicaNum := startReplica; replicaNum <= endReplica; replicaNum++ {
		var suffixedReq mimirpb.WriteRequest
		if replicaNum == 1 {
			// Replica 1 is the original - preserve all fields
			suffixedReq = req
		} else {
			// Create a new request with suffixed series, preserving all other fields
			suffixedReq = mimirpb.WriteRequest{
				Source:                   req.Source,
				Metadata:                 req.Metadata,
				SkipLabelValidation:      req.SkipLabelValidation,
				SkipLabelCountValidation: req.SkipLabelCountValidation,
				Timeseries:               make([]mimirpb.PreallocTimeseries, len(req.Timeseries)),
			}
			for i := range req.Timeseries {
				suffixedReq.Timeseries[i] = applySuffixToTimeSeries(&req.Timeseries[i], replicaNum)
			}
		}

		marshaled, err := proto.Marshal(&suffixedReq)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal RW 1.0 replica %d: %w", replicaNum, err)
		}
		compressed := snappy.Encode(nil, marshaled)
		bodies = append(bodies, compressed)
	}

	return bodies, nil
}

// applySuffixToRW2Request applies the _amp{N} suffix to all label values (except __name__)
// for all series in an RW 2.0 request.
func applySuffixToRW2Request(req *mimirpb.WriteRequestRW2, replicaNum int) mimirpb.WriteRequestRW2 {
	// Find __name__ symbol ref (to identify which values to exclude from suffixing)
	nameSymbolRef := findSymbolRef(req.Symbols, model.MetricNameLabel)

	// Collect value refs that need suffixing (exclude values of __name__ labels)
	valueRefsToSuffix, count := collectValueRefsToSuffix(req.Timeseries, nameSymbolRef)

	// Build the suffixed symbol table with pre-allocated capacity
	suffixedSymbols := make([]string, len(req.Symbols), len(req.Symbols)+count)
	copy(suffixedSymbols, req.Symbols)

	// Slice: suffixedValueRefs[originalRef] = newRef (0 means not mapped)
	suffixedValueRefs := make([]uint32, len(valueRefsToSuffix))

	// Create suffixed symbols for each value ref that needs suffixing
	suffix := "_amp" + strconv.Itoa(replicaNum)
	for valueRef, needsSuffix := range valueRefsToSuffix {
		if !needsSuffix {
			continue
		}
		// Bounds check: skip value refs that exceed symbol table bounds
		if valueRef >= len(req.Symbols) {
			continue
		}
		originalValue := req.Symbols[valueRef]
		newRef := uint32(len(suffixedSymbols))
		suffixedSymbols = append(suffixedSymbols, originalValue+suffix)
		suffixedValueRefs[valueRef] = newRef
	}

	// Apply suffixes to all time series
	suffixedSeries := make([]mimirpb.TimeSeriesRW2, len(req.Timeseries))
	for i, ts := range req.Timeseries {
		suffixedSeries[i] = applySuffixToTimeSeriesRW2(&ts, nameSymbolRef, suffixedValueRefs)
	}

	return mimirpb.WriteRequestRW2{
		Symbols:    suffixedSymbols,
		Timeseries: suffixedSeries,
	}
}

// applySuffixToTimeSeriesRW2 creates a copy of an RW 2.0 time series with label values suffixed.
// This operates on uint32 label references, not actual label strings.
// For each label (except __name__), the value ref is replaced with its suffixed version.
func applySuffixToTimeSeriesRW2(original *mimirpb.TimeSeriesRW2, nameSymbolRef uint32, suffixedValueRefs []uint32) mimirpb.TimeSeriesRW2 {
	// Copy the time series, sharing immutable slice references
	ts := mimirpb.TimeSeriesRW2{
		// Same number of label refs - we're replacing values, not adding labels
		LabelsRefs: make([]uint32, len(original.LabelsRefs)),
		// Share samples slice (immutable)
		Samples: original.Samples,
		// Share exemplars slice (immutable)
		Exemplars: original.Exemplars,
		// Share histograms slice (immutable)
		Histograms: original.Histograms,
		// Copy metadata
		Metadata:         original.Metadata,
		CreatedTimestamp: original.CreatedTimestamp,
	}

	// Copy and transform label refs.
	// No re-sorting needed - label names unchanged.
	// Bounds check: ensure we have complete name/value pairs (i+1 must be valid)
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
			// Keep original (shouldn't happen if we collected all value refs)
			ts.LabelsRefs[i+1] = labelValueRef
		}
	}

	return ts
}

// applySuffixToTimeSeries creates a copy of a time series with all label values suffixed.
// The replicaNum is appended as _amp{N} to all label values except __name__.
// The original series is considered replica 1 (no suffix), so replicaNum should be >= 2.
// This increases cardinality across all label dimensions.
func applySuffixToTimeSeries(original *mimirpb.PreallocTimeseries, replicaNum int) mimirpb.PreallocTimeseries {
	suffix := "_amp" + strconv.Itoa(replicaNum)

	// Create a copy, sharing immutable slice references
	ts := mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			// Share samples slice (immutable)
			Samples: original.Samples,
			// Share exemplars slice (immutable)
			Exemplars: original.Exemplars,
			// Share histograms slice (immutable)
			Histograms: original.Histograms,
		},
	}

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

// collectValueRefsToSuffix collects all value refs that need suffixing.
// It excludes values of __name__ labels (metric names should not be suffixed).
// Returns a bool slice indexed by ref (true = needs suffix) and the count of refs to suffix.
func collectValueRefsToSuffix(timeseries []mimirpb.TimeSeriesRW2, nameSymbolRef uint32) ([]bool, int) {
	// Find max ref to size the slice
	maxRef := uint32(0)
	for i := range timeseries {
		ts := &timeseries[i]
		for j := 0; j+1 < len(ts.LabelsRefs); j += 2 {
			if ts.LabelsRefs[j] != nameSymbolRef && ts.LabelsRefs[j+1] > maxRef {
				maxRef = ts.LabelsRefs[j+1]
			}
		}
	}

	valueRefs := make([]bool, maxRef+1)
	count := 0
	for i := range timeseries {
		ts := &timeseries[i]
		// Bounds check: ensure we have complete name/value pairs (i+1 must be valid)
		for j := 0; j+1 < len(ts.LabelsRefs); j += 2 {
			labelNameRef := ts.LabelsRefs[j]
			labelValueRef := ts.LabelsRefs[j+1]
			if labelNameRef != nameSymbolRef && !valueRefs[labelValueRef] {
				valueRefs[labelValueRef] = true
				count++
			}
		}
	}
	return valueRefs, count
}
