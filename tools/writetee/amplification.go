// SPDX-License-Identifier: AGPL-3.0-only

package writetee

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	amplifiedLabelName  = "__amplified__"
	amplifiedLabelValue = "true"
)

// isRW2Error checks if an error is caused by trying to unmarshal RW 2.0 data
func isRW2Error(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "Remote Write 2.0") || strings.Contains(errStr, "Symbols")
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
	Body              []byte
	OriginalSeriesCount int
	AmplifiedSeriesCount int
	IsRW2             bool
	WasAmplified      bool
}

// AmplifyWriteRequest takes a Prometheus remote write request body and amplifies it
// by duplicating time series based on the amplification factor.
// The amplification factor determines how many copies of each time series to create:
//   - factor = 1.0: no amplification (returns original)
//   - factor = 2.0: each time series is duplicated once (2x total)
//   - factor = 3.5: each time series gets 3 full copies + 50% chance of a 4th copy
//
// Each amplified copy gets an additional label: __amplified__="<replica_num>"
// The body is expected to be Snappy-compressed Prometheus remote write protobuf.
//
// Both RW 1.0 and RW 2.0 requests are amplified using the same amplification factor.
// RW 2.0 requests are converted to RW 1.0 format before amplification (memory intensive).
// If tracker is provided, series counts are recorded for observability.
// Returns AmplificationResult with metadata about what was done.
func AmplifyWriteRequest(body []byte, amplificationFactor float64, tracker *AmplificationTracker) (AmplificationResult, error) {
	if amplificationFactor <= 1.0 {
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

	// If RW 1.0 unmarshal fails with RW 2.0 error, convert and amplify RW 2.0
	if err != nil {
		// Check if this is a RW 2.0 request by looking for the specific error message
		if !isRW2Error(err) {
			return AmplificationResult{}, fmt.Errorf("failed to unmarshal write request: %w", err)
		}
		// Use PreallocWriteRequest with UnmarshalFromRW2=true to convert RW 2.0 to RW 1.0
		var prealloc mimirpb.PreallocWriteRequest
		prealloc.UnmarshalFromRW2 = true // Enable RW 2.0 parsing

		if err := prealloc.Unmarshal(decompressed); err != nil {
			return AmplificationResult{}, fmt.Errorf("failed to unmarshal RW 2.0 write request: %w", err)
		}

		// After unmarshaling with UnmarshalFromRW2=true, the data is in Timeseries (RW 1.0 format)
		originalSeriesCount := len(prealloc.WriteRequest.Timeseries)

		// Record RW 2.0 series for dynamic adjustment
		if tracker != nil && originalSeriesCount > 0 {
			tracker.RecordRW2Series(originalSeriesCount)
		}

		// Amplify the converted RW 2.0 data (now in RW 1.0 format)
		fullCopies := int(math.Floor(amplificationFactor))
		fractionalPart := amplificationFactor - float64(fullCopies)

		amplifiedSeries := make([]mimirpb.PreallocTimeseries, 0, len(prealloc.WriteRequest.Timeseries)*fullCopies)

		for _, ts := range prealloc.WriteRequest.Timeseries {
			// Keep the original
			amplifiedSeries = append(amplifiedSeries, ts)

			// Create full copies
			for i := 1; i < fullCopies; i++ {
				amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, i))
			}

			// Handle fractional part with probability
			if fractionalPart > 0 && rand.Float64() < fractionalPart {
				amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, fullCopies))
			}
		}

		// Replace the time series in the request
		prealloc.WriteRequest.Timeseries = amplifiedSeries

		// Marshal back to protobuf
		amplifiedProto, err := proto.Marshal(&prealloc.WriteRequest)
		if err != nil {
			return AmplificationResult{}, fmt.Errorf("failed to marshal amplified RW 2.0 write request: %w", err)
		}

		// Compress back with snappy
		amplifiedBody := snappy.Encode(nil, amplifiedProto)

		return AmplificationResult{
			Body:                 amplifiedBody,
			OriginalSeriesCount:  originalSeriesCount,
			AmplifiedSeriesCount: len(amplifiedSeries) - originalSeriesCount,
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

	// Calculate copies based on amplification factor
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

		// Handle fractional part with probability
		if fractionalPart > 0 && rand.Float64() < fractionalPart {
			amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, fullCopies))
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

// amplifyRequestBody is a helper that wraps AmplifyWriteRequest for use with io.ReadCloser
func amplifyRequestBody(body io.ReadCloser, amplificationFactor float64, tracker *AmplificationTracker) (io.ReadCloser, AmplificationResult, error) {
	if amplificationFactor <= 1.0 {
		return body, AmplificationResult{WasAmplified: false}, nil
	}

	// Read the entire body
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, AmplificationResult{}, fmt.Errorf("failed to read request body for amplification: %w", err)
	}

	// Close the original body
	if err := body.Close(); err != nil {
		return nil, AmplificationResult{}, fmt.Errorf("failed to close original body: %w", err)
	}

	// Amplify
	result, err := AmplifyWriteRequest(bodyBytes, amplificationFactor, tracker)
	if err != nil {
		return nil, AmplificationResult{}, err
	}

	// Return as a new ReadCloser
	return io.NopCloser(bytes.NewReader(result.Body)), result, nil
}
