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

// AmplificationTracker tracks RW 1.0 vs RW 2.0 series counts over time
// and dynamically adjusts RW 1.0 amplification to compensate for unamplified RW 2.0.
type AmplificationTracker struct {
	mu                    sync.RWMutex
	totalRW1Series        int64
	totalRW2Series        int64
	targetAmplification   float64
	adjustedAmplification float64
	lastReset             time.Time
	resetInterval         time.Duration
}

// NewAmplificationTracker creates a new tracker with the target amplification factor.
// The tracker resets its counters every hour to keep the RW2 ratio fresh and adaptive.
func NewAmplificationTracker(targetAmplification float64) *AmplificationTracker {
	return &AmplificationTracker{
		targetAmplification:   targetAmplification,
		adjustedAmplification: targetAmplification,
		lastReset:             time.Now(),
		resetInterval:         time.Hour,
	}
}

// RecordRW1Series records RW 1.0 series count and recalculates adjustment.
func (t *AmplificationTracker) RecordRW1Series(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checkAndReset()
	t.totalRW1Series += int64(count)
	t.recalculateAdjustment()
}

// RecordRW2Series records RW 2.0 series count and recalculates adjustment.
func (t *AmplificationTracker) RecordRW2Series(count int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.checkAndReset()
	t.totalRW2Series += int64(count)
	t.recalculateAdjustment()
}

// checkAndReset resets counters if the reset interval has elapsed.
// Must be called with mu locked.
func (t *AmplificationTracker) checkAndReset() {
	if time.Since(t.lastReset) >= t.resetInterval {
		t.totalRW1Series = 0
		t.totalRW2Series = 0
		t.lastReset = time.Now()
		// Reset adjustment to target until we have new data
		t.adjustedAmplification = t.targetAmplification
	}
}

// GetAdjustedAmplification returns the current adjusted amplification factor for RW 1.0.
func (t *AmplificationTracker) GetAdjustedAmplification() float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.adjustedAmplification
}

// GetStats returns current statistics.
func (t *AmplificationTracker) GetStats() (rw1Series, rw2Series int64, rw2Ratio float64, adjustedFactor float64) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	total := t.totalRW1Series + t.totalRW2Series
	var ratio float64
	if total > 0 {
		ratio = float64(t.totalRW2Series) / float64(total)
	}

	return t.totalRW1Series, t.totalRW2Series, ratio, t.adjustedAmplification
}

// recalculateAdjustment adjusts the RW 1.0 amplification factor based on RW 2.0 ratio.
// Formula: adjusted = target / (1 - rw2_ratio)
// Example: If 30% are RW 2.0 and target is 2.0, then RW 1.0 gets 2.0/0.7 = 2.86x
func (t *AmplificationTracker) recalculateAdjustment() {
	total := t.totalRW1Series + t.totalRW2Series
	if total == 0 {
		t.adjustedAmplification = t.targetAmplification
		return
	}

	rw2Ratio := float64(t.totalRW2Series) / float64(total)

	// If all series are RW 2.0, we can't amplify RW 1.0 to compensate
	if rw2Ratio >= 1.0 {
		t.adjustedAmplification = t.targetAmplification
		return
	}

	// Adjust RW 1.0 amplification to compensate for unamplified RW 2.0
	t.adjustedAmplification = t.targetAmplification / (1.0 - rw2Ratio)
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
// RW 2.0 requests are NOT amplified to avoid memory-intensive symbol table conversion.
// If tracker is provided, series counts are recorded and RW 1.0 amplification is automatically
// adjusted to compensate for unamplified RW 2.0 series.
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

	// If RW 1.0 unmarshal fails with RW 2.0 error, skip amplification but count series
	if err != nil && strings.Contains(err.Error(), "Remote Write 2.0") {
		// Use PreallocWriteRequest with UnmarshalFromRW2=true to count series
		// This temporarily converts to RW 1.0 just for counting, then we discard it
		var prealloc mimirpb.PreallocWriteRequest
		prealloc.UnmarshalFromRW2 = true // Enable RW 2.0 parsing

		if err := prealloc.Unmarshal(decompressed); err == nil {
			// After unmarshaling with UnmarshalFromRW2=true, the data is in Timeseries (RW 1.0 format)
			seriesCount := len(prealloc.WriteRequest.Timeseries)

			// Record RW 2.0 series for dynamic adjustment
			if tracker != nil && seriesCount > 0 {
				tracker.RecordRW2Series(seriesCount)
			}

			// Return original body unmodified for RW 2.0 requests
			// The converted data in prealloc will be garbage collected
			return AmplificationResult{
				Body:                 body,
				OriginalSeriesCount:  seriesCount,
				AmplifiedSeriesCount: 0,
				IsRW2:                true,
				WasAmplified:         false,
			}, nil
		}

		// If PreallocWriteRequest also fails, return original without counting
		return AmplificationResult{
			Body:                 body,
			OriginalSeriesCount:  0,
			AmplifiedSeriesCount: 0,
			IsRW2:                true,
			WasAmplified:         false,
		}, nil
	} else if err != nil {
		return AmplificationResult{}, fmt.Errorf("failed to unmarshal write request: %w", err)
	}

	originalSeriesCount := len(req.Timeseries)

	// Record RW 1.0 series and get adjusted amplification factor
	var effectiveAmplification float64
	if tracker != nil {
		tracker.RecordRW1Series(originalSeriesCount)
		effectiveAmplification = tracker.GetAdjustedAmplification()
	} else {
		effectiveAmplification = amplificationFactor
	}

	// Recalculate copies based on effective amplification
	effectiveFullCopies := int(math.Floor(effectiveAmplification))
	effectiveFractionalPart := effectiveAmplification - float64(effectiveFullCopies)

	// RW 1.0: amplify using Timeseries
	amplifiedSeries := make([]mimirpb.PreallocTimeseries, 0, len(req.Timeseries)*effectiveFullCopies)

	for _, ts := range req.Timeseries {
		// Keep the original
		amplifiedSeries = append(amplifiedSeries, ts)

		// Create full copies
		for i := 1; i < effectiveFullCopies; i++ {
			amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, i))
		}

		// Handle fractional part with probability
		if effectiveFractionalPart > 0 && rand.Float64() < effectiveFractionalPart {
			amplifiedSeries = append(amplifiedSeries, amplifyTimeSeries(&ts, effectiveFullCopies))
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
