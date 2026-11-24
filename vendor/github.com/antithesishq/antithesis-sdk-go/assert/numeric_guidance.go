//go:build enable_antithesis_sdk

package assert

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/antithesishq/antithesis-sdk-go/internal"
)

func operandKind[T Number]() reflect.Kind {
	var zero T
	return reflect.TypeOf(zero).Kind()
}

// --------------------------------------------------------------------------------
// numericGuidanceTracker - Tracking Info for Numeric Guidance
//
// For GuidanceFnMaximize:
//   - gap is the largest value sent so far
//
// For GuidanceFnMinimize:
//   - gap is the most negative value sent so far
//
// --------------------------------------------------------------------------------
type numericGuidanceInfo struct {
	maximize bool
	// The best gap sent so far, as a float64. Read lock-free on every
	// evaluation; updated under mu. Un-stored until the first hit.
	gap atomic.Value
	// Serializes improvements (compare, update, emit) so guidance emissions
	// form an ordered monotone chain; the no-improvement fast path never
	// takes it.
	mu sync.Mutex
}

var numericGuidanceTracker sync.Map // message key -> *numericGuidanceInfo

func getNumericGuidanceEntry(messageKey string, maximize bool) *numericGuidanceInfo {
	if entry, ok := numericGuidanceTracker.Load(messageKey); ok {
		return entry.(*numericGuidanceInfo)
	}
	entry, _ := numericGuidanceTracker.LoadOrStore(messageKey,
		newNumericGuidanceInfo(maximize))
	return entry.(*numericGuidanceInfo)
}

// Create a numeric guidance entry
func newNumericGuidanceInfo(maximize bool) *numericGuidanceInfo {
	return &numericGuidanceInfo{maximize: maximize}
}

func improvesGap(info *numericGuidanceInfo, candidate float64) bool {
	stored := info.gap.Load()
	if stored == nil {
		// Nothing seen yet for this id, so anything improves
		return true
	}
	previous := stored.(float64)
	if info.maximize {
		return candidate > previous
	}
	return candidate < previous
}

// The gap between the operands and whether it improves the watermark. The
// subtraction is the only arithmetic this SDK performs on operands; the
// platform cannot observe differences lost in the conversion to float64.
//
// A non-finite operand skips the hit without touching the watermark and warns
// once per process, whether or not the gap would have improved. An infinite
// gap between finite operands stays eligible. Both the early probe and the
// send check here, so a call warns at most once.
func numericGuidanceImproves[T Number](info *numericGuidanceInfo, left, right T, id string) (float64, bool) {
	l, r := float64(left), float64(right)
	// x-x is zero for every finite x and NaN for NaN and both infinities.
	if l-l != 0 || r-r != 0 {
		internal.WarnNonFiniteOperand(id)
		return 0, false
	}
	gap := l - r
	return gap, improvesGap(info, gap)
}

func sendNumericGuidance[T Number](info *numericGuidanceInfo,
	left, right T, message, id string, loc *locationInfo, guidanceFn guidanceFnType) {
	candidate, improves := numericGuidanceImproves(info, left, right, id)
	if !improves {
		return
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	if !improvesGap(info, candidate) {
		return
	}
	info.gap.Store(candidate)
	emitGuidance(build_numeric_guidance(guidanceFn, message, left, right, loc, id, true))
}
