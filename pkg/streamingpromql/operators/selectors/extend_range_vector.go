// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type undoAction int
type extendedPointsMode int

const (
	none undoAction = iota
	removed
	replaced
)

const (
	anchored extendedPointsMode = iota
	smoothed
	smoothedCounter
)

// RevertibleExtendedPointsState calculates anchored and smoothed boundary points.
//
// The underlying buffer is mutated to align points to a given time range boundary.
//
// These mutations may result in the buffer having points trimmed from the start or end, and/or
// synthetic points being inserted on the boundaries.
//
// The utility maintains a record of mutations so they can be undone - there by restoring
// the underlying buffer to its original state. See UndoChanges()
//
// The utility supports three modes of operation;
//	* anchored - an existing point value is allocated to the start and end boundary points
//	* smoothed - an interpolated value is allocated to the start and end boundary points
//	* smoothedCounter - an interpolated value which compensates for a counter reset is allocated to the start and end boundary points

type RevertibleExtendedPointsState struct {
	// The buffer where points will be mutated.
	buff *types.FPointRingBuffer

	// The operation mode for how points will be calculated at the boundaries
	mode extendedPointsMode

	// The original first point in the set of points considered for the extension. This may not be the first point in the overall buffer.
	first promql.FPoint
	// The original last point in the set of points considered for the extension. This may not be the last point in the overall buffer.
	last promql.FPoint

	// If the buffer has a trailing point which is outside consideration for the extension it is stored here so it can be restored.
	excludedLast promql.FPoint
	// Flag indicating that the point in excludedLast needs to be re-inserted to the end of the buffer.
	restoreExcludedLast bool

	// If the buffer has a leading point which is outside consideration for the extension it is stored here so it can be restored.
	// This can occur when there is a single point in the extended look-back. The point can not be used since there is no point within the original time range.
	// But we can not delete it from the buffer since the point may be used in subsequent step iterations.
	excludedFirst        promql.FPoint
	restoreExcludedFirst bool

	// The actions required to undo synthetic head and tail modifications.
	undoTailModifications undoAction
	undoHeadModifications undoAction
}

func NewRevertibleExtendedPointsState(buff *types.FPointRingBuffer, mode extendedPointsMode) *RevertibleExtendedPointsState {
	return &RevertibleExtendedPointsState{
		buff: buff,
		mode: mode,
	}
}

// ApplyBoundaryMutations trims the provided buffer to the requested time range and
// applies anchored or smoothed values at the range boundaries.
//
// The boundary values depend on the selected operation mode and the points present
// in the underlying buffer:
//
// Anchored mode:
//   - Boundary values are taken directly from existing points.
//   - The point at T=rangeStart is set to the value of the last point with T <= rangeStart within the lookback window.
//     If none exists, the value of the first point with T > rangeStart is used.
//   - The point at T=rangeEnd follows the same pattern: preference is given to the first
//     point with T >= rangeEnd within the lookahead window; if none exists, the value of the last point with
//     T < rangeEnd is used.
//
// Smoothed mode:
//   - Boundary values are computed using interpolation rather than existing point values.
//   - Interpolation is only applied if points exist on both sides of the boundary.
//     For example, the rangeStart value is interpolated only if there is a point
//     with T < rangeStart and another with T > rangeStart.
//   - If no point exists on the opposite side of a boundary, the anchored-mode
//     logic is used instead.
//
// SmoothedCounter mode:
//   - Identical to smoothed mode, except that interpolation compensates for any
//     counter resets between the points used for interpolation.
//
// This function mutates the underlying buffer so that it contains only points within
// the [rangeStart, rangeEnd] interval and ensures that points exist at both boundaries.
//
// All mutations are recorded so they can be undone, allowing the buffer to be reused
// in the next step iteration.
//
// Note: mutation tracking assumes that the next step iteration will have a rangeStart
// greater than the current rangeStart.
//
// This implementation is based on extendFloats() from promql/engine.go.
func (m *RevertibleExtendedPointsState) ApplyBoundaryMutations(rangeStart, rangeEnd, extendedRangeEnd int64) error {

	if m.buff.Count() == 0 {
		return nil
	}

	// Left-trim the float buffer so that at most one point with timestamp <= rangeStart remains.
	// We record the modification so it can be undone and the point restored into the buffer for the next step iteration.
	// Note: if multiple points exist before rangeStart, we only need to retain the most recent
	// one, since the next step iteration will have a rangeStart greater than this one.
	for m.buff.Count() > 1 && m.buff.PointAt(1).T <= rangeStart {
		m.excludedFirst = m.buff.PointAt(0)
		m.restoreExcludedFirst = true
		m.buff.RemoveFirst()
	}

	m.first = m.buff.PointAt(0)
	m.last = m.buff.Last()

	// Discard any trailing point which is outside the extended range end.
	// Note that we only expect at most 1 point to be found here.
	if m.last.T > extendedRangeEnd {
		m.excludedLast = m.last
		m.restoreExcludedLast = true
		m.buff.RemoveLast()
		if m.buff.Count() == 0 {
			return nil
		}
		m.last = m.buff.Last()
	}

	// A single point can be within the extended look-back, but we discard it because
	// we need at least one point within the original range to proceed.
	// Note that this may replace a point already stored in excludedFirst.
	// This is ok, as we only require one point before the rangeStart and the
	// next step will have a rangeStart greater than this rangeStart.
	if m.buff.Count() == 1 && m.first.T <= rangeStart {
		m.excludedFirst = m.first
		m.restoreExcludedFirst = true
		m.buff.RemoveFirst()
		return nil
	}

	// Note re common subexpression elimination and range vector selector re-use.
	// In a hypothetical rate(metric[5m] smoothed) / delta(metric[5m] smoothed) we can not re-use the same range vector selector
	// for both left and right hand sides. This is because the LHS will have smoothed values with counter adjusted values
	// and the RHS will have smoothed values without counter adjusted values.
	// However, the MatrixSelector node tracks the outer function name, and this field is included in the nodes equivalent comparator function
	// which is used by the common sub expression elimination optimisation pass.

	// Add synthetic or clamp start boundary
	if m.first.T > rangeStart {
		if err := m.buff.AppendAtStart(promql.FPoint{T: rangeStart, F: m.first.F}); err != nil {
			return err
		}
		m.undoHeadModifications = removed

	} else if m.first.T < rangeStart {
		boundaryValue := m.first.F
		if m.mode != anchored {
			pointAfterRangeStart := m.buff.PointAt(1)
			boundaryValue = interpolate(m.first, pointAfterRangeStart, rangeStart, true, m.mode == smoothedCounter)
		}
		if err := m.buff.ReplaceFirst(promql.FPoint{T: rangeStart, F: boundaryValue}); err != nil {
			return err
		}
		m.undoHeadModifications = replaced
	}

	// Add synthetic or clamp end boundary
	if m.last.T < rangeEnd {
		if err := m.buff.Append(promql.FPoint{T: rangeEnd, F: m.last.F}); err != nil {
			return err
		}
		m.undoTailModifications = removed

	} else if m.last.T > rangeEnd {
		boundaryValue := m.last.F
		if m.mode != anchored {
			pointBeforeRangeEnd := m.buff.PointAt(m.buff.Count() - 2)
			boundaryValue = interpolate(pointBeforeRangeEnd, m.last, rangeEnd, false, m.mode == smoothedCounter)
		}
		if err := m.buff.ReplaceLast(promql.FPoint{T: rangeEnd, F: boundaryValue}); err != nil {
			return err
		}
		m.undoTailModifications = replaced
	}

	return nil
}

// UndoChanges will restore the buffer to its original points.
func (m *RevertibleExtendedPointsState) UndoChanges() error {

	switch m.undoTailModifications {
	case none:
		// nothing to be done
	case removed:
		m.buff.RemoveLast()
	case replaced:
		if err := m.buff.ReplaceLast(m.last); err != nil {
			return err
		}
	}

	switch m.undoHeadModifications {
	case none:
		// nothing to be done
	case removed:
		m.buff.RemoveFirst()
	case replaced:
		if err := m.buff.ReplaceFirst(m.first); err != nil {
			return err
		}
	}

	if m.restoreExcludedLast {
		if err := m.buff.Append(m.excludedLast); err != nil {
			return err
		}
	}

	if m.restoreExcludedFirst {
		if err := m.buff.AppendAtStart(m.excludedFirst); err != nil {
			return err
		}
	}

	// reset the modification tracking so calling this function is idempotent
	m.Reset()

	return nil
}

func (m *RevertibleExtendedPointsState) Reset() {
	m.undoHeadModifications = none
	m.undoTailModifications = none
	m.restoreExcludedLast = false
	m.restoreExcludedFirst = false
}

func interpolate(p1, p2 promql.FPoint, t int64, leftEdge bool, isCounter bool) float64 {
	// This has been adapted from interpolate() in promql/functions.go
	y1 := p1.F
	y2 := p2.F

	if isCounter && y2 < y1 {
		if leftEdge {
			y1 = 0
		} else {
			y2 += y1
		}
		return y1 + (y2-y1)*float64(t-p1.T)/float64(p2.T-p1.T)
	}

	return y1 + (y2-y1)*float64(t-p1.T)/float64(p2.T-p1.T)
}
