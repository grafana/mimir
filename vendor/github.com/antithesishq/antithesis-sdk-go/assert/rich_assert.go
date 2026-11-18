//go:build enable_antithesis_sdk

package assert

import (
	"reflect"

	"github.com/antithesishq/antithesis-sdk-go/internal"
)

// A type for writing raw assertions.
// guidanceFnType allows the assertion to provide guidance to
// the Antithesis platform when testing in Antithesis.
// Regular users of the assert package should not use it.
type guidanceFnType int

const (
	guidanceFnMaximize guidanceFnType = iota // Maximize (left - right) values
	guidanceFnMinimize                       // Minimize (left - right) values
	guidanceFnWantAll                        // Encourages fuzzing explorations where boolean values are true
	guidanceFnWantNone                       // Encourages fuzzing explorations where boolean values are false
	guidanceFnExplore
)

// guidanceFnExplore

func get_guidance_type_string(gt guidanceFnType) string {
	switch gt {
	case guidanceFnMaximize, guidanceFnMinimize:
		return "numeric"
	case guidanceFnWantAll, guidanceFnWantNone:
		return "boolean"
	case guidanceFnExplore:
		return "json"
	}
	return ""
}

type numericOperands[T operandConstraint] struct {
	Left  T `json:"left"`
	Right T `json:"right"`
}

type guidanceInfo struct {
	Data         any           `json:"guidance_data,omitempty"`
	Location     *locationInfo `json:"location"`
	GuidanceType string        `json:"guidance_type"`
	Message      string        `json:"message"`
	Id           string        `json:"id"`
	Maximize     bool          `json:"maximize"`
	Hit          bool          `json:"hit"`
}

func uses_maximize(gt guidanceFnType) bool {
	return gt == guidanceFnMaximize || gt == guidanceFnWantAll
}

func newOperands[T Number](left, right T) any {
	switch operandKind[T]() {
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return numericOperands[int32]{int32(left), int32(right)}
	case reflect.Int, reflect.Int64:
		return numericOperands[int64]{int64(left), int64(right)}
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint, reflect.Uint64, reflect.Uintptr:
		return numericOperands[uint64]{uint64(left), uint64(right)}
	case reflect.Float32, reflect.Float64:
		return numericOperands[float64]{float64(left), float64(right)}
	}
	return nil
}

func build_numeric_guidance[T Number](gt guidanceFnType, message string, left, right T, loc *locationInfo, id string, hit bool) *guidanceInfo {

	var operands any
	if hit {
		operands = newOperands(left, right)
	}

	gI := guidanceInfo{
		GuidanceType: get_guidance_type_string(gt),
		Message:      message,
		Id:           id,
		Location:     loc,
		Maximize:     uses_maximize(gt),
		Data:         operands,
		Hit:          hit,
	}

	return &gI
}

type namedBoolDictionary map[string]bool

func build_boolean_guidance(gt guidanceFnType, message string, named_bools []NamedBool,
	loc *locationInfo,
	id string, hit bool) *guidanceInfo {

	var guidance_data any

	// To ensure the sequence and naming for the named_bool values
	if hit {
		named_bool_dictionary := namedBoolDictionary{}
		for _, named_bool := range named_bools {
			named_bool_dictionary[named_bool.First] = named_bool.Second
		}
		guidance_data = named_bool_dictionary
	}

	bgI := guidanceInfo{
		GuidanceType: get_guidance_type_string(gt),
		Message:      message,
		Id:           id,
		Location:     loc,
		Maximize:     uses_maximize(gt),
		Data:         guidance_data,
		Hit:          hit,
	}

	return &bgI
}

func behavior_to_guidance(behavior string) guidanceFnType {
	guidance := guidanceFnExplore
	switch behavior {
	case "maximize":
		guidance = guidanceFnMaximize
	case "minimize":
		guidance = guidanceFnMinimize
	case "all":
		guidance = guidanceFnWantAll
	case "none":
		guidance = guidanceFnWantNone
	}
	return guidance
}

func numericGuidanceImpl[T Number](left, right T, message, id string, loc *locationInfo, guidanceFn guidanceFnType, hit bool) {
	tI := getNumericGuidanceEntry(id, uses_maximize(guidanceFn))
	if !hit {
		emitGuidance(build_numeric_guidance(guidanceFn, message, left, right, loc, id, false))
		return
	}
	sendNumericGuidance(tI, left, right, message, id, loc, guidanceFn)
}

func booleanGuidanceImpl(named_bools []NamedBool, message, id string, loc *locationInfo, guidanceFn guidanceFnType, hit bool) {
	tI := getBooleanGuidanceEntry(id)
	bgI := build_boolean_guidance(guidanceFn, message, named_bools, loc, id, hit)
	tI.send_value(bgI)
}

// NumericGuidanceRaw is a low-level method designed to be used by third-party frameworks. Regular users of the assert package should not call it.
func NumericGuidanceRaw[T Number](
	left, right T,
	message, id string,
	classname, funcname, filename string,
	line int, column int,
	behavior string,
	hit bool,
) {
	if hit {
		if entry, ok := numericGuidanceTracker.Load(id); ok {
			if _, improves := numericGuidanceImproves(entry.(*numericGuidanceInfo), left, right, id); !improves {
				return
			}
		}
	}
	loc := &locationInfo{classname, funcname, filename, line, column}
	guidanceFn := behavior_to_guidance(behavior)
	numericGuidanceImpl(left, right, message, id, loc, guidanceFn, hit)
}

// BooleanGuidanceRaw is a low-level method designed to be used by third-party frameworks. Regular users of the assert package should not call it.
func BooleanGuidanceRaw(
	named_bools []NamedBool,
	message, id string,
	classname, funcname, filename string,
	line int, column int,
	behavior string,
	hit bool,
) {
	loc := &locationInfo{classname, funcname, filename, line, column}
	guidanceFn := behavior_to_guidance(behavior)
	booleanGuidanceImpl(named_bools, message, id, loc, guidanceFn, hit)
}

func numericFields[T Number](left, right T) map[string]any {
	return map[string]any{"left": left, "right": right}
}

func booleanFields(named_bools []NamedBool) map[string]any {
	fields := make(map[string]any, len(named_bools))
	for _, named_bool := range named_bools {
		fields[named_bool.First] = named_bool.Second
	}
	return fields
}

// Whether a numeric rich-assert evaluation can return before capturing its
// location or building its details: the assertion side has already emitted
// for this condition, and the guidance side does not improve the tracked
// extremum. Both checks are lock-free loads. False while the guidance entry
// does not exist, so the first evaluation still takes the full path.
func numericRichAssertSettled[T Number](left, right T, message string, condition bool) bool {
	if !trackerEmitted(message, condition) {
		return false
	}
	entry, ok := numericGuidanceTracker.Load(message)
	if !ok {
		return false
	}
	_, improves := numericGuidanceImproves(entry.(*numericGuidanceInfo), left, right, message)
	return !improves
}

// Equivalent to asserting Always(left > right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func AlwaysGreaterThan[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left > right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left > right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, universalTest, alwaysDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMinimize, wasHit)
}

// Equivalent to asserting Always(left >= right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func AlwaysGreaterThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left >= right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left >= right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, universalTest, alwaysDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMinimize, wasHit)
}

// Equivalent to asserting Sometimes(T left > T right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func SometimesGreaterThan[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left > right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left > right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, existentialTest, sometimesDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMaximize, wasHit)
}

// Equivalent to asserting Sometimes(T left >= T right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func SometimesGreaterThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left >= right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left >= right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, existentialTest, sometimesDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMaximize, wasHit)
}

// Equivalent to asserting Always(left < right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func AlwaysLessThan[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left < right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left < right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, universalTest, alwaysDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMaximize, wasHit)
}

// Equivalent to asserting Always(left <= right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func AlwaysLessThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left <= right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left <= right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, universalTest, alwaysDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMaximize, wasHit)
}

// Equivalent to asserting Sometimes(T left < T right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func SometimesLessThan[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left < right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left < right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, existentialTest, sometimesDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMinimize, wasHit)
}

// Equivalent to asserting Sometimes(T left <= T right, message, details). Information about left and right will automatically be added to the details parameter, with keys left and right. If you use this function for assertions that compare numeric quantities, you may help Antithesis find more bugs.
func SometimesLessThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {
	if numericRichAssertSettled(left, right, message, left <= right) {
		return
	}
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	condition := left <= right
	assertImpl(condition, message, details, loc, wasHit, mustBeHit, existentialTest, sometimesDisplay, id, func() map[string]any {
		return numericFields(left, right)
	})

	numericGuidanceImpl(left, right, message, id, loc, guidanceFnMinimize, wasHit)
}

// Asserts that every time this is called, at least one bool in named_bools is true. Equivalent to Always(named_bools[0].second || named_bools[1].second || ..., message, details). If you use this for assertions about the behavior of booleans, you may help Antithesis find more bugs. Information about named_bools will automatically be added to the details parameter, and the keys will be the names of the bools.
func AlwaysSome(named_bools []NamedBool, message string, details map[string]any) {
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	disjunction := false
	for _, named_bool := range named_bools {
		if named_bool.Second {
			disjunction = true
			break
		}
	}
	assertImpl(disjunction, message, details, loc, wasHit, mustBeHit, universalTest, alwaysDisplay, id, func() map[string]any {
		return booleanFields(named_bools)
	})

	booleanGuidanceImpl(named_bools, message, id, loc, guidanceFnWantNone, wasHit)
}

// Asserts that at least one time this is called, every bool in named_bools is true. Equivalent to Sometimes(named_bools[0].second && named_bools[1].second && ..., message, details). If you use this for assertions about the behavior of booleans, you may help Antithesis find more bugs. Information about named_bools will automatically be added to the details parameter, and the keys will be the names of the bools.
func SometimesAll(named_bools []NamedBool, message string, details map[string]any) {
	loc := newLocationInfo(offsetAPICaller)
	id := makeKey(message, loc)
	conjunction := true
	for _, named_bool := range named_bools {
		if !named_bool.Second {
			conjunction = false
			break
		}
	}
	assertImpl(conjunction, message, details, loc, wasHit, mustBeHit, existentialTest, sometimesDisplay, id, func() map[string]any {
		return booleanFields(named_bools)
	})

	booleanGuidanceImpl(named_bools, message, id, loc, guidanceFnWantAll, wasHit)
}

// Guidance data is nil or already an object (the numeric operands struct or
// the named-bool map), so the packet needs no wrapping and serializes in one
// pass; a failure drops it.
func emitGuidance(gI *guidanceInfo) {
	internal.EmitWithDetails(struct {
		Guidance *guidanceInfo `json:"antithesis_guidance"`
	}{gI}, gI.Id)
}
