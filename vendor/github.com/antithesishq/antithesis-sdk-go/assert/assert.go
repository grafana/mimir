//go:build enable_antithesis_sdk

// Package assert enables defining [test properties] about your program or [workload]. It is part of the [Antithesis Go SDK], which enables Go applications to integrate with the [Antithesis platform].
//
// Code that uses this package should be instrumented with the [antithesis-go-generator] utility. This step is required for the Always, Sometime, and Reachable methods. It is not required for the Unreachable and AlwaysOrUnreachable methods, but it will improve the experience of using them.
//
// Outside the Antithesis environment, emitted records are discarded unless the environment variable ANTITHESIS_SDK_LOCAL_OUTPUT is set. When set, these functions log to the file pointed to by that variable using a structured JSON format defined [here]. This allows you to make use of the Antithesis assertions package in your regular testing, or even in production. In particular, very few assertions frameworks offer a convenient way to define [Sometimes assertions], but they can be quite useful even outside Antithesis.
//
// Each function in this package takes a parameter called message, which is a human readable identifier used to aggregate assertions. Antithesis generates one test property per unique message and this test property will be named "<message>" in the [triage report]. Message must be provided as a string literal.
//
// This test property either passes or fails, which depends upon the evaluation of every assertion that shares its message. Different assertions in different parts of the code should have different message, but the same assertion should always have the same message even if it is moved to a different file.
//
// Each function also takes a parameter called details, additional context shown in the [triage report] under the details section of the corresponding property. Details is a map whose values may include structs and other JSON-serializable values. Pass nil for no details; an explicit empty map is preserved as an empty object.
//
// Argument expressions always run, even when the assertion does not emit. Constructing a map or computing an expensive value at the call site still incurs that cost.
//
// Serialization failures report an antithesis_error containing the context and
// error message. The assertion still emits without details.
//
// [Antithesis Go SDK]: https://antithesis.com/docs/using_antithesis/sdk/go/
// [Antithesis platform]: https://antithesis.com
// [test properties]: https://antithesis.com/docs/properties_assertions/properties/
// [workload]: https://antithesis.com/docs/test_templates/first_test/
// [antithesis-go-generator]: https://antithesis.com/docs/using_antithesis/sdk/go/instrumentor/
// [triage report]: https://antithesis.com/docs/reports/
// [here]: https://antithesis.com/docs/using_antithesis/sdk/fallback/
// [Sometimes assertions]: https://antithesis.com/docs/best_practices/sometimes_assertions/
//
// [details]: https://antithesis.com/docs/reports/
package assert

import (
	"encoding/json"

	"github.com/antithesishq/antithesis-sdk-go/internal"
)

type assertInfo struct {
	fields      map[string]any
	Location    *locationInfo  `json:"location"`
	Details     map[string]any `json:"-"`
	AssertType  string         `json:"assert_type"`
	DisplayType string         `json:"display_type"`
	Message     string         `json:"message"`
	Id          string         `json:"id"`
	Hit         bool           `json:"hit"`
	MustHit     bool           `json:"must_hit"`
	Condition   bool           `json:"condition"`
}

type wrappedAssertInfo struct {
	A struct {
		*assertInfo
		Details json.RawMessage `json:"details,omitempty"`
	} `json:"antithesis_assert"`
}

// Prepare details only after the emission gate, so a serialization failure
// cannot discard the assertion or cause repeated attempts on suppressed hits.
func (f *assertInfo) event() wrappedAssertInfo {
	var event wrappedAssertInfo
	event.A.assertInfo = f
	if f.Hit {
		event.A.Details = internal.DetailsWithFields(f.Details, f.fields, f.Id)
	}
	return event
}

// --------------------------------------------------------------------------------
// Assertions
// --------------------------------------------------------------------------------
const (
	wasHit        = true
	mustBeHit     = true
	optionallyHit = false
	expectingTrue = true
)

const (
	universalTest    = "always"
	existentialTest  = "sometimes"
	reachabilityTest = "reachability"
)

const (
	alwaysDisplay              = "Always"
	alwaysOrUnreachableDisplay = "AlwaysOrUnreachable"
	sometimesDisplay           = "Sometimes"
	reachableDisplay           = "Reachable"
	unreachableDisplay         = "Unreachable"
)

// Always asserts that condition is true every time this function is called, and that it is called at least once. The corresponding test property will be viewable in the Antithesis SDK: Always group of your triage report.
func Always(condition bool, message string, details map[string]any) {
	if trackerEmitted(message, condition) {
		return
	}
	locationInfo := newLocationInfo(offsetAPICaller)
	id := makeKey(message, locationInfo)
	assertImpl(condition, message, details, locationInfo, wasHit, mustBeHit, universalTest, alwaysDisplay, id, nil)
}

// AlwaysOrUnreachable asserts that condition is true every time this function is called. The corresponding test property will pass if the assertion is never encountered (unlike Always assertion types). This test property will be viewable in the “Antithesis SDK: Always” group of your triage report.
func AlwaysOrUnreachable(condition bool, message string, details map[string]any) {
	if trackerEmitted(message, condition) {
		return
	}
	locationInfo := newLocationInfo(offsetAPICaller)
	id := makeKey(message, locationInfo)
	assertImpl(condition, message, details, locationInfo, wasHit, optionallyHit, universalTest, alwaysOrUnreachableDisplay, id, nil)
}

// Sometimes asserts that condition is true at least one time that this function was called. (If the assertion is never encountered, the test property will therefore fail.) This test property will be viewable in the “Antithesis SDK: Sometimes” group.
func Sometimes(condition bool, message string, details map[string]any) {
	if trackerEmitted(message, condition) {
		return
	}
	locationInfo := newLocationInfo(offsetAPICaller)
	id := makeKey(message, locationInfo)
	assertImpl(condition, message, details, locationInfo, wasHit, mustBeHit, existentialTest, sometimesDisplay, id, nil)
}

// Unreachable asserts that a line of code is never reached. The corresponding test property will fail if this function is ever called. (If it is never called the test property will therefore pass.) This test property will be viewable in the “Antithesis SDK: Reachablity assertions” group.
func Unreachable(message string, details map[string]any) {
	if trackerEmitted(message, false) {
		return
	}
	locationInfo := newLocationInfo(offsetAPICaller)
	id := makeKey(message, locationInfo)
	assertImpl(false, message, details, locationInfo, wasHit, optionallyHit, reachabilityTest, unreachableDisplay, id, nil)
}

// Reachable asserts that a line of code is reached at least once. The corresponding test property will pass if this function is ever called. (If it is never called the test property will therefore fail.) This test property will be viewable in the “Antithesis SDK: Reachablity assertions” group.
func Reachable(message string, details map[string]any) {
	if trackerEmitted(message, true) {
		return
	}
	locationInfo := newLocationInfo(offsetAPICaller)
	id := makeKey(message, locationInfo)
	assertImpl(true, message, details, locationInfo, wasHit, mustBeHit, reachabilityTest, reachableDisplay, id, nil)
}

// AssertRaw is a low-level method designed to be used by third-party frameworks. Regular users of the assert package should not call it.
func AssertRaw(cond bool, message string, details map[string]any,
	classname, funcname, filename string, line int, column int,
	hit bool, mustHit bool,
	assertType string, displayType string,
	id string,
) {
	if hit && trackerEmitted(id, cond) {
		return
	}
	assertImpl(cond, message, details,
		&locationInfo{classname, funcname, filename, line, column},
		hit, mustHit,
		assertType, displayType,
		id, nil)
}

// Rich assertions always carry a details object, even without guidance
// fields; this stands in for absent ones and is never written.
var noGuidanceFields = map[string]any{}

// RichAssertRaw is a low-level method designed to be used by third-party frameworks. Regular users of the assert package should not call it.
func RichAssertRaw(cond bool, message string, details map[string]any, guidanceDetails map[string]any,
	classname, funcname, filename string, line int, column int,
	hit bool, mustHit bool,
	assertType string, displayType string,
	id string,
) {
	if hit && trackerEmitted(id, cond) {
		return
	}
	assertImpl(cond, message, details,
		&locationInfo{classname, funcname, filename, line, column},
		hit, mustHit,
		assertType, displayType,
		id, func() map[string]any {
			if guidanceDetails == nil {
				return noGuidanceFields
			}
			return guidanceDetails
		})
}

func assertImpl(cond bool, message string, details map[string]any,
	loc *locationInfo,
	hit bool, mustHit bool,
	assertType string, displayType string,
	id string,
	buildFields func() map[string]any,
) {
	trackerEntry := getTrackerEntry(id, loc.Filename, loc.Classname)

	// Always grab the Filename and Classname captured when the trackerEntry was established
	// This provides the consistency needed between instrumentation-time and runtime
	if loc.Filename != trackerEntry.Filename {
		loc.Filename = trackerEntry.Filename
	}

	if loc.Classname != trackerEntry.Classname {
		loc.Classname = trackerEntry.Classname
	}

	trackerEntry.emit(hit, cond, func() error {
		aI := &assertInfo{
			Hit:         hit,
			MustHit:     mustHit,
			AssertType:  assertType,
			DisplayType: displayType,
			Message:     message,
			Condition:   cond,
			Id:          id,
			Location:    loc,
			Details:     details,
		}

		if hit && buildFields != nil {
			aI.fields = buildFields()
		}
		return emitAssert(aI)
	})
}

func makeKey(message string, _ *locationInfo) string {
	return message
}
