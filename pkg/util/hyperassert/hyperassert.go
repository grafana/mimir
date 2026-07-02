// Package hyperassert provides a no-op implementation of a property-based
// assertion API. Callers can use these functions freely in production code
// paths; a later build step may substitute a real implementation to make the
// assertions effective.
package hyperassert

// Number matches the allowable numeric types of comparison parameters.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint8 | ~uint16 | ~uint32 | ~float32 | ~float64 | ~uint64 | ~uint | ~uintptr
}

// NamedBool is used for boolean assertions.
type NamedBool struct {
	First  string `json:"first"`
	Second bool   `json:"second"`
}

// NewNamedBool constructs a NamedBool used for boolean assertions.
func NewNamedBool(first string, second bool) *NamedBool {
	return &NamedBool{First: first, Second: second}
}

// Always asserts that condition is true every time this function is called,
// and that it is called at least once.
func Always(condition bool, message string, details map[string]any) {}

// AlwaysOrUnreachable asserts that condition is true every time this function
// is called. The corresponding test property passes if the assertion is never
// encountered.
func AlwaysOrUnreachable(condition bool, message string, details map[string]any) {}

// AlwaysGreaterThan is equivalent to asserting Always(left > right, message, details).
func AlwaysGreaterThan[T Number](left, right T, message string, details map[string]any) {}

// AlwaysGreaterThanOrEqualTo is equivalent to asserting Always(left >= right, message, details).
func AlwaysGreaterThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {}

// AlwaysLessThan is equivalent to asserting Always(left < right, message, details).
func AlwaysLessThan[T Number](left, right T, message string, details map[string]any) {}

// AlwaysLessThanOrEqualTo is equivalent to asserting Always(left <= right, message, details).
func AlwaysLessThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {}

// AlwaysSome asserts that every time this is called, at least one bool in
// namedBools is true.
func AlwaysSome(namedBools []NamedBool, message string, details map[string]any) {}

// Sometimes asserts that condition is true at least one time that this
// function was called.
func Sometimes(condition bool, message string, details map[string]any) {}

// SometimesAll asserts that at least one time this is called, every bool in
// namedBools is true.
func SometimesAll(namedBools []NamedBool, message string, details map[string]any) {}

// SometimesGreaterThan is equivalent to asserting Sometimes(left > right, message, details).
func SometimesGreaterThan[T Number](left, right T, message string, details map[string]any) {}

// SometimesGreaterThanOrEqualTo is equivalent to asserting Sometimes(left >= right, message, details).
func SometimesGreaterThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {}

// SometimesLessThan is equivalent to asserting Sometimes(left < right, message, details).
func SometimesLessThan[T Number](left, right T, message string, details map[string]any) {}

// SometimesLessThanOrEqualTo is equivalent to asserting Sometimes(left <= right, message, details).
func SometimesLessThanOrEqualTo[T Number](left, right T, message string, details map[string]any) {}

// Reachable asserts that a line of code is reached at least once.
func Reachable(message string, details map[string]any) {}

// Unreachable asserts that a line of code is never reached.
func Unreachable(message string, details map[string]any) {}

// AssertRaw is a low-level method designed to be used by third-party
// frameworks. Regular users should not call it.
func AssertRaw(cond bool, message string, details map[string]any,
	classname, funcname, filename string, line int,
	hit bool, mustHit bool,
	assertType string, displayType string,
	id string,
) {
}

// BooleanGuidanceRaw is a low-level method designed to be used by third-party
// frameworks. Regular users should not call it.
func BooleanGuidanceRaw(
	namedBools []NamedBool,
	message, id string,
	classname, funcname, filename string,
	line int,
	behavior string,
	hit bool,
) {
}

// NumericGuidanceRaw is a low-level method designed to be used by third-party
// frameworks. Regular users should not call it.
func NumericGuidanceRaw[T Number](
	left, right T,
	message, id string,
	classname, funcname, filename string,
	line int,
	behavior string,
	hit bool,
) {
}
