// SPDX-License-Identifier: AGPL-3.0-only

package util

// Optional represents a value that may or may not be present.
type Optional[T any] struct {
	value   T
	present bool
}

// None returns an Optional with no value present.
func None[T any]() Optional[T] {
	return Optional[T]{present: false}
}

// Some returns an Optional with the given value present.
func Some[T any](value T) Optional[T] {
	return Optional[T]{value: value, present: true}
}

// IsPresent returns true if the Optional contains a value.
func (o Optional[T]) IsPresent() bool {
	return o.present
}

// Value returns the value if present, or the zero value of T if not present.
// Use IsPresent() to check if a value is actually present.
func (o Optional[T]) Value() T {
	return o.value
}

// ValueOr returns the value if present, or the provided default value if not present.
func (o Optional[T]) ValueOr(defaultValue T) T {
	if o.present {
		return o.value
	}
	return defaultValue
}

// Map applies the given function to the value if present, returning a new Optional.
// If no value is present, returns None.
func Map[T any, U any](o Optional[T], f func(T) U) Optional[U] {
	if o.present {
		return Some(f(o.value))
	}
	return None[U]()
}
