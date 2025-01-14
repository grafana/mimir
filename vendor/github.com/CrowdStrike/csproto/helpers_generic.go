//go:build go1.18
// +build go1.18

package csproto

// NativeTypes defines a generic constraint for the native Go types that need to be converted to
// pointers when storing values in generated Protobuf message structs.
//
// The int32 constraint is "expanded" to ~int32 to support generated Protobuf enum types, which always
// have an underlying type of int32.  While this technically means we will accept types that are not,
// in fact, Protobuf enums, the utility of making things work for enums outweighs the potential downside
// of developers intentionally using [PointerTo] to take a pointer to a custom type that happens to
// be based on int32.
//
// Unsized integers (type int) also need to be converted to a pointer, but Protobuf doesn't support
// unsized integers.  Use the [csproto.Int] function instead.
type NativeTypes interface {
	bool | ~int32 | int64 | uint32 | uint64 | float32 | float64 | string
}

// PointerTo makes a copy of v and returns a pointer to that copy.
//
// The [NativeTypes] type constraint restricts this function to only types that are valid for Protobuf
// scalar field values (boolean, integer, float, string, and Protobuf enum).
func PointerTo[T NativeTypes](v T) *T {
	return &v
}
