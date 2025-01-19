package csproto

// Bool returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func Bool(v bool) *bool {
	return &v
}

// Int returns a pointer to v as an int32 value (for use when assigning pointer fields on Protobuf message types)
func Int(v int) *int32 {
	p := int32(v)
	return &p
}

// Int32 returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func Int32(v int32) *int32 {
	return &v
}

// Int64 returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func Int64(v int64) *int64 {
	return &v
}

// Uint32 returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func Uint32(v uint32) *uint32 {
	return &v
}

// Uint64 returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func Uint64(v uint64) *uint64 {
	return &v
}

// Float32 returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func Float32(v float32) *float32 {
	return &v
}

// Float64 returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func Float64(v float64) *float64 {
	return &v
}

// String returns a pointer to v (for use when assigning pointer fields on Protobuf message types)
func String(v string) *string {
	return &v
}
