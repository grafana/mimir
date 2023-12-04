package ballast

// Allocate allocates ballast of given size, to alter GC behaviour. See https://github.com/golang/go/issues/23044
// Instead of allocating one big slice, we allocate many small ones to avoid keeping too much data in memory when Go runtime
// decides that it needs to zero the slice.
// Returned value should not be used.
func Allocate(ballastSize int) any {
	if ballastSize <= 0 {
		return nil
	}

	const ballastSliceSize = 2 * 1024 * 1024

	result := make([][]byte, 0, (ballastSize/ballastSliceSize)+1) // +1 just in case that ballast is not divisible by ballastSliceSize.
	for ballastSize > 0 {
		sz := ballastSliceSize
		if ballastSize < ballastSliceSize {
			sz = ballastSize
		}
		result = append(result, make([]byte, sz))
		ballastSize -= sz
	}
	return result
}
