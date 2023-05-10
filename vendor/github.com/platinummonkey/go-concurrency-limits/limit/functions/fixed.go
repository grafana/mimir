package functions

// FixedQueueSizeFunc implements a fixed amount the estimated limit can grow while latencies remain low
func FixedQueueSizeFunc(queueSize int) func(int) int {
	return func(_ int) int {
		return queueSize
	}
}
