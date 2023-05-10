package functions

import (
	"math"
	"os"
	"strconv"
)

func init() {
	initSqrtFunc()
	initLog10RootFunc()
}

func initSqrtFunc() {
	// compute square root numbers up to Max(1000||env['GO_CONCURRENCY_LIMIT_SQRT_PRE_COMPUTE'])
	defaultVal := 1000
	valStr, ok := os.LookupEnv("GO_CONCURRENCY_LIMIT_SQRT_PRE_COMPUTE")
	if !ok {
		valStr = "1000"
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		val = defaultVal
	}
	if val < defaultVal {
		val = defaultVal
	}
	for i := 0; i < val; i++ {
		// pre-compute the integer value with a min of 1 up to val samples.
		sqrtRootLookup = append(sqrtRootLookup, int(math.Max(1, float64(int(math.Sqrt(float64(i)))))))
	}
}

func initLog10RootFunc() {
	// compute log10 root numbers up to Max(1000||env['GO_CONCURRENCY_LIMIT_LOG10ROOT_PRE_COMPUTE'])
	defaultVal := 1000
	valStr, ok := os.LookupEnv("GO_CONCURRENCY_LIMIT_LOG10ROOT_PRE_COMPUTE")
	if !ok {
		valStr = "1000"
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		val = defaultVal
	}
	if val < defaultVal {
		val = defaultVal
	}
	for i := 0; i < val; i++ {
		// pre-compute the integer value with a min of 1 up to val samples.
		log10RootLookup = append(log10RootLookup, int(math.Max(1, float64(int(math.Log10(float64(i)))))))
	}
}
