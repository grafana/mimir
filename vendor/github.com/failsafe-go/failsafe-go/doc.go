// Package failsafe provides fault tolerance and resilience patterns.
//
// Failsafe-go adds fault tolerance to function execution. Functions can be wrapped with one or more resilience policies, for example:
//
//	result, err := failsafe.Get(fn, retryPolicy)
//
// When multiple policies are provided, are composed around the fn and will handle its results in reverse order. For
// example, consider:
//
//	failsafe.Get(fn, fallback, retryPolicy, circuitBreaker)
//
// This creates the following composition when executing the fn and handling its result:
//
//	Fallback(RetryPolicy(CircuitBreaker(fn)))
package failsafe
