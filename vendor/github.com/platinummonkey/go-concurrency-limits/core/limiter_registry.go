package core

// LimiterRegistry lookup for integrations that support multiple Limiters, i.e. one per RPC method.
type LimiterRegistry interface {
	// Get a limiter given a key.
	Get(key string) Limiter
}
