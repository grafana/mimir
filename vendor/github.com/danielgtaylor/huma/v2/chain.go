package huma

// Middlewares is a list of middleware functions that can be attached to an
// API and will be called for all incoming requests.
type Middlewares []func(ctx Context, next func(Context))

// Handler builds and returns a handler func from the chain of middlewares,
// with `endpoint func` as the final handler.
func (m Middlewares) Handler(endpoint func(Context)) func(Context) {
	return m.chain(endpoint)
}

// wrap user middleware func with the next func to one func
func wrap(fn func(Context, func(Context)), next func(Context)) func(Context) {
	return func(ctx Context) {
		fn(ctx, next)
	}
}

// chain builds a Middleware composed of an inline middleware stack and endpoint
// handler in the order they are passed.
func (m Middlewares) chain(endpoint func(Context)) func(Context) {
	// Return ahead of time if there aren't any middlewares for the chain
	if len(m) == 0 {
		return endpoint
	}

	// Wrap the end handler with the middleware chain
	w := wrap(m[len(m)-1], endpoint)
	for i := len(m) - 2; i >= 0; i-- {
		w = wrap(m[i], w)
	}
	return w
}
