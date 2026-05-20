// SPDX-License-Identifier: AGPL-3.0-only

package requestoptions

import "context"

type contextKey int

const optionsKey contextKey = iota

// WithOptions returns a context with the given Options attached.
func WithOptions(ctx context.Context, options Options) context.Context {
	return context.WithValue(ctx, optionsKey, options)
}

// FromContext returns the Options attached to ctx, or the zero value if none
// was set.
func FromContext(ctx context.Context) Options {
	if v := ctx.Value(optionsKey); v != nil {
		return v.(Options)
	}
	return Options{}
}
