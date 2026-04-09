// SPDX-License-Identifier: AGPL-3.0-only

package remotewrite

import "context"

type contextKeyRemoteWriteURLs struct{}

// ContextWithRemoteWriteURLs returns a context that carries the given per-rule-group
// remote write URL list. A nil or empty slice means "use all tenant-level configs".
func ContextWithRemoteWriteURLs(ctx context.Context, urls []string) context.Context {
	return context.WithValue(ctx, contextKeyRemoteWriteURLs{}, urls)
}

// RemoteWriteURLsFromContext returns the per-rule-group URL list stored in ctx,
// or nil if none was set.
func RemoteWriteURLsFromContext(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(contextKeyRemoteWriteURLs{}).([]string)
	return v
}
