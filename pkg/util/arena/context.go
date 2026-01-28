// SPDX-License-Identifier: AGPL-3.0-only

package arena

import (
	"context"
)

type ctxKey struct{}

func FromContext(ctx context.Context) *Arena {
	v, _ := ctx.Value(ctxKey{}).(*Arena)
	return v
}

func ContextWithArena(ctx context.Context, a *Arena) context.Context {
	return context.WithValue(ctx, ctxKey{}, a)
}
