// SPDX-License-Identifier: AGPL-3.0-only

package testutil

import (
	"context"

	"github.com/grafana/mimir/pkg/mimirpb/internal"
)

// NextRefLeakChannel returns a channel where the next check for reference leaks
// to the given object will be reported, whether a leak is detected
// or not. It replaces the default behavior of panicking when a leak is detected.
//
// The object must be identified with a reference value: a pointer, a slice, or
// a string.
//
// Canceling the context immediately closes the channel and restores the default
// behavior.
//
// Only works when [testing.Testing].
func NextRefLeakCheck(ctx context.Context, object any) <-chan bool {
	return internal.NextRefLeakCheck(ctx, object)
}
