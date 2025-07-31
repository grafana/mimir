// SPDX-License-Identifier: AGPL-3.0-only

package frontendv2pb

import "context"

type QueryResultStream interface {
	Write(context.Context, *QueryResultStreamRequest) error
}
