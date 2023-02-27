// SPDX-License-Identifier: AGPL-3.0-only

package api

import v1 "github.com/prometheus/prometheus/web/api/v1"

type mimirJSONCodec struct {
	v1.JSONCodec
}
