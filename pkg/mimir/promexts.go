// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/prometheus/common/model"
)

func init() {
	promqlext.ExtendPromQL()
	// Mimir doesn't support Prometheus' UTF-8 metric/label name scheme yet.
	model.NameValidationScheme = model.LegacyValidation
}
