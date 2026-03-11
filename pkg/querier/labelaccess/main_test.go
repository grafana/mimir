// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"os"
	"testing"

	"github.com/grafana/dskit/labelaccess"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestMain(m *testing.M) {
	// This is needed to initiate ParseMetricSelector for tests that might use it.
	labelaccess.SetSelectorParser(promqlext.NewPromQLParser().ParseMetricSelector)
	os.Exit(m.Run())
}
