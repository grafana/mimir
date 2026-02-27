// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
)

// SplitFunctionRegistry is the registry of functions that support range vector splitting.
var SplitFunctionRegistry = map[functions.Function]SplitOperatorFactory{
	functions.FUNCTION_SUM_OVER_TIME:   SplitSumOverTime,
	functions.FUNCTION_COUNT_OVER_TIME: SplitCountOverTime,
	functions.FUNCTION_MIN_OVER_TIME:   SplitMinOverTime,
	functions.FUNCTION_MAX_OVER_TIME:   SplitMaxOverTime,
	functions.FUNCTION_RATE:            SplitRate,
	functions.FUNCTION_INCREASE:        SplitIncrease,
}
