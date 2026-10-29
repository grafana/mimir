// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"math/rand" //#nosec G404 -- Test-only random generator, no need for a CSPRNG -- nosemgrep: math-random-used
	"reflect"
)

// Generate implements testing/quick.Generator.
func (StartTimestampStrategy) Generate(rand *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(StartTimestampStrategy(rand.Intn(4)))
}
