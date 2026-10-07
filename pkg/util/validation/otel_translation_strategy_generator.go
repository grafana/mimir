// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"math/rand" //#nosec G404 -- Test-only random generator, no need for a CSPRNG -- nosemgrep: math-random-used
	"reflect"

	"github.com/prometheus/otlptranslator"
)

// Generate implements testing/quick.Generator.
func (OTelTranslationStrategyValue) Generate(rand *rand.Rand, _ int) reflect.Value {
	choices := []otlptranslator.TranslationStrategyOption{
		otlptranslator.NoUTF8EscapingWithSuffixes,
		otlptranslator.UnderscoreEscapingWithSuffixes,
		otlptranslator.UnderscoreEscapingWithoutSuffixes,
		otlptranslator.NoTranslation,
	}
	return reflect.ValueOf(OTelTranslationStrategyValue(choices[rand.Intn(len(choices))]))
}
