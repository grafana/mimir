// SPDX-License-Identifier: AGPL-3.0-only

package activeseriesmodel

import (
	"math/rand" //#nosec G404 -- Test-only random generator, no need for a CSPRNG -- nosemgrep: math-random-used
	"reflect"
	"strings"
)

// Generate implements testing/quick.Generator.
func (CustomTrackersConfig) Generate(rand *rand.Rand, _ int) reflect.Value {
	m := map[string]string{}
	for i := rand.Intn(3); i > 0; i-- {
		m[randLabelName(rand)] = randMatchers(rand)
	}
	c, err := NewCustomTrackersConfig(m)
	if err != nil {
		panic(err)
	}
	return reflect.ValueOf(c)
}

func randMatchers(rand *rand.Rand) string {
	ops := []string{"=", "!=", "=~", "!~"}
	parts := make([]string, rand.Intn(3)+1)
	for i := range parts {
		// Values are alphanumeric, so they are also valid regular expressions
		// for the =~ and !~ operators.
		parts[i] = randLabelName(rand) + ops[rand.Intn(len(ops))] + `"` + randAlphaNum(rand, rand.Intn(6)) + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func randLabelName(rand *rand.Rand) string {
	const head = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"
	name := []byte{head[rand.Intn(len(head))]}
	return string(name) + randAlphaNum(rand, rand.Intn(6))
}

func randAlphaNum(rand *rand.Rand, n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}
