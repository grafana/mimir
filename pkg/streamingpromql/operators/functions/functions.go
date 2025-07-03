// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"fmt"
	"strings"
)

var functionsToPromQLNames = convertFunctionConstantsToPromQLNames()
var promQLNamesToFunctions = invert(functionsToPromQLNames)

func convertFunctionConstantsToPromQLNames() map[Function]string {
	names := make(map[Function]string, len(Function_name))

	for idx, constantName := range Function_name {
		f := Function(idx)

		if f == FUNCTION_UNKNOWN {
			continue
		}

		names[f] = strings.ToLower(strings.TrimPrefix(constantName, "FUNCTION_"))
	}

	return names
}

func invert[A, B comparable](original map[A]B) map[B]A {
	inverted := make(map[B]A, len(original))

	for k, v := range original {
		before := len(inverted)
		inverted[v] = k
		after := len(inverted)

		if before == after {
			panic(fmt.Sprintf("duplicate value %v detected", v))
		}
	}

	return inverted
}

func (f Function) PromQLName() string {
	name, ok := functionsToPromQLNames[f]

	if !ok {
		return f.String()
	}

	return name
}

func FromPromQLName(name string) (Function, bool) {
	f, ok := promQLNamesToFunctions[name]
	return f, ok
}
