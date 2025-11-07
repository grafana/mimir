// SPDX-License-Identifier: AGPL-3.0-only

package functions

var promQLNamesToFunctions = map[string]Function{}

var FunctionsSupportingAnchoredMatrix = map[Function]struct{}{
	FUNCTION_RESETS:   {},
	FUNCTION_CHANGES:  {},
	FUNCTION_RATE:     {},
	FUNCTION_INCREASE: {},
	FUNCTION_DELTA:    {},
}

var FunctionsSupportingSmoothedMatrix = map[Function]struct{}{
	FUNCTION_RATE:     {},
	FUNCTION_INCREASE: {},
	FUNCTION_DELTA:    {},
}

func (f Function) PromQLName() string {
	fnc, ok := RegisteredFunctions[f]

	if !ok {
		return f.String()
	}

	return fnc.Name
}

func FromPromQLName(name string) (Function, bool) {
	f, ok := promQLNamesToFunctions[name]
	return f, ok
}
