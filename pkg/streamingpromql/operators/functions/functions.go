// SPDX-License-Identifier: AGPL-3.0-only

package functions

var promQLNamesToFunctions = map[string]Function{}

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
