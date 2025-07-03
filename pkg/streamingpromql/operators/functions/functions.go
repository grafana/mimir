// SPDX-License-Identifier: AGPL-3.0-only

package functions

var functionsToPromQLNames = map[Function]string{}
var promQLNamesToFunctions = map[string]Function{}

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
