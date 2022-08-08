// SPDX-License-Identifier: AGPL-3.0-only

package usagestats

import (
	"expvar"
	"fmt"
)

const (
	statsPrefix = "github.com/grafana/mimir/"
	targetKey   = "target"
	editionKey  = "edition"

	EditionOSS        = "oss"
	EditionEnterprise = "enterprise"
)

func init() {
	SetEdition(EditionOSS)
}

// SetTarget sets the target name.
func SetTarget(target string) {
	getString(targetKey).Set(target)
}

// SetEdition sets the edition name.
func SetEdition(edition string) {
	getString(editionKey).Set(edition)
}

// getString returns the String stats object for the given name.
// It creates the stats object if doesn't exist yet.
func getString(name string) *expvar.String {
	existing := expvar.Get(statsPrefix + name)
	if existing != nil {
		if s, ok := existing.(*expvar.String); ok {
			return s
		}
		panic(fmt.Sprintf("%v is set to a non-string value", name))
	}
	return expvar.NewString(statsPrefix + name)
}
