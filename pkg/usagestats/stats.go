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
	NewString(targetKey).Set(target)
}

// SetEdition sets the edition name.
func SetEdition(edition string) {
	NewString(editionKey).Set(edition)
}

// NewString returns a new String stats object.
// If a String stats object with the same name already exists it is returned.
func NewString(name string) *expvar.String {
	existing := expvar.Get(statsPrefix + name)
	if existing != nil {
		if s, ok := existing.(*expvar.String); ok {
			return s
		}
		panic(fmt.Sprintf("%v is set to a non-string value", name))
	}
	return expvar.NewString(statsPrefix + name)
}
