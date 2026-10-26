// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"fmt"
	"regexp"
	"strconv"
)

// MaxHashesRefusal is the message of a source's refusal of a request that
// asks for hashes when its matching series are more than maxHashes. The
// querier reads the series back with HashesNeeded.
func MaxHashesRefusal(series, maxHashes int64) string {
	return fmt.Sprintf("the request would hash %d matching series, more than max_hashes %d", series, maxHashes)
}

var hashesNeededRE = regexp.MustCompile(`would hash (\d+) matching series, more than max_hashes`)

// HashesNeeded is the matching series a MaxHashesRefusal message names.
func HashesNeeded(msg string) (int64, bool) {
	m := hashesNeededRE.FindStringSubmatch(msg)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	return n, err == nil
}
