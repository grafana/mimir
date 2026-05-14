// SPDX-License-Identifier: AGPL-3.0-only
//
// Helper conversions for translating wire-level search options into
// streaminglabelvalues and Prometheus domain types. Kept here to avoid
// duplicating the same logic across multiple gRPC servers.
package streaminglabelvalues

import (
	"github.com/prometheus/prometheus/storage"
)

// ParamsFromWire constructs Params from wire-level fields.
// - caseInsensitive matches the wire polarity and is inverted internally.
// - useJaroWinkler selects the Jaro-Winkler fuzzy algorithm; subsequence is default.
func ParamsFromWire(terms []string, caseInsensitive bool, useJaroWinkler bool, fuzzThreshold int) (*Params, error) {
	alg := FuzzAlgSubsequence
	if useJaroWinkler {
		alg = FuzzAlgJaroWinkler
	}
	return NewParams(terms, !caseInsensitive, alg, fuzzThreshold)
}

// OrderingFromWire maps booleans derived from a wire enum onto storage.Ordering.
// Prefer score desc when both flags are set (defensive default).
func OrderingFromWire(isValueDesc bool, isScoreDesc bool) storage.Ordering {
	if isScoreDesc {
		return storage.OrderByScoreDesc
	}
	if isValueDesc {
		return storage.OrderByValueDesc
	}
	return storage.OrderByValueAsc
}
