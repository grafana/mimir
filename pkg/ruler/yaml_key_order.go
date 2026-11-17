// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/go-yaml/blob/4d2ac027e9e5e875b7b014d285f050fd1e6a9974/sorter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: 2011-2019 Canonical Ltd.

package ruler

import "unicode"

// yamlKeyLess reports whether yaml.v3 writes string map key a before string map key b.
// It copies the string case of keyList.Less in yaml.v3 sorter.go.
// TestYAMLKeyLess compares it with yaml.Marshal.
func yamlKeyLess(a, b string) bool {
	ar, br := []rune(a), []rune(b)
	digits := false
	for i := 0; i < len(ar) && i < len(br); i++ {
		if ar[i] == br[i] {
			digits = unicode.IsDigit(ar[i])
			continue
		}
		al := unicode.IsLetter(ar[i])
		bl := unicode.IsLetter(br[i])
		if al && bl {
			return ar[i] < br[i]
		}
		if al || bl {
			if digits {
				return al
			}
			return bl
		}
		var ai, bi int
		var an, bn int64
		if ar[i] == '0' || br[i] == '0' {
			for j := i - 1; j >= 0 && unicode.IsDigit(ar[j]); j-- {
				if ar[j] != '0' {
					an = 1
					bn = 1
					break
				}
			}
		}
		for ai = i; ai < len(ar) && unicode.IsDigit(ar[ai]); ai++ {
			an = an*10 + int64(ar[ai]-'0')
		}
		for bi = i; bi < len(br) && unicode.IsDigit(br[bi]); bi++ {
			bn = bn*10 + int64(br[bi]-'0')
		}
		if an != bn {
			return an < bn
		}
		if ai != bi {
			return ai < bi
		}
		return ar[i] < br[i]
	}
	return len(ar) < len(br)
}
