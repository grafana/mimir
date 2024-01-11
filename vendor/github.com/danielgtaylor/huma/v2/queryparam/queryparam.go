// Package queryparam provides utilities for efficiently working with query
// parameters in URLs. It focuses on zero allocation URL query parameter
// lookups.
//
//	func handler(w http.ResponseWriter, r *http.Request) {
//		// Get the value of the `key` query parameter.
//		value := queryparam.Get(r.URL.RawQuery, "key")
//	}
//
// Note that this method does not support multiple values for the same key,
// so using `val=1,2,3` is preferable to `val=1&val=2&val=3`.
package queryparam

import (
	"net/url"
	"strings"
)

// Get a query param by name without any dynamic allocations. For small numbers
// of query params, it is faster to call this method multiple times than to
// use `url.ParseQuery` and then call `Get` on the resulting `url.Values`.
// This method does not support multiple values for the same key, so using
// `val=1,2,3` is preferable to `val=1&val=2&val=3`.
func Get(query, name string) string {
	pos := 0
	for pos < len(query) {
		end := strings.IndexRune(query[pos:], '=')
		if end == -1 {
			end = strings.IndexRune(query[pos:], '&')
		}

		if end == -1 {
			end = len(query)
		} else {
			end += pos
		}

		ueName, _ := url.QueryUnescape(query[pos:end])
		if ueName == name {
			if end == len(query) || query[end] == '&' {
				return "true"
			}
			pos = end + 1
			end = strings.IndexRune(query[pos:], '&')
			if end == -1 {
				end = len(query)
			} else {
				end += pos
			}
			escaped, _ := url.QueryUnescape(query[pos:end])
			return escaped
		}
		tmp := pos
		pos = strings.IndexRune(query[pos:], '&')
		if pos == -1 {
			break
		}
		pos += tmp + 1
	}
	return ""
}
