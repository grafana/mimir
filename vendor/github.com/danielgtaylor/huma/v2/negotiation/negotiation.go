// Package negotiation provides utilities for working with HTTP client-
// driven content negotiation. It provides a zero-allocation utility for
// determining the best content type for the server to encode a response.
package negotiation

import (
	"strconv"
	"strings"
)

// SelectQValue selects and returns the best value from the allowed set
// given a header with optional quality values, as you would get for an
// Accept or Accept-Encoding header. The *first* item in allowed is preferred
// if there is a tie. If nothing matches, returns an empty string.
func SelectQValue(header string, allowed []string) string {
	formats := strings.Split(header, ",")
	best := ""
	bestQ := 0.0
	for _, format := range formats {
		parts := strings.Split(format, ";")
		name := strings.Trim(parts[0], " \t")

		found := false
		for _, n := range allowed {
			if n == name {
				found = true
				break
			}
		}

		if !found {
			// Skip formats we don't support.
			continue
		}

		// Default weight to 1 if no value is passed.
		q := 1.0
		if len(parts) > 1 {
			trimmed := strings.Trim(parts[1], " \t")
			if strings.HasPrefix(trimmed, "q=") {
				q, _ = strconv.ParseFloat(trimmed[2:], 64)
			}
		}

		// Prefer the first one if there is a tie.
		if q > bestQ || (q == bestQ && name == allowed[0]) {
			bestQ = q
			best = name
		}
	}

	return best
}

// SelectQValueFast is a faster version of SelectQValue that does not
// need any dynamic memory allocations.
func SelectQValueFast(header string, allowed []string) string {
	best := ""
	bestQ := 0.0

	name := ""
	start := -1
	end := 0

	for pos, char := range header {
		// Format is like "a; q=0.5, b;q=1.0,c; q=0.3"
		if char == ';' {
			name = header[start : end+1]
			start = -1
			continue
		}

		if char == ',' || pos == len(header)-1 {
			q := 1.0
			if pos == len(header)-1 {
				// This is the end, so it must be the name.
				// Example: "application/yaml"
				name = header[start : pos+1]
			} else if name == "" {
				// No name yet means we did not encounter a `;`.
				// Example: "a, b, c"
				name = header[start:pos]
			} else {
				if parsed, _ := strconv.ParseFloat(header[start+2:end+1], 64); parsed > 0 {
					q = parsed
				}
			}
			start = -1

			found := false
			for _, n := range allowed {
				if n == name {
					found = true
					break
				}
			}

			if !found {
				// Skip formats we don't support.
				continue
			}

			if q > bestQ || (q == bestQ && name == allowed[0]) {
				bestQ = q
				best = name
			}
			continue
		}

		if char != ' ' && char != '\t' {
			end = pos
			if start == -1 {
				start = pos
			}
		}
	}

	return best
}
