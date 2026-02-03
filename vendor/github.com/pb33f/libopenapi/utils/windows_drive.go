package utils

import (
	"path/filepath"
	"strings"
)

func ReplaceWindowsDriveWithLinuxPath(path string) string {
	if len(path) > 1 && path[1] == ':' {
		path = strings.ReplaceAll(path, "\\", "/")
		return path[2:]
	}
	return strings.ReplaceAll(path, "\\", "/")
}

// CheckPathOverlap joins pathA and pathB while avoiding duplicated overlapping segments.
// It tolerates mixed separators in the inputs and uses OS-specific path comparison rules.
func CheckPathOverlap(pathA, pathB, sep string) string {
	if sep == "" {
		sep = string(filepath.Separator)
	}

	// Split on both separators so mixed-path inputs are handled safely.
	split := func(p string) []string {
		return strings.FieldsFunc(p, func(r rune) bool {
			return r == '/' || r == '\\'
		})
	}

	aParts := split(pathA)
	bParts := split(pathB)

	// Find the longest suffix of aParts that matches the prefix of bParts.
	overlap := 0
	maxCheck := len(aParts)
	if len(bParts) < maxCheck {
		maxCheck = len(bParts)
	}
	for i := maxCheck; i > 0; i-- {
		start := len(aParts) - i
		match := true
		for j := 0; j < i; j++ {
			if !pathPartEqual(aParts[start+j], bParts[j]) {
				match = false
				break
			}
		}
		if match {
			overlap = i
			break
		}
	}

	if overlap > 0 {
		bParts = bParts[overlap:]
	}

	// sep is used to build the tail; filepath.Join normalizes separators for the OS.
	tail := strings.Join(bParts, sep)
	if tail == "" {
		if pathA == "" {
			return ""
		}
		return filepath.Clean(pathA)
	}

	return filepath.Join(pathA, tail)
}
