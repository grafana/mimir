// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"strings"
	"testing"
)

func Unindent(t *testing.T, s string) string {
	lines := strings.Split(s, "\n")
	outlines := make([]string, 0, len(lines))
	removeSpaces := 0
	spaceCharacter := " "

	for _, line := range lines {
		if len(line) == 0 {
			// Empty line.
			outlines = append(outlines, "")
			continue
		}
		if line[0] != ' ' && line[0] != '\t' {
			// Nothing to do if line does not start with space or tab.
			return s
		}
		if removeSpaces == 0 {
			if line[0] == '\t' {
				spaceCharacter = "\t"
			}
			removeSpaces = len(line) - len(strings.TrimLeft(line, spaceCharacter))
			if removeSpaces <= 0 {
				return s
			}
		}
		if len(line) < removeSpaces {
			if len(strings.TrimLeft(line, spaceCharacter)) == 0 {
				// Empty line.
				outlines = append(outlines, "")
				continue
			}
			t.Fatal("non empty line is shorter than the indentation")
		}
		for i := 0; i < removeSpaces; i++ {
			if line[i] != spaceCharacter[0] {
				t.Fatal("line does not start with the same space characters")
			}
		}
		outlines = append(outlines, line[removeSpaces:])
	}
	return strings.Join(outlines, "\n")
}
