// SPDX-License-Identifier: AGPL-3.0-only

package main

/*
	This linter checks for the presence of a `lint:sorted` directive in a file and verifies that the lines following the directive are sorted lexicographically.
*/

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/grafana/dskit/flagext"
)

const directive = "lint:sorted"

func main() {
	var include string
	var paths flagext.StringSlice
	fs := flag.NewFlagSet("lint-sorted", flag.ExitOnError)
	fs.StringVar(&include, "include", "", "File pattern to include (e.g. \"*.go\")")
	fs.Var(&paths, "path", "Paths to scan (can specify multiple)")
	err := flagext.ParseFlagsWithoutArguments(fs)

	if err != nil || include == "" || len(paths) == 0 {
		if err != nil {
			log.Printf("Error parsing flags: %v\n", err)
		}
		flag.Usage()
		os.Exit(1)
	}

	linted := true
	for _, root := range paths {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			matched, err := filepath.Match(include, info.Name())
			if err != nil {
				return err
			}
			if matched {
				linted = lintFile(path) && linted
			}
			return nil
		})
		if err != nil {
			log.Printf("Error walking the path %s: %v\n", root, err)
		}
	}
	if !linted {
		os.Exit(1)
	}
}

// lintFile reads a file and checks for lint directives
func lintFile(filePath string) bool {
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Unable to open file %s: %v\n", filePath, err)
		return false
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string
	lineNum := 0
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	totalLines := len(lines)
	linted := true
	for lineNum < totalLines {
		if strings.Contains(lines[lineNum], directive) {
			linted = checkSorted(lines, filePath, lineNum+1) && linted
		}
		lineNum++
	}
	return linted

}

// checkSorted verifies whether lines after the directive are sorted lexicographically
func checkSorted(lines []string, filePath string, startIdx int) bool {
	var block []string
	currentIdx := startIdx
	for currentIdx < len(lines) {
		trimmedLine := strings.TrimSpace(lines[currentIdx])
		if trimmedLine == "" || trimmedLine == "}" {
			break
		}
		block = append(block, trimmedLine)
		currentIdx++
	}

	if !sort.StringsAreSorted(block) {
		fmt.Printf("%s:%d Lines not sorted lexicographically after '%s':\n", filePath, startIdx, directive)
		for _, bLine := range block {
			fmt.Printf("  %s\n", bLine)
		}
		return false
	}
	return true
}
