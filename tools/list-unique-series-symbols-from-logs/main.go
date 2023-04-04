package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/grafana/regexp"
)

func main() {
	path := "file-path-to-analyze"

	f, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	seriesRE := regexp.MustCompile(`{([^}]+)}`)
	labelNameValueRE := regexp.MustCompile(`([^ ,]+)="([^"]+)"`)
	allSymbols := map[string]int{}

	// Read the file, line by line.
	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)
	for s.Scan() {
		matches := seriesRE.FindStringSubmatch(s.Text())
		if len(matches) != 2 {
			fmt.Println("WARN - Line not parsed:", s.Text())
			continue
		}

		labelsMatch := strings.ReplaceAll(matches[1], `\"`, `"`)
		allMatches := labelNameValueRE.FindAllStringSubmatch(labelsMatch, -1)

		for _, entry := range allMatches {
			labelName := entry[1]
			labelValue := entry[2]

			allSymbols[labelName]++
			allSymbols[labelValue]++
		}
	}

	// Find symbols with only 1 occurrence.
	var singleSymbols []string
	for symbol, count := range allSymbols {
		if count == 1 {
			singleSymbols = append(singleSymbols, symbol)
		}
	}

	sort.Strings(singleSymbols)

	fmt.Println("Symbols with only 1 occurrence:")
	fmt.Println("===============================")
	for _, symbol := range singleSymbols {
		fmt.Println(symbol)
	}
}
