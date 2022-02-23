package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "Usage: check-out-of-order <dir>")
		os.Exit(1)
	}

	if err := action(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func action() error {
	dpath := flag.Args()[0]
	entries, err := os.ReadDir(dpath)
	if err != nil {
		return fmt.Errorf("failed to read directory %q: %w", dpath, err)
	}

	reOutOfOrder := regexp.MustCompile(`^"OutOfOrderLabels": (\d+),$`)

	for _, de := range entries {
		cmd := exec.Command("./tools/tsdb-index-health/tsdb-index-health", filepath.Join(dpath, de.Name()))
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("tsdb-index-health failed: %w\n%s", err, string(out))
		}

		for _, l := range strings.Split(string(out), "\n") {
			l = strings.TrimSpace(l)
			ms := reOutOfOrder.FindStringSubmatch(l)
			if ms == nil {
				continue
			}

			num, err := strconv.Atoi(ms[1])
			if err != nil {
				panic(err)
			}
			if num == 0 {
				continue
			}

			log.Printf("block %s has %d out of order labels\n", de.Name(), num)
		}
	}

	return nil
}
