// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/oklog/ulid"
)

func main() {
	seconds := flag.Bool("seconds", false, "Print timestamp as unix timestamp in seconds")
	flag.Parse()

	if len(flag.Args()) == 0 {
		fmt.Println("Usage:", os.Args[0], "[ulid ...]")
		os.Exit(1)
		return
	}

	exit := 0
	for _, v := range flag.Args() {
		id, err := ulid.Parse(v)
		if err != nil {
			log.Printf("failed to parse %q: %v", v, err)
			exit = 1
		} else {
			if *seconds {
				fmt.Println(id.String(), ulid.Time(id.Time()).UTC().Unix())
			} else {
				fmt.Println(id.String(), ulid.Time(id.Time()).UTC().Format(time.RFC3339Nano))
			}
		}
	}

	os.Exit(exit)
}
