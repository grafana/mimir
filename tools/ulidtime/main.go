// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid"
)

func main() {
	seconds := flag.Bool("seconds", false, "Show timestamp as Unix timestamp in seconds")
	millis := flag.Bool("millis", false, "Show timestamp as Unix timestamp in milliseconds")
	entropy := flag.Bool("entropy", false, "Show random part of ULID as hex")
	header := flag.Bool("header", false, "Show header")

	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) == 0 {
		fmt.Println("Usage:", os.Args[0], "[ulid ...]")
		os.Exit(1)
		return
	}

	tw := tabwriter.NewWriter(os.Stdout, 1, 8, 2, ' ', tabwriter.TabIndent)

	if *header {
		fmt.Fprint(tw, "ULID\tTIMESTAMP")
		if *seconds {
			fmt.Fprint(tw, "\tSECONDS")
		}
		if *millis {
			fmt.Fprint(tw, "\tMILLISECONDS")
		}
		if *entropy {
			fmt.Fprint(tw, "\tRANDOM")
		}
		fmt.Fprintln(tw)
	}

	exit := 0
	for _, v := range args {
		id, err := ulid.Parse(v)
		if err != nil {
			log.Printf("failed to parse %q: %v", v, err)
			exit = 1
		} else {
			fmt.Fprint(tw, id.String())
			fmt.Fprint(tw, "\t", ulid.Time(id.Time()).UTC().Format(time.RFC3339Nano))
			if *seconds {
				fmt.Fprint(tw, "\t", ulid.Time(id.Time()).UTC().Unix())
			}
			if *millis {
				fmt.Fprint(tw, "\t", ulid.Time(id.Time()).UTC().UnixMilli())
			}
			if *entropy {
				fmt.Fprintf(tw, "\t%02x", id.Entropy())
			}
			fmt.Fprintln(tw)
		}
	}

	tw.Flush()
	os.Exit(exit)
}
