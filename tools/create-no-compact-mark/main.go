package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"time"

	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

// Creates a no-compaction mark for a block.
func main() {
	var (
		ulidString   string
		reasonString string
		details      string
	)
	flag.StringVar(&ulidString, "ulid", "", "The ulid of the block to mark.")
	flag.StringVar(&reasonString, "reason", string(metadata.ManualNoCompactReason),
		fmt.Sprintf("The reason field of the marker. Valid values are %q, %q and %q",
			metadata.ManualNoCompactReason, metadata.IndexSizeExceedingNoCompactReason, metadata.OutOfOrderChunksNoCompactReason))
	flag.StringVar(&details, "details", "", "The details field of the marker.")
	flag.Parse()

	if ulidString == "" {
		fmt.Println("ulid cannot be blank")
		os.Exit(1)
	}

	reason := metadata.NoCompactReason(reasonString)
	switch reason {
	case metadata.ManualNoCompactReason,
		metadata.IndexSizeExceedingNoCompactReason,
		metadata.OutOfOrderChunksNoCompactReason:
		// Valid.
	default:
		fmt.Printf("Invalid reason %q, see help for valid reason values.\n", reasonString)
		os.Exit(1)
	}

	blockID, err := ulid.Parse(ulidString)
	if err != nil {
		fmt.Printf("Can't parse %q as ULID: %s.\n", ulidString, err)
		os.Exit(1)
	}

	noCompactMark, err := json.Marshal(metadata.NoCompactMark{
		ID:            blockID,
		Version:       metadata.NoCompactMarkVersion1,
		NoCompactTime: time.Now().Unix(),
		Reason:        reason,
		Details:       details,
	})

	if err != nil {
		fmt.Printf("Can't create the mark: %s.\n", err)
		os.Exit(1)
	}

	filename := blockID.String() + "-no-compact-mark.json"
	if err := ioutil.WriteFile(filename, noCompactMark, fs.ModePerm); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("Mark saved in %q.\n", filename)
}
