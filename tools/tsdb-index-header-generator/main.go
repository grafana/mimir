// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"log"
	"path"

	"github.com/oklog/ulid"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storegateway/indexheader"
)

// Writes the index header for the given block directory (first argument) to the given file (second argument).
// eg. tsdb-index-header-generator /path/to/blocks/10428/01E0VTN88859KW1KTDVBS14E7A /path/to/index-header
func main() {
	flag.Parse()

	blockDirectory := flag.Arg(0)
	outputPath := flag.Arg(1)

	bucketRoot, id := path.Split(blockDirectory)
	bkt, err := filesystem.NewBucket(bucketRoot)

	if err != nil {
		log.Fatalf(err.Error())
	}

	parsedId, err := ulid.Parse(id)

	if err != nil {
		log.Fatalf(err.Error())
	}

	if err := indexheader.WriteBinary(context.Background(), bkt, parsedId, outputPath); err != nil {
		log.Fatalf(err.Error())
	}
}
