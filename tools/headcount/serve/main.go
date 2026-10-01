// SPDX-License-Identifier: AGPL-3.0-only

// Command serve runs the Headcount demo: an HTTP API that answers
// cardinality questions from the blocks of one compacted snapshot, using
// the cardpoc library, and a single page that puts each answer next to the
// same question asked of a running Mimir through PromQL.
package main

import (
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/grafana/mimir/tools/headcount/cardgen"
	"github.com/grafana/mimir/tools/headcount/model"
)

//go:embed static
var static embed.FS

func main() {
	listen := flag.String("listen", "localhost:18090", "address to serve the API and page on")
	snapshots := flag.String("snapshots", "", "directory holding the compacted snapshot from tools/headcount/fixtures (required)")
	profile := flag.String("profile", "medium", fmt.Sprintf("profile the snapshot was built from, one of %v", cardgen.ProfileNames()))
	seed := flag.Int64("seed", 1, "seed the snapshot was built with")
	mimirURL := flag.String("mimir", "http://localhost:18080", "base URL of a Mimir serving the same compacted snapshot")
	mimirLog := flag.String("mimir-log", "", "Mimir's log file, read for the query stats line of each PromQL query")
	runtimeConfig := flag.String("mimir-runtime-config", "", "Mimir's runtime config file, rewritten to apply a series limit to PromQL queries")
	flag.Parse()

	if *snapshots == "" {
		log.Fatal("-snapshots is required")
	}
	p, err := cardgen.LoadProfile(*profile, *seed)
	if err != nil {
		log.Fatal(err)
	}
	pop, err := model.New(p.Population)
	if err != nil {
		log.Fatal(err)
	}

	t0 := time.Now()
	s, err := newServer(filepath.Join(*snapshots, "compacted"), pop, &mimir{
		baseURL:       strings.TrimRight(*mimirURL, "/"),
		logPath:       *mimirLog,
		runtimeConfig: *runtimeConfig,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("loaded %d block ranges in %s", len(s.ranges), time.Since(t0).Round(time.Millisecond))

	page, err := fs.Sub(static, "static")
	if err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /", http.FileServerFS(page))
	s.register(mux)
	log.Printf("serving on http://%s", *listen)
	log.Fatal(http.ListenAndServe(*listen, mux))
}
