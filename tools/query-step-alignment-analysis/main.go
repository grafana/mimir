// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/grafana/mimir/tools/query-step-alignment-analysis/query_stat"
)

func main() {
	dec := json.NewDecoder(os.Stdin)

	var (
		total                          uint64
		countStepAligned               uint64
		countCacheableWithStepAlign    uint64
		countCacheableWithoutStepAlign uint64
		startTimestamp                 time.Time
		endTimestamp                   time.Time
	)

	var qs query_stat.QueryStat

	for {
		if err := dec.Decode(&qs); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			log.Print("warning: ", err)
			continue
		}
		total++

		if !qs.IsInLast10Minutes() {
			countCacheableWithStepAlign++
		}

		if qs.IsStepAligned() {
			countStepAligned++
		}

		if !qs.IsInLast10Minutes() && qs.IsStepAligned() {
			countCacheableWithoutStepAlign++
		}

		if startTimestamp.IsZero() || startTimestamp.After(qs.Timestamp) {
			startTimestamp = qs.Timestamp
		}
		if endTimestamp.Before(qs.Timestamp) {
			endTimestamp = qs.Timestamp
		}
	}

	fmt.Printf("time-frame:                  %s to %s\n", startTimestamp, endTimestamp)
	fmt.Printf("total queries:               % 9d\n", total)
	fmt.Printf("cachable-with-step-align:    % 9d % 2.2f%%\n", countCacheableWithStepAlign, float64(countCacheableWithStepAlign)/float64(total)*100.0)
	fmt.Printf("cachable-without-step-align: % 9d % 2.2f%%\n", countCacheableWithoutStepAlign, float64(countCacheableWithoutStepAlign)/float64(total)*100.0)
	fmt.Printf("step-aligned:                % 9d % 2.2f%%\n", countStepAligned, float64(countStepAligned)/float64(total)*100.0)
}
