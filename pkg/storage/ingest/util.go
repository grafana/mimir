// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/grafana/regexp"
)

var (
	// Regular expression used to parse the ingester numeric ID.
	ingesterIDRegexp = regexp.MustCompile("-(zone-.-)?([0-9]+)$")

	// The Prometheus summary objectives used when tracking latency.
	latencySummaryObjectives = map[float64]float64{
		0.5:   0.05,
		0.90:  0.01,
		0.99:  0.001,
		0.995: 0.001,
		0.999: 0.001,
		1:     0.001,
	}
)

// IngesterPartition returns the partition ID to use to write to a specific ingester partition.
// The input ingester ID is expected to end either with "zone-X-Y" or only "-Y" where "X" is a letter in the range [a,d]
// and "Y" is a positive integer number. This means that this function supports up to 4 zones starting
// with letter "a" or no zones at all.
func IngesterPartition(ingesterID string) (int32, error) {
	match := ingesterIDRegexp.FindStringSubmatch(ingesterID)
	if len(match) == 0 {
		return 0, fmt.Errorf("name doesn't match regular expression %s %q", ingesterID, ingesterIDRegexp.String())
	}

	// Convert the zone ID to a number starting from 0.
	var zoneID int32
	if wholeZoneStr := match[1]; len(wholeZoneStr) > 1 {
		if !strings.HasPrefix(wholeZoneStr, "zone-") {
			return 0, fmt.Errorf("invalid zone ID %s in %s", wholeZoneStr, ingesterID)
		}

		zoneID = rune(wholeZoneStr[len(wholeZoneStr)-2]) - 'a'
		if zoneID < 0 || zoneID > 4 {
			return 0, fmt.Errorf("zone ID is not between a and d %s", ingesterID)
		}
	}

	// Parse the ingester sequence number.
	ingesterSeq, err := strconv.Atoi(match[2])
	if err != nil {
		return 0, fmt.Errorf("no ingester sequence in name %s", ingesterID)
	}

	partitionID := int32(ingesterSeq<<2) | (zoneID & 0b11)
	return partitionID, nil
}
