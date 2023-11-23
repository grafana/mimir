package ingest

import (
	"fmt"
	"regexp"
	"strconv"
)

// Regular expression used to parse the ingester numeric ID.
var ingesterIDRegexp = regexp.MustCompile(".*zone-([abcd])-([0-9]+)$")

// IngesterPartition returns the partition ID to use to write to a specific ingester partition.
// The input ingester ID is expected to end with "zone-X-Y" where "X" is a letter in the range [a,d]
// and "Y" is a positive integer number. This means that this function supports up to 4 zones starting
// with letter "a".
func IngesterPartition(ingesterID string) (int, error) {
	match := ingesterIDRegexp.FindStringSubmatch(ingesterID)
	if len(match) == 0 {
		return 0, fmt.Errorf("unable to get the partition ID for %s", ingesterID)
	}

	// Convert the zone ID to a number starting from 0.
	zoneID := int(rune(match[1][0])) - int('a')
	if zoneID < 0 || zoneID > 4 {
		return 0, fmt.Errorf("unable to get the partition ID for %s", ingesterID)
	}

	// Parse the ingester sequence number.
	ingesterSeq, err := strconv.Atoi(match[2])
	if err != nil {
		return 0, fmt.Errorf("unable to get the partition ID for %s", ingesterID)
	}

	partitionID := (ingesterSeq << 2) | (zoneID & 0x3)
	return partitionID, nil
}
