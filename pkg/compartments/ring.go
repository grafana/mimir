// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import "strconv"

// readCompartmentRingPrefix builds the "readcomp-<id>-" prefix prepended to a ring key or name to
// scope it to a single read compartment.
func readCompartmentRingPrefix(compartmentID int) string {
	return "readcomp-" + strconv.Itoa(compartmentID) + "-"
}

// ReadCompartmentRingKey returns the KVStore key for the partition ring of the given read
// compartment, derived from the non-compartment ring key.
func ReadCompartmentRingKey(compartmentID int, ringKey string) string {
	return readCompartmentRingPrefix(compartmentID) + ringKey
}

// ReadCompartmentRingName returns the ring name for the given read compartment, derived from the
// non-compartment ring name.
func ReadCompartmentRingName(compartmentID int, ringName string) string {
	return readCompartmentRingPrefix(compartmentID) + ringName
}
