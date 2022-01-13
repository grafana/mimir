// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/table_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package chunk

// TableDesc describes a table.
type TableDesc struct {
	Name              string
	UseOnDemandIOMode bool
	ProvisionedRead   int64
	ProvisionedWrite  int64
	Tags              Tags
	WriteScale        AutoScalingConfig
	ReadScale         AutoScalingConfig
}

// Equals returns true if other matches desc.
func (desc TableDesc) Equals(other TableDesc) bool {
	if desc.WriteScale != other.WriteScale {
		return false
	}

	if desc.ReadScale != other.ReadScale {
		return false
	}

	// Only check provisioned read if auto scaling is disabled
	if !desc.ReadScale.Enabled && desc.ProvisionedRead != other.ProvisionedRead {
		return false
	}

	// Only check provisioned write if auto scaling is disabled
	if !desc.WriteScale.Enabled && desc.ProvisionedWrite != other.ProvisionedWrite {
		return false
	}

	// if the billing mode needs updating
	if desc.UseOnDemandIOMode != other.UseOnDemandIOMode {
		return false
	}

	if !desc.Tags.Equals(other.Tags) {
		return false
	}

	return true
}
