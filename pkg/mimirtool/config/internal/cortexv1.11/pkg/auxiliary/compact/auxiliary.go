// SPDX-License-Identifier: AGPL-3.0-only

package compact

import "time"

const (
	// PartialUploadThresholdAge is a time after partial block is assumed aborted and ready to be cleaned.
	// Keep it long as it is based on block creation time not upload start time.
	PartialUploadThresholdAge = 2 * 24 * time.Hour
)
