// SPDX-License-Identifier: AGPL-3.0-only

//go:build !native_metadata_nomemo

package store

// recordMemo is whether a record's series find their metadata's id through the record's memo of its values by their
// symbol references: the memo-off variant, built with the native_metadata_nomemo tag, looks each series' up by content
// in the dictionary instead, to measure what the memo saves.
const recordMemo = true
