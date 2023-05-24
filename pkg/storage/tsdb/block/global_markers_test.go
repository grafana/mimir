// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/bucketindex/markers_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package block

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
)

func TestDeletionMarkFilepath(t *testing.T) {
	id := ulid.MustNew(1, nil)

	assert.Equal(t, "markers/"+id.String()+"-deletion-mark.json", DeletionMarkFilepath(id))
}

func TestIsDeletionMarkFilename(t *testing.T) {
	expected := ulid.MustNew(1, nil)

	_, ok := IsDeletionMarkFilename("xxx")
	assert.False(t, ok)

	_, ok = IsDeletionMarkFilename("xxx-deletion-mark.json")
	assert.False(t, ok)

	_, ok = IsDeletionMarkFilename("tenant-deletion-mark.json")
	assert.False(t, ok)

	actual, ok := IsDeletionMarkFilename(expected.String() + "-deletion-mark.json")
	assert.True(t, ok)
	assert.Equal(t, expected, actual)
}

func TestNoCompactMarkFilepath(t *testing.T) {
	id := ulid.MustNew(1, nil)

	assert.Equal(t, "markers/"+id.String()+"-no-compact-mark.json", NoCompactMarkFilepath(id))
}

func TestIsNoCompactMarkFilename(t *testing.T) {
	expected := ulid.MustNew(1, nil)

	_, ok := IsNoCompactMarkFilename("xxx")
	assert.False(t, ok)

	_, ok = IsNoCompactMarkFilename("xxx-no-compact-mark.json")
	assert.False(t, ok)

	_, ok = IsNoCompactMarkFilename("tenant-no-compact-mark.json")
	assert.False(t, ok)

	actual, ok := IsNoCompactMarkFilename(expected.String() + "-no-compact-mark.json")
	assert.True(t, ok)
	assert.Equal(t, expected, actual)
}
