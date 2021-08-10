// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/chunk/cache/background_extra_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package cache

func Flush(c Cache) {
	b := c.(*backgroundCache)
	close(b.bgWrites)
	b.wg.Wait()
}
