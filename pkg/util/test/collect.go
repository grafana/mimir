// SPDX-License-Identifier: AGPL-3.0-only

package test

import "github.com/stretchr/testify/assert"

type CollectWithLogf struct {
	*assert.CollectT
}

func NewCollectWithLogf(collectT *assert.CollectT) *CollectWithLogf {
	return &CollectWithLogf{
		CollectT: collectT,
	}
}

func (c *CollectWithLogf) Logf(_ string, _ ...interface{}) {}
