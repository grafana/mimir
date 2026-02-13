// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"encoding/base64"

	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/crypto/blake2b"
)

const HashLabelName = "__series_hash__"

type Reducer struct {
	builder *labels.Builder
	buffer  []byte
}

func NewReducer() *Reducer {
	return &Reducer{
		builder: labels.NewBuilder(labels.EmptyLabels()),
		buffer:  make([]byte, 0, 1024),
	}
}

func (h *Reducer) Reduce(lbls labels.Labels, retain []string) labels.Labels {
	h.builder.Reset(lbls)
	h.buffer = h.buffer[:0]

	lbls.Range(func(l labels.Label) {
		h.buffer = append(h.buffer, []byte(l.Name)...)
		h.buffer = append(h.buffer, []byte(l.Value)...)
	})

	sum := blake2b.Sum256(h.buffer)
	h.builder.Keep(retain...)
	h.builder.Set(HashLabelName, base64.URLEncoding.EncodeToString(sum[0:]))

	return h.builder.Labels()
}
