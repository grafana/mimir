// SPDX-License-Identifier: AGPL-3.0-only

package series

import (
	"encoding/base64"

	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/crypto/blake2b"
)

// HashLabelName is the name of the label used for storing the unique ID
// for a series before any labels are dropped. It is named the way it is
// so that it sorts _after_ all other label names so that it does not affect
// the order that series should be returned in. The series merging logic
// in queriers depends on results from ingesters and store-gateways being
// sorted by all labels to deduplicate them.
const HashLabelName = "~~series_hash~~"

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

	sum := blake2b.Sum256(lbls.Bytes(h.buffer))

	h.builder.Keep(retain...)
	h.builder.Set(HashLabelName, base64.URLEncoding.EncodeToString(sum[0:]))

	return h.builder.Labels()
}
