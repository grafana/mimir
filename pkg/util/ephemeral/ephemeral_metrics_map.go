package ephemeral

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
)

// GetCodec returns the codec used to encode and decode data for KV client.
func GetCodec() codec.Codec {
	return codec.NewProtoCodec("ephemeralMetrics", func() proto.Message {
		return NewMetrics()
	})
}

func NewMetrics() *Metrics {
	return &Metrics{Metrics: map[string]Metric{}}
}

func (m *Metrics) Merge(mergeable memberlist.Mergeable, _ bool) (_ memberlist.Mergeable, error error) {
	if mergeable == nil {
		return nil, nil
	}

	other, ok := mergeable.(*Metrics)
	if !ok {
		return nil, fmt.Errorf("unexpected type for merging *ephemeral.Metrics: %T", mergeable)
	}

	change := NewMetrics()

	// check for added metrics
	for n, e := range other.Metrics {
		normalizeMetric(&e)

		local, ok := m.Metrics[n]
		if !ok {
			m.Metrics[n] = e
			change.Metrics[n] = e
		} else {
			origLocal := local
			if e.CreatedTimestamp > local.CreatedTimestamp {
				local.CreatedTimestamp = e.CreatedTimestamp
			}
			if e.DeletedTimestamp > local.DeletedTimestamp {
				local.DeletedTimestamp = e.DeletedTimestamp
			}
			normalizeMetric(&local)
			if local != origLocal {
				m.Metrics[n] = local
				change.Metrics[n] = local
			}
		}
	}

	if len(change.Metrics) == 0 {
		return nil, nil
	}
	return change, nil
}

func (m *Metrics) MergeContent() []string {
	result := make([]string, 0, len(m.Metrics))
	for n := range m.Metrics {
		result = append(result, n)
	}
	return result
}

func (m *Metrics) RemoveTombstones(limit time.Time) (total, removed int) {
	unixSeconds := limit.Unix()
	for n, v := range m.Metrics {
		total++
		if v.GetDeletedTimestamp() > unixSeconds {
			removed++
			delete(m.Metrics, n)
		}
	}
	return total, removed
}

func (m *Metrics) Clone() memberlist.Mergeable {
	result := NewMetrics()
	for n, e := range m.Metrics {
		result.Metrics[n] = e
	}
	return result
}

func normalizeMetric(m *Metric) {
	if m.CreatedTimestamp > m.DeletedTimestamp {
		m.DeletedTimestamp = 0
	}
	if m.DeletedTimestamp > m.CreatedTimestamp {
		m.CreatedTimestamp = 0
	}
	if m.CreatedTimestamp == m.DeletedTimestamp && m.CreatedTimestamp > 0 {
		// "created" wins.
		m.DeletedTimestamp = 0
	}
}

func (m *Metrics) IsEphemeral(name string) bool {
	e := m.Metrics[name]
	if e.CreatedTimestamp > e.DeletedTimestamp {
		return true
	}
	return false
}

func (m *Metrics) EphemeralMetrics() []string {
	result := make([]string, 0, len(m.Metrics))
	for n, e := range m.Metrics {
		if e.CreatedTimestamp > e.DeletedTimestamp {
			result = append(result, n)
		}
	}
	return result
}

func (m *Metrics) AddEphemeral(name string) {
	m.addEphemeral(name, time.Now())
}

func (m *Metrics) addEphemeral(name string, now time.Time) {
	prev := m.Metrics[name]
	if prev.CreatedTimestamp == 0 || prev.CreatedTimestamp < prev.DeletedTimestamp {
		m.Metrics[name] = Metric{CreatedTimestamp: now.Unix()}
	}
}

func (m *Metrics) RemoveEphemeral(name string) {
	m.removeEphemeral(name, time.Now())
}

func (m *Metrics) removeEphemeral(name string, now time.Time) {
	e := m.Metrics[name]
	if e.CreatedTimestamp > e.DeletedTimestamp {
		e.CreatedTimestamp = 0
		e.DeletedTimestamp = now.Unix()
		m.Metrics[name] = e
	}
}
