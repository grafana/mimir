// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"time"

	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util/version"
)

const (
	// offsetMarkerVersion1 is the version of the offset marker file. Like the block deletion and
	// no-compact marks, a marker with any other version is rejected.
	offsetMarkerVersion1 = 1

	// offsetMarkerKeyDir is the directory, inside bucket.MimirInternalsPrefix, holding scheduler markers.
	offsetMarkerKeyDir = "block-builder-scheduler"

	// offsetMarkerTimeout bounds each marker read or write. Like the bucket index, one minute is
	// hard-coded to protect against operations that can take unbounded time.
	offsetMarkerTimeout = time.Minute
)

// offsetMarker is the durable record of the scheduler's committed offsets, kept in object storage
// so that it survives expiry of the Kafka consumer group's offsets.
type offsetMarker struct {
	Version       int    `json:"version"`
	Topic         string `json:"topic"`
	ConsumerGroup string `json:"consumer_group"`
	// Clusters holds one entry per write-compartment cluster. Without compartments there is one, with ClusterID 0.
	Clusters     []offsetMarkerCluster `json:"clusters"`
	Timestamp    time.Time             `json:"timestamp"`
	MimirVersion string                `json:"mimir_version"`
}

type offsetMarkerCluster struct {
	ClusterID int `json:"cluster_id"`
	// KafkaClusterID and KafkaTopicID identify the Kafka cluster and topic the offsets belong to. They are
	// empty if the broker doesn't report them. A recreated topic gets a new ID, and Kafka drops the old
	// topic's offsets, so a mismatch tells us this entry's offsets are for a topic that no longer exists.
	KafkaClusterID string `json:"kafka_cluster_id,omitempty"`
	KafkaTopicID   string `json:"kafka_topic_id,omitempty"`
	// Offsets maps partition ID to the committed offset.
	Offsets map[int32]int64 `json:"offsets"`
}

// clusterIdentity identifies a Kafka cluster and topic. An empty field is unknown.
type clusterIdentity struct {
	kafkaClusterID string
	kafkaTopicID   string
}

// conflict returns the name of the first ID that is known on both sides and differs, or "" if none.
// An unknown ID (a broker that doesn't support it) never conflicts.
func (a clusterIdentity) conflict(b clusterIdentity) string {
	if a.kafkaClusterID != "" && b.kafkaClusterID != "" && a.kafkaClusterID != b.kafkaClusterID {
		return "kafka_cluster_id"
	}
	if a.kafkaTopicID != "" && b.kafkaTopicID != "" && a.kafkaTopicID != b.kafkaTopicID {
		return "kafka_topic_id"
	}
	return ""
}

// markerOffsets holds committed offsets indexed by cluster ID, then partition.
type markerOffsets []map[int32]int64

func (m markerOffsets) equal(o markerOffsets) bool {
	if len(m) != len(o) {
		return false
	}
	for i := range m {
		if len(m[i]) != len(o[i]) {
			return false
		}
		for p, off := range m[i] {
			if oo, ok := o[i][p]; !ok || oo != off {
				return false
			}
		}
	}
	return true
}

func (m markerOffsets) empty() bool {
	for _, c := range m {
		if len(c) > 0 {
			return false
		}
	}
	return true
}

func (m markerOffsets) clone() markerOffsets {
	c := make(markerOffsets, len(m))
	for i := range m {
		c[i] = make(map[int32]int64, len(m[i]))
		for p, o := range m[i] {
			c[i][p] = o
		}
	}
	return c
}

// newOffsetMarker builds a marker from the given offsets. ids, if not nil, holds each cluster's identity.
func newOffsetMarker(topic, consumerGroup string, offsets markerOffsets, ids []clusterIdentity, now time.Time) *offsetMarker {
	m := &offsetMarker{
		Version:       offsetMarkerVersion1,
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		Timestamp:     now.UTC(),
		MimirVersion:  version.Version,
	}
	for clusterID, offs := range offsets {
		c := offsetMarkerCluster{ClusterID: clusterID, Offsets: offs}
		if clusterID < len(ids) {
			c.KafkaClusterID = ids[clusterID].kafkaClusterID
			c.KafkaTopicID = ids[clusterID].kafkaTopicID
		}
		m.Clusters = append(m.Clusters, c)
	}
	return m
}

// cluster returns the marker's entry for a cluster, or nil if it has none.
func (m *offsetMarker) cluster(clusterID int) *offsetMarkerCluster {
	for i := range m.Clusters {
		if m.Clusters[i].ClusterID == clusterID {
			return &m.Clusters[i]
		}
	}
	return nil
}

// offsetsForCluster returns the marker's offsets for a cluster, or nil if it has none.
func (m *offsetMarker) offsetsForCluster(clusterID int) map[int32]int64 {
	if c := m.cluster(clusterID); c != nil {
		return c.Offsets
	}
	return nil
}

func (m *offsetMarker) encode() ([]byte, error) {
	// encoding/json writes map keys in sorted order, so the output is deterministic.
	sorted := *m
	sorted.Clusters = slices.Clone(m.Clusters)
	sort.Slice(sorted.Clusters, func(i, j int) bool { return sorted.Clusters[i].ClusterID < sorted.Clusters[j].ClusterID })
	return json.Marshal(&sorted)
}

func decodeOffsetMarker(data []byte) (*offsetMarker, error) {
	var m offsetMarker
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("decode offset marker: %w", err)
	}
	if m.Version != offsetMarkerVersion1 {
		return nil, fmt.Errorf("unexpected offset marker version %d, expected %d", m.Version, offsetMarkerVersion1)
	}
	return &m, nil
}

// offsetMarkerStore reads and writes the scheduler's marker object.
type offsetMarkerStore struct {
	bkt     objstore.Bucket
	key     string
	timeout time.Duration
}

// newOffsetMarkerStore returns a store that keeps the marker for the topic and consumer group under
// bucket.MimirInternalsPrefix. The topic is part of the key so that schedulers for different topics that
// share a bucket and a consumer group don't overwrite each other. bkt is the root blocks-storage bucket.
func newOffsetMarkerStore(bkt objstore.Bucket, topic, consumerGroup string) *offsetMarkerStore {
	return &offsetMarkerStore{
		bkt:     bucket.NewPrefixedBucketClient(bkt, bucket.MimirInternalsPrefix),
		key:     offsetMarkerKeyDir + objstore.DirDelim + url.PathEscape(topic) + objstore.DirDelim + url.PathEscape(consumerGroup) + ".json",
		timeout: offsetMarkerTimeout,
	}
}

// write uploads the marker, bounded by the store's timeout.
func (st *offsetMarkerStore) write(ctx context.Context, m *offsetMarker) error {
	data, err := m.encode()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, st.timeout)
	defer cancel()
	return st.bkt.Upload(ctx, st.key, bytes.NewReader(data))
}

// read returns the stored marker, or nil if there is none.
func (st *offsetMarkerStore) read(ctx context.Context) (*offsetMarker, error) {
	ctx, cancel := context.WithTimeout(ctx, st.timeout)
	defer cancel()

	r, err := st.bkt.Get(ctx, st.key)
	if err != nil {
		if st.bkt.IsObjNotFoundErr(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get offset marker: %w", err)
	}
	defer r.Close()

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		if st.bkt.IsObjNotFoundErr(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read offset marker: %w", err)
	}
	return decodeOffsetMarker(buf.Bytes())
}

// markerWriteGate decides whether the marker needs to be (re)written: when the offsets changed since
// the last successful write, when refreshInterval has passed since it, or when forced. A write that
// failed leaves the gate untouched, so the next attempt tries again.
type markerWriteGate struct {
	refreshInterval time.Duration

	written     bool
	lastOffsets markerOffsets
	lastWrite   time.Time
}

func (g *markerWriteGate) shouldWrite(offsets markerOffsets, now time.Time, force bool) bool {
	if force || !g.written {
		return true
	}
	if now.Sub(g.lastWrite) >= g.refreshInterval {
		return true
	}
	return !g.lastOffsets.equal(offsets)
}

func (g *markerWriteGate) recordWrite(offsets markerOffsets, now time.Time) {
	g.written = true
	g.lastOffsets = offsets.clone()
	g.lastWrite = now
}

// mergeMarkerOffsets returns, for each partition, the later of its offset in current and in base.
// Both only hold offsets of completed jobs, so the marker never moves a partition backwards. A partition that
// is absent from current, or behind base, means this scheduler lost track of it (e.g. Kafka lost the commit and
// it resumed from lookback-on-no-commit), not that the work was undone, so the recorded offset is kept: it is
// the only record of how far the scheduler really got.
// A base entry whose Kafka identity conflicts with ids is for a different topic incarnation, so is not carried.
func mergeMarkerOffsets(current markerOffsets, base *offsetMarker, ids []clusterIdentity) markerOffsets {
	out := current.clone()
	if base == nil {
		return out
	}
	for clusterID := range out {
		entry := base.cluster(clusterID)
		if entry == nil {
			continue
		}
		if clusterID < len(ids) && ids[clusterID].conflict(clusterIdentity{entry.KafkaClusterID, entry.KafkaTopicID}) != "" {
			continue
		}
		for p, o := range entry.Offsets {
			if cur, ok := out[clusterID][p]; !ok || o > cur {
				out[clusterID][p] = o
			}
		}
	}
	return out
}

const (
	// The marker is ahead of, or behind, the Kafka commit. A marker behind Kafka is expected if marker writes lag
	// or fail, as they never block the Kafka commit.
	mismatchMarkerAhead     = "marker_ahead"
	mismatchMarkerBehind    = "marker_behind"
	mismatchMissingInKafka  = "missing_in_kafka"
	mismatchMissingInMarker = "missing_in_marker"
)

var mismatchReasons = []string{mismatchMarkerAhead, mismatchMarkerBehind, mismatchMissingInKafka, mismatchMissingInMarker}

// compareMarkerToKafka counts, per reason, the partitions of the topic whose offset in the marker disagrees
// with the Kafka commit. marker may be nil, which is no disagreement: there is no marker yet. It is only a
// report: the scheduler never starts from the marker's offsets.
func compareMarkerToKafka(topic string, marker map[int32]int64, kafka kadm.Offsets) map[string]int {
	mismatches := map[string]int{}
	if marker == nil {
		return mismatches
	}
	for p, mo := range marker {
		ko, inKafka := kafka.Lookup(topic, p)
		switch {
		case !inKafka:
			mismatches[mismatchMissingInKafka]++
		case mo > ko.At:
			mismatches[mismatchMarkerAhead]++
		case mo < ko.At:
			mismatches[mismatchMarkerBehind]++
		}
	}
	kafka.Each(func(o kadm.Offset) {
		if _, inMarker := marker[o.Partition]; o.Topic == topic && !inMarker {
			mismatches[mismatchMissingInMarker]++
		}
	})
	return mismatches
}

// markerOffsetsMissingInKafka returns the marker's offset for each partition of the topic that has no Kafka
// commit. When the scheduler resumed such a partition from lookback-on-no-commit beyond this offset, the
// records in between were not built into blocks, and the marker is the only record of where they start.
func markerOffsetsMissingInKafka(topic string, marker map[int32]int64, kafka kadm.Offsets) map[int32]int64 {
	missing := map[int32]int64{}
	for p, mo := range marker {
		if _, inKafka := kafka.Lookup(topic, p); !inKafka {
			missing[p] = mo
		}
	}
	return missing
}

// validateMarkerIdentity returns an error if the marker belongs to a different topic or consumer group.
func validateMarkerIdentity(m *offsetMarker, topic, consumerGroup string) error {
	if m.Topic != topic || m.ConsumerGroup != consumerGroup {
		return fmt.Errorf("offset marker is for topic %q and group %q, expected %q and %q", m.Topic, m.ConsumerGroup, topic, consumerGroup)
	}
	return nil
}
