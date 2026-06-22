package wgo

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// kerrNoError is the Kafka ErrorCode value that signals "no error".
const kerrNoError int16 = 0

var (
	errEmptyProduceResult = errors.New("empty ProduceResult: both resp and err are nil")
)

// ProduceResult carries the outcome of a produce request.
type ProduceResult struct {
	resp *kmsg.ProduceResponse
	err  error
}

// succeeded reports whether ProduceResult is fully-successful.
func (r ProduceResult) succeeded() bool {
	// error() is the single predicate: it returns nil only for a fully-successful result
	// (an empty result is not).
	return r.error() == nil
}

// error returns the error to surface for a failed produce result.
// Returns nil only for a fully-successful result; an empty result (both
// fields nil) returns errEmptyProduceResult.
func (r ProduceResult) error() error {
	// Transport-level errors win.
	if r.err != nil {
		return r.err
	}
	if r.resp == nil {
		return errEmptyProduceResult
	}
	// The first non-zero per-partition ErrorCode, if any.
	return parseProduceResponse(r.resp)
}

// produceResultErr is the classification of a produce error.
type produceResultErr struct {
	// The original error.
	err error

	// Low-cardinality and stable reason label for metrics.
	reason string

	// Whether this error should be considered as retriable.
	retriable bool

	// Kafka error code to use. Guaranteed to always be non-zero.
	code int16
}

// getProduceResultErr classifies err into a produceResultErr.
func getProduceResultErr(err error) produceResultErr {
	switch {
	case errors.Is(err, context.Canceled):
		return produceResultErr{err: err, reason: "canceled", retriable: false, code: kerr.RequestTimedOut.Code}
	case errors.Is(err, context.DeadlineExceeded):
		return produceResultErr{err: err, reason: "timeout", retriable: true, code: kerr.RequestTimedOut.Code}
	case kerr.IsRetriable(err):
		code := kerr.UnknownServerError.Code
		var kerrErr *kerr.Error
		if errors.As(err, &kerrErr) {
			code = kerrErr.Code
		}
		return produceResultErr{err: err, reason: "kafka_retriable_error", retriable: true, code: code}
	case kgo.IsRetryableBrokerErr(err):
		return produceResultErr{err: err, reason: "transport", retriable: true, code: kerr.UnknownServerError.Code}
	}
	// Distinguish a known-but-non-retriable kerr error (MessageTooLarge,
	// auth, etc.) from anything else. Unknown errors stay non-retriable: we
	// don't know whether retrying is safe, and the conservative choice is to
	// surface the error rather than spin.
	if kerrErr := (*kerr.Error)(nil); errors.As(err, &kerrErr) {
		return produceResultErr{err: err, reason: "kafka_non_retriable_error", retriable: false, code: kerrErr.Code}
	}
	return produceResultErr{err: err, reason: "unknown", retriable: false, code: kerr.UnknownServerError.Code}
}

// produceResultAccumulator merges the outcomes of one or more produce
// attempts into a single merged response, tracking which topic-
// partitions still need work and whether the caller should abort.
//
// Warpstream-specific assumption: a single Produce request to one agent
// is all-or-nothing. accumulate() treats any response with at least one
// per-partition error as a whole-call failure — successful per-partition
// entries inside such a response are dropped and re-produced on retry.
//
// This Warpstream client has been designed for use cases where duplicated
// produced records are tolerated.
type produceResultAccumulator struct {
	mu       sync.Mutex
	pending  map[topicPartition][]*kgo.Record
	resolved map[topicPartition]kmsg.ProduceResponseTopicPartition
	// failed holds the most recent per-partition entry with a non-zero
	// ErrorCode for partitions still in pending. response() prefers
	// these over a synthesized code so the caller sees the actual error
	// each agent reported. An entry is dropped when the partition is
	// later resolved (success supersedes prior failure).
	failed map[topicPartition]kmsg.ProduceResponseTopicPartition

	lastErr produceResultErr
	aborted bool

	responseVersion        int16
	responseThrottleMillis int32
	responseTopicIDs       map[string][16]byte
}

// newProduceResultAccumulator returns an accumulator seeded with the
// topic-partitions to produce. Duplicate (topic, partition) keys return
// an error.
func newProduceResultAccumulator(partitions []topicPartitionRecords) (*produceResultAccumulator, error) {
	a := &produceResultAccumulator{
		pending:          make(map[topicPartition][]*kgo.Record, len(partitions)),
		resolved:         make(map[topicPartition]kmsg.ProduceResponseTopicPartition),
		failed:           make(map[topicPartition]kmsg.ProduceResponseTopicPartition),
		responseTopicIDs: make(map[string][16]byte),
	}
	for _, p := range partitions {
		key := topicPartition{topic: p.topic, partition: p.partition}
		if _, exists := a.pending[key]; exists {
			// Reject duplicates: the merge logic assumes one record set per key.
			return nil, fmt.Errorf("duplicate topic-partition in input: topic=%q partition=%d", p.topic, p.partition)
		}
		a.pending[key] = p.records
	}
	return a, nil
}

// remaining returns the partitions still pending.
func (a *produceResultAccumulator) remaining() []topicPartitionRecords {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]topicPartitionRecords, 0, len(a.pending))
	for tp, records := range a.pending {
		out = append(out, topicPartitionRecords{
			topic:     tp.topic,
			partition: tp.partition,
			records:   records,
		})
	}
	return out
}

// accumulate folds one produce attempt's outcome into the merged state.
func (a *produceResultAccumulator) accumulate(res ProduceResult) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !res.succeeded() {
		// On failure nothing is resolved (whole-call failure under the
		// all-or-nothing assumption); record the err and abort when
		// it's non-retriable so the caller can stop retrying.
		a.lastErr = getProduceResultErr(res.error())
		if !a.lastErr.retriable {
			a.aborted = true
		}
		// Capture per-partition error entries so response() can surface
		// the actual error each agent reported for partitions that end
		// up exhausting their retry budget.
		if res.resp != nil {
			for _, t := range res.resp.Topics {
				for _, entry := range t.Partitions {
					if entry.ErrorCode == kerrNoError {
						continue
					}
					tp := topicPartition{topic: t.Topic, partition: entry.Partition}
					// Only track partitions we're still waiting on; a stray
					// partition the broker returned but we never requested
					// would never resolve, so it must not enter failed.
					if _, ok := a.pending[tp]; !ok {
						continue
					}
					a.failed[tp] = entry
				}
			}
		}
		return
	}

	// Capture wire metadata once. Version is identical across calls;
	// ThrottleMillis is the max so the most-throttling agent wins. The value
	// is propagated to the caller but intentionally not enforced here: this
	// client steers load via hedging/demotion, not by sleeping on a
	// broker-requested throttle.
	if a.responseVersion == 0 {
		a.responseVersion = res.resp.Version
	}
	a.responseThrottleMillis = max(a.responseThrottleMillis, res.resp.ThrottleMillis)
	for _, t := range res.resp.Topics {
		if t.TopicID == ([16]byte{}) {
			continue
		}
		if _, ok := a.responseTopicIDs[t.Topic]; !ok {
			a.responseTopicIDs[t.Topic] = t.TopicID
		}
	}

	// Resolve every partition in the response. A success also drops
	// any prior failed entry — success supersedes a previously-
	// reported failure.
	for _, t := range res.resp.Topics {
		for _, entry := range t.Partitions {
			tp := topicPartition{topic: t.Topic, partition: entry.Partition}
			a.resolved[tp] = entry
			delete(a.pending, tp)
			delete(a.failed, tp)
		}
	}
}

// done reports whether the caller should stop retrying.
func (a *produceResultAccumulator) done() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.pending) == 0 || a.aborted
}

// result builds the final ProduceResult that the caller should surface.
func (a *produceResultAccumulator) result() ProduceResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	resp := a.responseLocked()

	if len(a.pending) == 0 {
		// Every partition has a terminal entry. Per-partition codes in
		// resp carry any per-partition errors; the leg overall succeeded
		// enough to not need a retry-exhausted envelope.
		return ProduceResult{resp: resp}
	}

	if a.lastErr.err != nil {
		// A leg reported an error and partitions remain pending: surface
		// the specific kerr, wrapped in kgo.ErrRecordTimeout, because we
		// can consider it like "response not received in time".
		return ProduceResult{resp: resp, err: fmt.Errorf("%w: %w", kgo.ErrRecordTimeout, a.lastErr.err)}
	}

	// Pending partitions but no failure was observed (e.g. partial
	// response coverage with successful per-partition codes): bare
	// retry-exhausted envelope.
	return ProduceResult{resp: resp, err: kgo.ErrRecordTimeout}
}

// responseLocked merges every accumulated outcome into a single
// response. Caller must hold a.mu.
func (a *produceResultAccumulator) responseLocked() *kmsg.ProduceResponse {
	merged := &kmsg.ProduceResponse{
		Version:        a.responseVersion,
		ThrottleMillis: a.responseThrottleMillis,
	}
	topicIdx := map[string]int{}
	getOrAddTopic := func(topic string) int {
		if idx, ok := topicIdx[topic]; ok {
			return idx
		}
		idx := len(merged.Topics)
		t := kmsg.ProduceResponseTopic{Topic: topic}
		if id, ok := a.responseTopicIDs[topic]; ok {
			t.TopicID = id
		}
		merged.Topics = append(merged.Topics, t)
		topicIdx[topic] = idx
		return idx
	}

	for tp, entry := range a.resolved {
		idx := getOrAddTopic(tp.topic)
		merged.Topics[idx].Partitions = append(merged.Topics[idx].Partitions, entry)
	}

	// Pending entries: prefer the most-recent per-partition entry we
	// recorded from a failed leg so callers see the actual error each
	// agent reported. Fall back to a synthesized code when we never
	// saw a per-partition entry: use the most recent leg's kerr code
	// (preserving the specific failure through perPartitionDone) when
	// any failure was observed; otherwise REQUEST_TIMED_OUT, which
	// perPartitionDone wraps into kgo.ErrRecordTimeout (the writer's
	// retry-exhausted signal).
	syntheticCode := kerr.RequestTimedOut.Code
	if a.lastErr.code != 0 {
		syntheticCode = a.lastErr.code
	}
	for tp := range a.pending {
		idx := getOrAddTopic(tp.topic)
		entry, ok := a.failed[tp]
		if !ok {
			entry = kmsg.ProduceResponseTopicPartition{
				Partition: tp.partition,
				ErrorCode: syntheticCode,
			}
		}
		merged.Topics[idx].Partitions = append(merged.Topics[idx].Partitions, entry)
	}

	return merged
}

// selectProduceResult picks which produce outcome ProduceSync should surface.
func selectProduceResult(primary, fallback ProduceResult) ProduceResult {
	if primary.succeeded() {
		return primary
	}
	if fallback.err == nil {
		return fallback
	}
	if primary.err != nil {
		// Both transport-errored. Prefer fallback's resp: it encodes the
		// accumulator's per-partition view (strictly richer than
		// primary's resp once primary has failed transport-level).
		return ProduceResult{
			resp: fallback.resp,
			err:  fmt.Errorf("%w: %w", primary.err, fallback.err),
		}
	}
	if fallback.resp != nil {
		// Primary failed via per-partition codes only (no transport err), so it
		// resolved nothing under the all-or-nothing leg model. The fallback
		// merges produce attempts across multiple agents, so its response can
		// carry partitions a hedge leg landed even alongside its error — that
		// richer view wins.
		return fallback
	}
	return primary
}
