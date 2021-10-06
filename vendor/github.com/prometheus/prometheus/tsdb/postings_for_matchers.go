package tsdb

import (
	"container/list"
	"strings"
	"sync"
	"time"

	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

const defaultPostingsForMatchersCacheTTL = 10 * time.Second

// IndexForPostings is a subset of IndexReader methods, the minimum required to evaluate PostingsForMatchers
type IndexForPostings interface {
	// LabelValues returns possible label values which may not be sorted.
	LabelValues(name string, matchers ...*labels.Matcher) ([]string, error)

	// Postings returns the postings list iterator for the label pairs.
	// The Postings here contain the offsets to the series inside the index.
	// Found IDs are not strictly required to point to a valid Series, e.g.
	// during background garbage collections. Input values must be sorted.
	Postings(name string, values ...string) (index.Postings, error)
}

// PostingsForMatchersProvider provides a PostingsForMatcher method implementations, some of them might just call
// PostingsForMatchers, others might deduplicate concurrent calls, or even cache the results.
type PostingsForMatchersProvider interface {
	// PostingsForMatchers assembles a single postings iterator based on the  given matchers.
	// The resulting postings are not ordered by series.
	// If concurrent is set to true, call will be optimized for a (most likely) concurrent call with same matchers,
	// avoiding same calculations twice, however this implementation may lead to a worse performance when called once.
	PostingsForMatchers(concurrent bool, ms ...*labels.Matcher) (index.Postings, error)
	// Close stops the cache expiration goroutine.
	Close() error
}

// NewPostingsForMatchersProvider creates a new builder for PostingsForMatchersProviderImpl.
func NewPostingsForMatchersProvider(ttl time.Duration) *PostingsForMatchersProviderBuilder {
	b := &PostingsForMatchersProviderBuilder{
		calls:  &sync.Map{},
		cached: list.New(),
		ttl:    ttl,

		close:  make(chan struct{}),
		closed: sync.WaitGroup{},
	}
	if ttl > 0 {
		b.closed.Add(1)
		go func() {
			defer b.closed.Done()
			ticker := time.NewTicker(ttl)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					b.expire()
				case <-b.close:
					return
				}
			}
		}()
	}

	return b
}

// PostingsForMatchersProviderBuilder builds PostingsForMatchersProviderImpl that all share the same concurrent calls
// when the concurrent hint is passed in.
type PostingsForMatchersProviderBuilder struct {
	calls *sync.Map

	cachedMtx sync.RWMutex
	cached    *list.List

	ttl time.Duration

	once   sync.Once
	close  chan struct{}
	closed sync.WaitGroup
}

// WithIndex creates a PostingsForMatchersProvider for a provided index.
// This should be called always for the same underlying block (like different head ranges calls).
// Each block should instantiate its own builder, otherwise PostingsForMatchers would mix postings from different blocks.
func (b *PostingsForMatchersProviderBuilder) WithIndex(ifp IndexForPostings) PostingsForMatchersProviderImpl {
	return PostingsForMatchersProviderImpl{
		PostingsForMatchersProviderBuilder: b,
		indexForPostings:                   ifp,
		postingsForMatchers:                PostingsForMatchers,
	}
}

// Close stops the cache expiration goroutine. It shouldn't be called twice.
func (b *PostingsForMatchersProviderBuilder) Close() error {
	b.once.Do(func() {
		close(b.close)
	})
	b.closed.Wait()
	return nil
}

type PostingsForMatchersProviderImpl struct {
	*PostingsForMatchersProviderBuilder

	indexForPostings    IndexForPostings
	postingsForMatchers func(ix IndexForPostings, ms ...*labels.Matcher) (index.Postings, error)
}

func (p PostingsForMatchersProviderImpl) PostingsForMatchers(concurrent bool, ms ...*labels.Matcher) (index.Postings, error) {
	if !concurrent {
		return p.postingsForMatchers(p.indexForPostings, ms...)
	}

	p.expire()

	type postingsForMatchersPromise struct {
		sync.Once
		sync.WaitGroup

		postings *index.PostingsCloner
		err      error
	}

	promise := &postingsForMatchersPromise{}
	promise.Add(1)
	key := matchersKey(ms)

	wi, loaded := p.calls.LoadOrStore(key, promise)
	if loaded {
		promise = wi.(*postingsForMatchersPromise)
	} else {
		p.created(key)
	}

	promise.Do(func() {
		defer promise.Done()
		postings, err := p.postingsForMatchers(p.indexForPostings, ms...)
		if err != nil {
			promise.err = err
			return
		}
		promise.postings = index.NewPostingsCloner(postings)
	})
	promise.Wait()

	if promise.err != nil {
		return nil, promise.err
	}

	return promise.postings.Clone(), nil
}

type postingsForMatchersCachedCall struct {
	key string
	ts  time.Time
}

func (b *PostingsForMatchersProviderBuilder) expire() {
	if b.ttl <= 0 {
		return
	}

	b.cachedMtx.RLock()
	if !b.headExpired() {
		b.cachedMtx.RUnlock()
		return
	}
	b.cachedMtx.RUnlock()

	b.cachedMtx.Lock()
	defer b.cachedMtx.Unlock()

	for b.headExpired() {
		front := b.cached.Front()
		oldest := front.Value.(*postingsForMatchersCachedCall)
		b.calls.Delete(oldest.key)
		b.cached.Remove(front)
	}
}

func (b *PostingsForMatchersProviderBuilder) headExpired() bool {
	h := b.cached.Front()
	return h != nil && time.Since(h.Value.(*postingsForMatchersCachedCall).ts) >= b.ttl
}

func (b *PostingsForMatchersProviderBuilder) created(key string) {
	if b.ttl <= 0 {
		return
	}

	b.cachedMtx.Lock()
	defer b.cachedMtx.Unlock()

	b.cached.PushBack(&postingsForMatchersCachedCall{
		key: key,
		ts:  time.Now(),
	})
}

// matchersKey provides a unique string key for the given matchers slice
// NOTE: different orders of matchers will produce different keys,
// but it's unlikely that we'll receive same matchers in different orders at the same time
func matchersKey(ms []*labels.Matcher) string {
	const (
		typeLen = 2
		sepLen  = 1
	)
	var size int
	for _, m := range ms {
		size += len(m.Name) + len(m.Value) + typeLen + sepLen
	}
	sb := strings.Builder{}
	sb.Grow(size)
	for _, m := range ms {
		sb.WriteString(m.Name)
		sb.WriteString(m.Type.String())
		sb.WriteString(m.Value)
		sb.WriteByte(0)
	}
	key := sb.String()
	return key
}

// indexReaderWithPostingsForMatchers adapts an index.Reader to be an IndexReader by adding the PostingsForMatchers method
type indexReaderWithPostingsForMatchers struct {
	*index.Reader
	PostingsForMatchersProvider
}

func (ir indexReaderWithPostingsForMatchers) Close() error {
	return tsdb_errors.NewMulti(
		ir.Reader.Close(),
		ir.PostingsForMatchersProvider.Close(),
	).Err()
}

var _ IndexReader = indexReaderWithPostingsForMatchers{}
