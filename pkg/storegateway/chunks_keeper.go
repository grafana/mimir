package storegateway

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

// chunksKeeper provisions and reclaims chunks and the underlying byte slices for their data.
type chunksKeeper struct {
	mapMtx  sync.Mutex
	pool    pool.Bytes
	tracker map[*storepb.Chunk]*[]byte
}

func newChunksKeeper(pool pool.Bytes) *chunksKeeper {
	return &chunksKeeper{
		pool:    pool,
		tracker: map[*storepb.Chunk]*[]byte{},
	}
}

func (k *chunksKeeper) Reclaim(chunk *storepb.Chunk) error {
	k.mapMtx.Lock()
	pooledBytes, ok := k.tracker[chunk]
	delete(k.tracker, chunk)
	k.mapMtx.Unlock()

	if !ok {
		// This should never happen. But just in case we return an error.
		return errors.Errorf("reclaim chunk: not found")
	}
	k.pool.Put(pooledBytes)
	return nil
}

func (k *chunksKeeper) ReclaimAll() {
	k.mapMtx.Lock()
	defer k.mapMtx.Unlock()

	for chunk, pooledBytes := range k.tracker {
		k.pool.Put(pooledBytes)
		delete(k.tracker, chunk)
	}
}

func (k *chunksKeeper) NewChunk(in chunkenc.Chunk) (*storepb.Chunk, error) {
	if in.Encoding() != chunkenc.EncXOR {
		return nil, errors.Errorf("unsupported chunk encoding %d", in.Encoding())
	}
	out := &storepb.Chunk{}
	b, err := k.save(out, in.Bytes())
	if err != nil {
		return nil, err
	}
	out.Data = b
	out.Type = storepb.Chunk_XOR
	return out, nil
}

func (k *chunksKeeper) save(chunkID *storepb.Chunk, toSave []byte) ([]byte, error) {
	toSaveLen := len(toSave)
	pooledBytes, err := k.pool.Get(toSaveLen)
	if err != nil {
		return nil, errors.Wrap(err, "allocate chunk bytes")
	}

	k.mapMtx.Lock()
	k.tracker[chunkID] = pooledBytes
	k.mapMtx.Unlock()

	saved := (*pooledBytes)[:toSaveLen]
	copy(saved, toSave)
	return saved, nil
}

type chunksReclaimer interface {
	Reclaim(chunk *storepb.Chunk) error
	ReclaimAll()
}

// reclaimingChunksSeriesSet uses a reclaimer to reclaim each series' chunks after Next is called.
// The client should not keep references to the label set or the chunks after calling Next
type reclaimingChunksSeriesSet struct {
	delegate  storepb.SeriesSet
	reclaimer chunksReclaimer

	toReclaim []*storepb.Chunk
	lastErr   error
}

func newReclaimingSeriesSet(delegate storepb.SeriesSet, reclaimer chunksReclaimer) *reclaimingChunksSeriesSet {
	return &reclaimingChunksSeriesSet{
		delegate:  delegate,
		reclaimer: reclaimer,
	}
}

func (r *reclaimingChunksSeriesSet) Next() bool {
	if len(r.toReclaim) > 0 {
		for _, c := range r.toReclaim {
			err := r.reclaimer.Reclaim(c)
			if err != nil {
				r.lastErr = err
			}
		}
		r.toReclaim = r.toReclaim[:0]
	}

	hasNext := r.delegate.Next()

	if hasNext {
		_, chunks := r.delegate.At()
		for _, c := range chunks {
			r.toReclaim = append(r.toReclaim, c.Raw)
		}
	}

	return hasNext
}

func (r *reclaimingChunksSeriesSet) At() (labels.Labels, []storepb.AggrChunk) {
	return r.delegate.At()
}

func (r *reclaimingChunksSeriesSet) Err() error {
	if r.lastErr != nil {
		return r.lastErr
	}
	return r.delegate.Err()
}
