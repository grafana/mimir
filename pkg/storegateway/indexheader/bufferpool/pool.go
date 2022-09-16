// SPDX-License-Identifier: AGPL-3.0-only

package bufferpool

import (
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	errNoBufferPages = errors.New("no buffer pages available")
)

type pageIndex struct {
	fileName string
	idx      int
}

type bufferPage struct {
	pool     *Pool
	idx      pageIndex
	size     int
	refCount int32
	loadedCh chan struct{}
	b        []byte
}

func (p *bufferPage) load(f *os.File, idx int, pageSize int) error {
	defer close(p.loadedCh)

	offset := int64(idx * pageSize)
	_, err := f.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	n, err := f.Read(p.b)
	if err != nil {
		return err
	}
	p.size = n
	return nil
}

func (p *bufferPage) bytes() []byte {
	// Wait for available content.
	<-p.loadedCh
	return p.b[:p.size]
}

func (p *bufferPage) pin() {
	if atomic.AddInt32(&p.refCount, 1) == 1 {
		p.pool.unmarkForEviction(p)
	}
}

func (p *bufferPage) unpin() {
	if atomic.AddInt32(&p.refCount, -1) == 0 {
		p.pool.markForEviction(p)
	}
}

// PageSet is a set of buffer pages.
type PageSet interface {
	// Next moves the set iterator to the next page.
	Next() bool

	// At returns the current page bytes.
	At() []byte

	// Size returns set total number of pages.
	Size() int

	// Close decrements reference count value for all set retained pages,
	// allowing them to be reclaimed by the pool on future evictions.
	Close()
}

type pageSet struct {
	nextIdx int
	pages   []*bufferPage
}

func (ps pageSet) Next() bool {
	if ps.nextIdx >= len(ps.pages) {
		return false
	}
	if ps.nextIdx > -1 {
		// Release previous retained page.
		ps.pages[ps.nextIdx].unpin()
	}
	ps.nextIdx++
	return true
}

func (ps pageSet) At() []byte {
	if ps.nextIdx == -1 || ps.nextIdx >= len(ps.pages) {
		return nil
	}
	return ps.pages[ps.nextIdx].bytes()
}

func (ps pageSet) Size() int {
	return len(ps.pages)
}

func (ps pageSet) Close() {
	i := 0
	if ps.nextIdx > -1 {
		i = ps.nextIdx
	}
	for ; i < len(ps.pages); i++ {
		ps.pages[i].unpin()
	}
}

type poolMetrics struct {
	totalPages     prometheus.Gauge
	pagesInUse     prometheus.Gauge
	evictablePages prometheus.Gauge
	pageFaultCount prometheus.Counter
	evictionCount  prometheus.Counter
}

// Pool is a pool of buffer pages.
type Pool struct {
	pageSize        int
	mu              sync.Mutex
	inUse           map[pageIndex]*bufferPage
	evictCandidates map[pageIndex]*bufferPage
	freeList        [][]byte
	metrics         poolMetrics
}

// New creates a new Pool.
func New(reg prometheus.Registerer, numPages, pageSize int) *Pool {
	metrics := poolMetrics{
		totalPages: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "buffer_pool_total_pages",
			Help: "Total number of buffer pages in the pool.",
		}),
		pagesInUse: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "buffer_pool_pages_in_use",
			Help: "Number of buffer pages currently in use.",
		}),
		evictablePages: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "buffer_pool_evictable_pages",
			Help: "Number of buffer pages currently in use.",
		}),
		pageFaultCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "buffer_pool_page_faults_total",
			Help: "Total number of buffer page faults.",
		}),
		evictionCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "buffer_pool_evictions_total",
			Help: "Total number of buffer page evictions.",
		}),
	}
	p := &Pool{
		inUse:           make(map[pageIndex]*bufferPage, numPages),
		evictCandidates: make(map[pageIndex]*bufferPage, numPages),
		freeList:        make([][]byte, numPages),
		pageSize:        pageSize,
		metrics:         metrics,
	}
	// Set total pages metric.
	p.metrics.totalPages.Set(float64(numPages))

	// Populate free list.
	for i := 0; i < numPages; i++ {
		p.freeList[i] = make([]byte, pageSize)
	}
	return p
}

// LoadPages loads pages from the given file.
func (p *Pool) LoadPages(f *os.File, startIdx, numPages int) (PageSet, error) {
	pages := make([]*bufferPage, 0, numPages)

	lastPage := startIdx + numPages
	for i := startIdx; i < lastPage; i++ {
		// Lookup for in-memory page.
		p.mu.Lock()
		pageIdx := pageIndex{
			fileName: f.Name(),
			idx:      i,
		}
		pg := p.inUse[pageIdx]
		if pg != nil {
			p.mu.Unlock()
			pg.pin()
			pages = append(pages, pg)
			continue
		}
		// Page fault... pick an available buffer page...
		p.metrics.pageFaultCount.Inc()

		pg, err := p.pickBufferPage()
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		// Mark page as used.
		pg.idx = pageIndex{
			fileName: f.Name(),
			idx:      i,
		}
		p.inUse[pg.idx] = pg

		// Update used/evictable pages metrics.
		p.metrics.pagesInUse.Set(float64(len(p.inUse)))
		p.metrics.evictablePages.Set(float64(len(p.evictCandidates)))

		p.mu.Unlock()

		// Load page content.
		if err := pg.load(f, i, p.pageSize); err != nil {
			return nil, err
		}
		pages = append(pages, pg)
	}
	return &pageSet{nextIdx: -1, pages: pages}, nil
}

func (p *Pool) pickBufferPage() (*bufferPage, error) {
	numFreePages := len(p.freeList)
	if numFreePages == 0 {
		if len(p.evictCandidates) == 0 {
			return nil, errNoBufferPages
		}
		// Pick an evictable page and return it to the free list.
		var evictPage *bufferPage

		// TODO(ortuman): make use of an LRU list
		for _, pg := range p.evictCandidates {
			evictPage = pg
			break
		}
		delete(p.inUse, evictPage.idx)
		delete(p.evictCandidates, evictPage.idx)

		p.freeList = append(p.freeList, evictPage.b)
		numFreePages = 1

		// Update eviction count metric.
		p.metrics.evictionCount.Inc()
	}
	page := &bufferPage{
		pool:     p,
		loadedCh: make(chan struct{}),
		b:        p.freeList[numFreePages-1],
		refCount: 1,
	}
	p.freeList = p.freeList[:numFreePages-1]
	return page, nil
}

func (p *Pool) markForEviction(page *bufferPage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.inUse, page.idx)
	p.evictCandidates[page.idx] = page

	p.metrics.pagesInUse.Set(float64(len(p.inUse)))
	p.metrics.evictablePages.Set(float64(len(p.evictCandidates)))
}

func (p *Pool) unmarkForEviction(page *bufferPage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.evictCandidates, page.idx)
	p.inUse[page.idx] = page

	p.metrics.pagesInUse.Set(float64(len(p.inUse)))
	p.metrics.evictablePages.Set(float64(len(p.evictCandidates)))
}
