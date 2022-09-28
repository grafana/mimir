package tsdb

import (
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
)

// IntersectTwoPostings returns a new index.Postings resulting from the intersection of the two provided.
// It's more efficient than calling a generic index.Intersect when there are only two postings available.
func IntersectTwoPostings(a, b index.Postings) index.Postings {
	if a == index.EmptyPostings() || b == index.EmptyPostings() {
		return index.EmptyPostings()
	}

	return &intersectTwoPostings{a: a, b: b}
}

type intersectTwoPostings struct {
	a, b index.Postings
	cur  storage.SeriesRef
}

func (itp *intersectTwoPostings) Err() error {
	if itp.a.Err() != nil {
		return itp.a.Err()
	}
	return itp.b.Err()
}

func (itp *intersectTwoPostings) At() storage.SeriesRef {
	return itp.cur
}

func (itp *intersectTwoPostings) Next() bool {
	if !itp.a.Next() {
		return false
	}
	if !itp.b.Next() {
		return false
	}
	return itp.seekSame()
}

func (itp *intersectTwoPostings) Seek(id storage.SeriesRef) bool {
	return itp.a.Seek(id) && itp.b.Seek(id) && itp.seekSame()
}

func (itp *intersectTwoPostings) seekSame() bool {
	for {
		if ca, cb := itp.a.At(), itp.b.At(); ca == cb {
			itp.cur = ca
			return true
		} else if ca < cb {
			if !itp.a.Seek(cb) {
				return false
			}
		} else if ca > cb {
			if !itp.b.Seek(ca) {
				return false
			}
		}
	}
}
