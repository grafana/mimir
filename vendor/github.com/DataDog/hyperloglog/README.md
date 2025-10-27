hyperloglog
===========

Package hyperloglog implements the HyperLogLog algorithm for
cardinality estimation. In English: it counts things. It counts things
using very small amounts of memory compared to the number of objects
it is counting.

For a full description of the algorithm, see the paper HyperLogLog:
the analysis of a near-optimal cardinality estimation algorithm by
Flajolet, et. al. at http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf

For documentation see http://godoc.org/github.com/DataDog/hyperloglog

Included are a set of fast implementations for murmurhash suitable for use
on 32 and 64 bit integers on little endian machines.

Quick start
===========

	$ go get github.com/DataDog/hyperloglog
	$ cd $GOPATH/src/github.com/DataDog/hyperloglog
	$ go test -test.v
	$ go test -bench=.

License
=======

hyperloglog is licensed under the MIT license.
