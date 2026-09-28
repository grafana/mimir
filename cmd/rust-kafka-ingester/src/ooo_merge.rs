//! Serves overlapping in-order and out-of-order chunks the way the Go ingester does with an
//! out-of-order window: overlapping chunks are grouped like Prometheus's `getOOOSeriesChunks`,
//! merged with `chainSampleIterator` (whose heap decides which sample wins a timestamp tie), and
//! re-encoded like `populateChunksFromIterable`.

use anyhow::Result;

use crate::histogram;
use crate::proto::cortexpb;
use crate::proto::cortexpb::histogram::Count;
use crate::xor;

pub const XOR_ENCODING: i32 = 4;
const MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND: usize = 1024;
const TARGET_BYTES_PER_HISTOGRAM_CHUNK: usize = 1024;
const MIN_SAMPLES_PER_HISTOGRAM_CHUNK: usize = 10;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Float(f64),
    Histogram(Box<cortexpb::Histogram>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ValueType {
    Float,
    Histogram,
    FloatHistogram,
}

impl Value {
    fn value_type(&self) -> ValueType {
        match self {
            Value::Float(_) => ValueType::Float,
            Value::Histogram(histogram) => match histogram.count {
                Some(Count::CountFloat(_)) => ValueType::FloatHistogram,
                _ => ValueType::Histogram,
            },
        }
    }
}

/// An encoded chunk: min time, max time, wire encoding and bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct Chunk {
    pub min_time: i64,
    pub max_time: i64,
    pub encoding: i32,
    pub data: Vec<u8>,
}

pub fn decode(encoding: i32, data: &[u8]) -> Result<Vec<(i64, Value)>> {
    if encoding == XOR_ENCODING {
        return Ok(xor::decode(data)
            .into_iter()
            .map(|(timestamp, value)| (timestamp, Value::Float(value)))
            .collect());
    }
    Ok(histogram::decode(encoding, data)?
        .into_iter()
        .map(|histogram| (histogram.timestamp, Value::Histogram(Box::new(histogram))))
        .collect())
}

// Accumulates samples into chunks, cutting a new chunk on a value type change and when
// `size_cut` says the current one is full; histograms also cut where the Prometheus appender
// would start a new chunk.
struct Encoder {
    chunks: Vec<Chunk>,
    floats: Option<(i64, xor::Appender)>,
    histograms: Vec<cortexpb::Histogram>,
    size_cut: bool,
}

impl Encoder {
    fn new(size_cut: bool) -> Self {
        Self {
            chunks: Vec::new(),
            floats: None,
            histograms: Vec::new(),
            size_cut,
        }
    }

    fn finish_current(&mut self) {
        if let Some((min_time, appender)) = self.floats.take() {
            let max_time = appender.last_timestamp().expect("float chunk has samples");
            self.chunks.push(Chunk {
                min_time,
                max_time,
                encoding: XOR_ENCODING,
                data: appender.into_bytes(),
            });
        }
        if !self.histograms.is_empty() {
            let encoded = histogram::encode_sequence(&self.histograms);
            self.chunks.push(Chunk {
                min_time: self.histograms[0].timestamp,
                max_time: self.histograms[self.histograms.len() - 1].timestamp,
                encoding: encoded.encoding,
                data: encoded.data,
            });
            self.histograms.clear();
        }
    }

    fn push(&mut self, timestamp: i64, value: &Value) {
        match value {
            Value::Float(value) => {
                if !self.histograms.is_empty() {
                    self.finish_current();
                }
                if let Some((_, appender)) = &self.floats
                    && self.size_cut
                    && appender.bytes().len() > MAX_BYTES_PER_XOR_CHUNK_BEFORE_APPEND
                {
                    self.finish_current();
                }
                self.floats
                    .get_or_insert_with(|| (timestamp, xor::Appender::default()))
                    .1
                    .append(timestamp, *value);
            }
            Value::Histogram(histogram) => {
                if self.floats.is_some() {
                    self.finish_current();
                }
                if let Some(last) = self.histograms.last() {
                    let same_type =
                        Value::Histogram(Box::new(last.clone())).value_type() == value.value_type();
                    let full = self.size_cut
                        && self.histograms.len() > MIN_SAMPLES_PER_HISTOGRAM_CHUNK
                        && histogram::encode_sequence(&self.histograms).data.len()
                            > TARGET_BYTES_PER_HISTOGRAM_CHUNK;
                    if !same_type || full || !histogram::compatible(last, histogram) {
                        self.finish_current();
                    }
                }
                let mut histogram = (**histogram).clone();
                histogram.timestamp = timestamp;
                self.histograms.push(histogram);
            }
        }
    }

    fn finish(mut self) -> Vec<Chunk> {
        self.finish_current();
        self.chunks
    }
}

/// Encodes an out-of-order head's samples, sorted by timestamp, like `OOOChunk.ToEncodedChunks`.
pub fn encode_out_of_order(samples: &[(i64, Value)]) -> Vec<Chunk> {
    let mut encoder = Encoder::new(false);
    for (timestamp, value) in samples {
        encoder.push(*timestamp, value);
    }
    encoder.finish()
}

/// A chunk available to a query. `order` breaks ties between chunks with the same min time like
/// Prometheus's head chunk references: in-order chunks before out-of-order ones, each by position.
pub struct Candidate {
    pub chunk: Chunk,
    pub order: (bool, usize),
}

/// Groups overlapping chunks and merges each group into re-encoded chunks; a chunk that overlaps
/// no other is returned unchanged.
pub fn merge_overlapping(mut candidates: Vec<Candidate>) -> Result<Vec<Chunk>> {
    candidates.sort_by(|a, b| (a.chunk.min_time, a.order).cmp(&(b.chunk.min_time, b.order)));
    let mut output = Vec::new();
    let mut group: Vec<Chunk> = Vec::new();
    let mut group_max = i64::MIN;
    let flush = |group: &mut Vec<Chunk>, output: &mut Vec<Chunk>| -> Result<()> {
        match group.len() {
            0 => {}
            1 => output.push(group.pop().expect("one chunk")),
            _ => {
                let iterators = group
                    .iter()
                    .map(|chunk| decode(chunk.encoding, &chunk.data))
                    .collect::<Result<Vec<_>>>()?;
                let mut encoder = Encoder::new(true);
                for (timestamp, value) in chain(iterators) {
                    encoder.push(timestamp, &value);
                }
                output.extend(encoder.finish());
                group.clear();
            }
        }
        Ok(())
    };
    for candidate in candidates {
        if !group.is_empty() && candidate.chunk.min_time > group_max {
            flush(&mut group, &mut output)?;
        }
        if group.is_empty() {
            group_max = candidate.chunk.max_time;
        } else {
            group_max = group_max.max(candidate.chunk.max_time);
        }
        group.push(candidate.chunk);
    }
    flush(&mut group, &mut output)?;
    Ok(output)
}

struct Cursor {
    samples: Vec<(i64, Value)>,
    position: Option<usize>,
}

impl Cursor {
    fn next(&mut self) -> bool {
        let next = self.position.map_or(0, |position| position + 1);
        self.position = Some(next);
        next < self.samples.len()
    }

    fn at_t(&self) -> i64 {
        self.samples[self.position.expect("positioned")].0
    }
}

// Go's container/heap over cursor indices, ordered only by the current timestamp, so ties land
// wherever Go's sift-up and sift-down put them.
struct GoHeap(Vec<usize>);

impl GoHeap {
    fn less(&self, cursors: &[Cursor], i: usize, j: usize) -> bool {
        cursors[self.0[i]].at_t() < cursors[self.0[j]].at_t()
    }

    fn push(&mut self, cursors: &[Cursor], cursor: usize) {
        self.0.push(cursor);
        let mut j = self.0.len() - 1;
        while j > 0 {
            let i = (j - 1) / 2;
            if i == j || !self.less(cursors, j, i) {
                break;
            }
            self.0.swap(i, j);
            j = i;
        }
    }

    fn pop(&mut self, cursors: &[Cursor]) -> usize {
        let n = self.0.len() - 1;
        self.0.swap(0, n);
        let mut i = 0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.less(cursors, j2, j1) {
                j = j2;
            }
            if !self.less(cursors, j, i) {
                break;
            }
            self.0.swap(i, j);
            i = j;
        }
        self.0.pop().expect("heap not empty")
    }
}

/// `chainSampleIterator.Next`: the first chunk is the current iterator, the others wait in the
/// heap, and a sample whose timestamp was already emitted is skipped.
fn chain(iterators: Vec<Vec<(i64, Value)>>) -> Vec<(i64, Value)> {
    let mut cursors = iterators
        .into_iter()
        .map(|samples| Cursor {
            samples,
            position: None,
        })
        .collect::<Vec<_>>();
    let mut heap = GoHeap(Vec::new());
    for index in 1..cursors.len() {
        if cursors[index].next() {
            heap.push(&cursors, index);
        }
    }
    let mut output: Vec<(i64, Value)> = Vec::new();
    let mut curr = 0;
    let mut last_t = i64::MIN;
    loop {
        loop {
            if !cursors[curr].next() {
                if heap.0.is_empty() {
                    return output;
                }
            } else {
                let t = cursors[curr].at_t();
                if t == last_t {
                    continue;
                }
                if heap.0.is_empty() || t < cursors[heap.0[0]].at_t() {
                    break;
                }
                heap.push(&cursors, curr);
            }
            curr = heap.pop(&cursors);
            if cursors[curr].at_t() != last_t {
                break;
            }
        }
        let position = cursors[curr].position.expect("positioned");
        let (t, value) = cursors[curr].samples[position].clone();
        last_t = t;
        output.push((t, value));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn floats(samples: &[(i64, f64)]) -> Chunk {
        Chunk {
            min_time: samples[0].0,
            max_time: samples[samples.len() - 1].0,
            encoding: XOR_ENCODING,
            data: xor::encode(samples),
        }
    }

    fn values(chunks: &[Chunk]) -> Vec<(i64, f64)> {
        chunks
            .iter()
            .flat_map(|chunk| xor::decode(&chunk.data))
            .collect()
    }

    fn candidate(chunk: Chunk, out_of_order: bool, index: usize) -> Candidate {
        Candidate {
            chunk,
            order: (out_of_order, index),
        }
    }

    // Results observed from the Go ingester in the Go/Rust parity test.
    #[test]
    fn timestamp_ties_follow_the_chain_iterator() {
        // The in-order chunk sorts first and the out-of-order one waits in the heap, so at 2000 the
        // out-of-order sample wins.
        let merged = merge_overlapping(vec![
            candidate(floats(&[(1000, 1.0), (2000, 2.0), (3000, 3.0)]), false, 0),
            candidate(floats(&[(2000, 20.0)]), true, 0),
        ])
        .unwrap();
        assert_eq!(values(&merged), [(1000, 1.0), (2000, 20.0), (3000, 3.0)]);
        // After the out-of-order chunk emitted 1500 it is the current iterator, so the in-order
        // sample waiting in the heap wins at 2000.
        let merged = merge_overlapping(vec![
            candidate(floats(&[(1000, 1.0), (2000, 2.0), (3000, 3.0)]), false, 0),
            candidate(floats(&[(1500, 15.0), (2000, 20.0)]), true, 0),
        ])
        .unwrap();
        assert_eq!(
            values(&merged),
            [(1000, 1.0), (1500, 15.0), (2000, 2.0), (3000, 3.0)]
        );
    }

    #[test]
    fn non_overlapping_chunks_are_returned_unchanged() {
        let first = floats(&[(1000, 1.0), (2000, 2.0)]);
        let second = floats(&[(3000, 3.0)]);
        let merged = merge_overlapping(vec![
            candidate(second.clone(), true, 0),
            candidate(first.clone(), false, 0),
        ])
        .unwrap();
        assert_eq!(merged, [first, second]);
    }

    #[test]
    fn go_heap_matches_container_heap_order() {
        // Five cursors all at the same timestamp: Go pops them in the order its sift-down leaves.
        let mut cursors = (0..5)
            .map(|_| Cursor {
                samples: vec![(1, Value::Float(0.0))],
                position: Some(0),
            })
            .collect::<Vec<_>>();
        cursors.push(Cursor {
            samples: vec![(0, Value::Float(0.0))],
            position: Some(0),
        });
        let mut heap = GoHeap(Vec::new());
        for index in 0..cursors.len() {
            heap.push(&cursors, index);
        }
        // Pushing 5 (t=0) sifts it up past 2 and 0: heap [5, 1, 0, 3, 4, 2].
        assert_eq!(heap.0, [5, 1, 0, 3, 4, 2]);
        let order = (0..cursors.len())
            .map(|_| heap.pop(&cursors))
            .collect::<Vec<_>>();
        assert_eq!(order, [5, 2, 4, 3, 0, 1]);
    }

    #[test]
    fn out_of_order_head_splits_by_value_type() {
        let histogram = |timestamp| {
            Value::Histogram(Box::new(cortexpb::Histogram {
                timestamp,
                count: Some(Count::CountInt(1)),
                positive_spans: vec![cortexpb::BucketSpan {
                    offset: 0,
                    length: 1,
                }],
                positive_deltas: vec![1],
                ..Default::default()
            }))
        };
        let chunks = encode_out_of_order(&[
            (1000, histogram(1000)),
            (1500, histogram(1500)),
            (2000, Value::Float(22.0)),
        ]);
        assert_eq!(
            chunks
                .iter()
                .map(|chunk| (chunk.min_time, chunk.max_time, chunk.encoding))
                .collect::<Vec<_>>(),
            [(1000, 1500, 5), (2000, 2000, XOR_ENCODING)]
        );
        assert_eq!(decode(5, &chunks[0].data).unwrap().len(), 2);
    }
}
