// SPDX-License-Identifier: AGPL-3.0-only
use super::*;

#[derive(Clone, Default, Serialize)]
pub(super) struct QueryChunk {
    pub(super) mint: i64,
    pub(super) maxt: i64,
    pub(super) data: Vec<u8>,
    pub(super) samples: Vec<Sample>,
}
struct QuerySeries {
    reference: u64,
    labels: Arc<Vec<Label>>,
    parts: Vec<Series>,
    chunks: Option<Vec<QueryChunk>>,
}
pub(super) struct QueryCursor {
    series: Vec<QuerySeries>,
    mint: i64,
    maxt: i64,
    unordered: bool,
}
impl QueryCursor {
    pub(super) fn open(engine: &Engine, req: &Request) -> Result<Self> {
        let selected: BTreeSet<_> = req.refs.iter().copied().collect();
        let mut all: BTreeMap<u64, QuerySeries> = BTreeMap::new();
        let mut add = |mut s: Series| {
            if !selected.contains(&s.reference) {
                return;
            }
            s.float_chunks
                .retain(|c| c.maxt >= req.mint && c.mint <= req.maxt);
            s.samples.retain(|t, _| *t >= req.mint && *t <= req.maxt);
            if s.open_float.as_ref().is_some_and(|c| {
                c.last_timestamp().is_none_or(|t| t < req.mint)
                    || c.first_timestamp().is_none_or(|t| t > req.maxt)
            }) {
                s.open_float = None;
            }
            if !s.has_samples() {
                return;
            }
            s.exemplars.clear();
            s.last = None;
            all.entry(s.reference)
                .or_insert_with(|| QuerySeries {
                    reference: s.reference,
                    labels: s.labels.clone(),
                    parts: Vec::new(),
                    chunks: None,
                })
                .parts
                .push(s);
        };
        if req.mint <= req.maxt {
            for block in &engine.state.cold {
                if block.maxt < req.mint
                    || block.mint > req.maxt
                    || block.refs.is_disjoint(&selected)
                {
                    continue;
                }
                // The immutable compressed snapshot survives retention and compaction.
                let cold = disk::read_cold(engine, block, &selected)?;
                for s in cold {
                    add(s);
                }
            }
            for r in &req.refs {
                if let Some(s) = engine.state.series.get(r) {
                    add(s.clone());
                }
            }
        }
        let mut series: Vec<_> = all.into_values().collect();
        if req.sorted {
            series.sort_unstable_by(|a, b| {
                a.labels
                    .iter()
                    .map(|l| (&l.name, &l.value))
                    .cmp(b.labels.iter().map(|l| (&l.name, &l.value)))
            });
        }
        Ok(Self {
            series,
            mint: req.mint,
            maxt: req.maxt,
            unordered: req.unordered,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.series.len()
    }
    pub(super) fn call(&mut self, req: &Request) -> Result<Response> {
        let s = self
            .series
            .get_mut(usize::try_from(req.reference).map_err(io)?)
            .ok_or("query series out of range")?;
        let chunks = s.chunks.get_or_insert_with(|| {
            prepare(
                std::mem::take(&mut s.parts),
                self.mint,
                self.maxt,
                self.unordered,
            )
        });
        let mut response = Response {
            count: chunks.len(),
            ..Default::default()
        };
        if req.op == "cursor_series" {
            response.series.push(Series {
                reference: s.reference,
                labels: s.labels.clone(),
                ..Default::default()
            });
        } else {
            let start = usize::try_from(req.up_to).map_err(io)?;
            if start > chunks.len() {
                return Err("query chunk offset out of range".into());
            }
            let end = start
                .saturating_add(req.limit.clamp(1, 16))
                .min(chunks.len());
            response.chunks.extend_from_slice(&chunks[start..end]);
            response.done = end == chunks.len();
        }
        Ok(response)
    }
}
fn float_chunk(samples: &[(i64, f64)]) -> QueryChunk {
    QueryChunk {
        mint: samples[0].0,
        maxt: samples.last().unwrap().0,
        data: xor::encode(samples),
        samples: Vec::new(),
    }
}
fn prepare(parts: Vec<Series>, mint: i64, maxt: i64, unordered: bool) -> Vec<QueryChunk> {
    let mut floats = Vec::new();
    let mut samples = BTreeMap::new();
    for s in parts {
        floats.extend(s.float_chunks);
        if let Some(c) = s.open_float {
            floats.push(FloatChunk {
                mint: c.first_timestamp().unwrap(),
                maxt: c.last_timestamp().unwrap(),
                data: c.into_bytes().into_boxed_slice(),
            });
        }
        samples.extend(s.samples);
    }
    floats.sort_unstable_by_key(|c| c.mint);
    if unordered || (samples.is_empty() && floats.windows(2).all(|w| w[0].maxt < w[1].mint)) {
        let mut out: Vec<_> = floats
            .into_iter()
            .filter_map(|c| {
                if c.maxt < mint || c.mint > maxt {
                    return None;
                }
                if c.mint >= mint && c.maxt <= maxt {
                    return Some(QueryChunk {
                        mint: c.mint,
                        maxt: c.maxt,
                        data: c.data.into_vec(),
                        samples: Vec::new(),
                    });
                }
                let values: Vec<_> = xor::decode(&c.data)
                    .into_iter()
                    .filter(|(t, _)| *t >= mint && *t <= maxt)
                    .collect();
                if values.is_empty() {
                    None
                } else {
                    Some(float_chunk(&values))
                }
            })
            .collect();
        // Streaming ingester reads allow overlapping chunks; the querier merges them.
        out.extend(encode_samples(samples));
        return out;
    }
    // OOO chunks may overlap; normalize a single series before exposing ordered chunks.
    let mut merged = BTreeMap::new();
    for c in floats {
        for (t, v) in xor::decode(&c.data) {
            if t >= mint && t <= maxt {
                merged.insert(
                    t,
                    Sample {
                        t,
                        kind: 1,
                        bits: v.to_bits(),
                        data: String::new(),
                    },
                );
            }
        }
    }
    merged.extend(samples);
    encode_samples(merged)
}
fn encode_samples(merged: BTreeMap<i64, Sample>) -> Vec<QueryChunk> {
    let mut out = Vec::new();
    let mut values = Vec::new();
    for s in merged.into_values() {
        if s.kind == 1 {
            values.push((s.t, f64::from_bits(s.bits)));
            if values.len() == 120 {
                out.push(float_chunk(&values));
                values.clear();
            }
        } else {
            if !values.is_empty() {
                out.push(float_chunk(&values));
                values.clear();
            }
            // Go owns histogram validation and encoding until the native histogram codec exists.
            out.push(QueryChunk {
                mint: s.t,
                maxt: s.t,
                data: Vec::new(),
                samples: vec![s],
            });
        }
    }
    if !values.is_empty() {
        out.push(float_chunk(&values));
    }
    out
}
