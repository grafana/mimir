// SPDX-License-Identifier: AGPL-3.0-only
use super::*;

#[derive(Clone, Default, Serialize)]
pub(super) struct QueryChunk {
    pub(super) mint: i64,
    pub(super) maxt: i64,
    pub(super) data: Arc<[u8]>,
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
    labels_only: bool,
}
impl QueryCursor {
    pub(super) fn open(engine: &Engine, req: &Request, selected: &BTreeSet<u64>) -> Result<Self> {
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
                    || block.refs.is_disjoint(selected)
                {
                    continue;
                }
                // The immutable compressed snapshot survives retention and compaction.
                let cold = disk::read_cold(engine, block, selected, req.mint, req.maxt)?;
                for s in cold {
                    add(s);
                }
            }
            for r in selected {
                if let Some(s) = engine.state.series.get(r)
                    && s.has_samples()
                {
                    add(head_snapshot(s, req.mint, req.maxt));
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
            labels_only: false,
        })
    }
    pub(super) fn labels(engine: &Engine, req: &Request, selected: &BTreeSet<u64>) -> Result<Self> {
        let mut visible = BTreeSet::new();
        let mut pending = BTreeMap::<usize, BTreeSet<u64>>::new();
        if req.mint <= req.maxt {
            for reference in selected {
                if let Some(series) = engine.state.series.get(reference)
                    && has_samples_in(series, req.mint, req.maxt)
                {
                    visible.insert(*reference);
                }
            }
            for (index, block) in engine.state.cold.iter().enumerate() {
                if block.maxt < req.mint || block.mint > req.maxt {
                    continue;
                }
                for reference in selected {
                    if visible.contains(reference) {
                        continue;
                    }
                    if let Some(&(first, last)) = block.ranges.get(reference) {
                        if first > req.maxt || last < req.mint {
                            continue;
                        }
                        // Persisted endpoints are actual samples, so these lookups need no chunk I/O.
                        if first >= req.mint || last <= req.maxt {
                            visible.insert(*reference);
                            continue;
                        }
                    } else if !block.refs.contains(reference) {
                        continue;
                    }
                    pending.entry(index).or_default().insert(*reference);
                }
            }
            for (index, mut refs) in pending {
                refs.retain(|reference| !visible.contains(reference));
                if refs.is_empty() {
                    continue;
                }
                for series in
                    disk::read_cold(engine, &engine.state.cold[index], &refs, req.mint, req.maxt)?
                {
                    if has_samples_in(&series, req.mint, req.maxt) {
                        visible.insert(series.reference);
                    }
                }
            }
        }
        let mut series: Vec<_> = visible
            .into_iter()
            .filter_map(|reference| {
                let series = engine.state.series.get(&reference)?;
                Some(QuerySeries {
                    reference,
                    labels: series.labels.clone(),
                    parts: Vec::new(),
                    chunks: Some(Vec::new()),
                })
            })
            .collect();
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
            unordered: false,
            labels_only: true,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.series.len()
    }
    pub(super) fn call(&mut self, req: &Request) -> Result<Response> {
        if req.op == "cursor_series_page" {
            let start = usize::try_from(req.reference).map_err(io)?;
            if start > self.series.len() {
                return Err("query series out of range".into());
            }
            let end = start
                .saturating_add(req.limit.clamp(1, 16))
                .min(self.series.len());
            let mut response = Response {
                series: Vec::with_capacity(end - start),
                ..Default::default()
            };
            if !self.labels_only {
                response.refs.reserve_exact(end - start);
                response.chunks.reserve_exact(end - start);
            }
            for s in &mut self.series[start..end] {
                if self.labels_only {
                    response.series.push(Series {
                        reference: s.reference,
                        labels: s.labels.clone(),
                        ..Default::default()
                    });
                    continue;
                }
                let chunks = s.chunks.get_or_insert_with(|| {
                    prepare(
                        std::mem::take(&mut s.parts),
                        self.mint,
                        self.maxt,
                        self.unordered,
                    )
                });
                response.series.push(Series {
                    reference: s.reference,
                    labels: s.labels.clone(),
                    ..Default::default()
                });
                response.refs.push(chunks.len() as u64);
                response
                    .chunks
                    .push(chunks.first().cloned().unwrap_or_default());
            }
            response.done = end == self.series.len();
            return Ok(response);
        }
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
fn has_samples_in(series: &Series, mint: i64, maxt: i64) -> bool {
    if series.samples.range(mint..=maxt).next().is_some() {
        return true;
    }
    for chunk in &series.float_chunks {
        if chunk.maxt < mint || chunk.mint > maxt {
            continue;
        }
        if chunk.mint >= mint
            || chunk.maxt <= maxt
            || xor::decode(&chunk.data)
                .into_iter()
                .any(|(t, _)| t >= mint && t <= maxt)
        {
            return true;
        }
    }
    if let Some(chunk) = &series.open_float
        && let (Some(first), Some(last)) = (chunk.first_timestamp(), chunk.last_timestamp())
        && first <= maxt
        && last >= mint
    {
        return first >= mint
            || last <= maxt
            || xor::decode(&chunk.clone().into_bytes())
                .into_iter()
                .any(|(t, _)| t >= mint && t <= maxt);
    }
    false
}
// Filtering before cloning bounds temporary histogram copies to the query range.
fn head_snapshot(series: &Series, mint: i64, maxt: i64) -> Series {
    Series {
        reference: series.reference,
        labels: series.labels.clone(),
        samples: if series
            .samples
            .first_key_value()
            .is_none_or(|(&t, _)| t >= mint)
            && series
                .samples
                .last_key_value()
                .is_none_or(|(&t, _)| t <= maxt)
        {
            series.samples.clone()
        } else {
            series
                .samples
                .range(mint..=maxt)
                .map(|(&t, sample)| (t, sample.clone()))
                .collect()
        },
        float_chunks: series
            .float_chunks
            .iter()
            .filter(|c| c.maxt >= mint && c.mint <= maxt)
            .cloned()
            .collect(),
        open_float: series
            .open_float
            .as_ref()
            .filter(|c| {
                c.last_timestamp().is_some_and(|t| t >= mint)
                    && c.first_timestamp().is_some_and(|t| t <= maxt)
            })
            .cloned(),
        ..Default::default()
    }
}
fn float_chunk(samples: &[(i64, f64)]) -> QueryChunk {
    QueryChunk {
        mint: samples[0].0,
        maxt: samples.last().unwrap().0,
        data: xor::encode(samples).into(),
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
                data: c.into_bytes().into(),
                out_of_order: false,
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
                // Chunk queries expose whole intersecting chunks; the querier
                // applies sample bounds while merging the streamed response.
                if unordered || (c.mint >= mint && c.maxt <= maxt) {
                    return Some(QueryChunk {
                        mint: c.mint,
                        maxt: c.maxt,
                        data: c.data,
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
                        ..Default::default()
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
                data: Arc::default(),
                samples: vec![s],
            });
        }
    }
    if !values.is_empty() {
        out.push(float_chunk(&values));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshots_and_pages_share_sealed_compressed_bytes() {
        let mut series = Series::default();
        for t in 0..=120 {
            series.insert(Sample {
                t,
                kind: 1,
                bits: (t as f64).to_bits(),
                ..Default::default()
            });
        }
        let sealed = series.float_chunks[0].data.clone();
        let snapshot = series.clone();
        assert!(Arc::ptr_eq(&sealed, &snapshot.float_chunks[0].data));
        let prepared = prepare(vec![snapshot], 10, 110, true);
        assert_eq!(prepared.len(), 1);
        assert!(Arc::ptr_eq(&sealed, &prepared[0].data));
        let page = prepared[..1].to_vec();
        assert!(Arc::ptr_eq(&sealed, &page[0].data));
        drop(series);
        assert_eq!(xor::decode(&page[0].data).len(), 120);
    }

    #[test]
    fn ordered_sample_queries_trim_chunk_boundaries() {
        let data = xor::encode(&(0..120).map(|t| (t, t as f64)).collect::<Vec<_>>());
        let series = Series {
            float_chunks: vec![FloatChunk {
                mint: 0,
                maxt: 119,
                data: data.into(),
                out_of_order: false,
            }],
            ..Default::default()
        };
        let prepared = prepare(vec![series], 10, 110, false);
        let samples = xor::decode(&prepared[0].data);
        assert_eq!(samples.len(), 101);
        assert_eq!(samples.first().unwrap().0, 10);
        assert_eq!(samples.last().unwrap().0, 110);
    }
}
