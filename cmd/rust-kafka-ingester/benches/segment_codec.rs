//! Compresses the frames of a real segment file with each candidate codec:
//! `SEGMENT_FILE=<path> cargo bench --bench segment_codec`.

use std::time::Instant;

const FILE_HEADER_LEN: usize = 28;
const FRAME_HEADER_LEN: usize = 8;
const FRAME_PREFIX_LEN: usize = 24;

fn main() {
    let path = std::env::var("SEGMENT_FILE").expect("SEGMENT_FILE");
    let data = std::fs::read(path).expect("read segment");
    let mut payloads = Vec::new();
    let mut at = FILE_HEADER_LEN;
    // Stops at the first torn frame, like replay.
    while at + FRAME_HEADER_LEN <= data.len() {
        let length = u32::from_le_bytes(data[at..at + 4].try_into().unwrap()) as usize;
        let body = at + FRAME_HEADER_LEN;
        if length < FRAME_PREFIX_LEN || body + length > data.len() {
            break;
        }
        let compressed = &data[body + FRAME_PREFIX_LEN..body + length];
        payloads.push(zstd::bulk::decompress(compressed, 128 * 1024 * 1024).expect("zstd frame"));
        at = body + length;
    }
    let raw: usize = payloads.iter().map(Vec::len).sum();
    println!(
        "frames={} raw_MB={:.1} mean_frame_KB={:.1}",
        payloads.len(),
        raw as f64 / 1e6,
        raw as f64 / payloads.len() as f64 / 1e3
    );
    type Codec = (
        &'static str,
        Box<dyn Fn(&[u8]) -> Vec<u8>>,
        Box<dyn Fn(&[u8], usize) -> Vec<u8>>,
    );
    let zstd_level = |level: i32| -> Codec {
        (
            Box::leak(format!("zstd {level}").into_boxed_str()),
            Box::new(move |payload| {
                thread_local! {
                    static COMPRESSORS: std::cell::RefCell<std::collections::HashMap<i32, zstd::bulk::Compressor<'static>>> =
                        std::cell::RefCell::default();
                }
                COMPRESSORS.with_borrow_mut(|compressors| {
                    compressors
                        .entry(level)
                        .or_insert_with(|| zstd::bulk::Compressor::new(level).unwrap())
                        .compress(payload)
                        .unwrap()
                })
            }),
            Box::new(|compressed, len| zstd::bulk::decompress(compressed, len).unwrap()),
        )
    };
    let codecs: Vec<Codec> = vec![
        zstd_level(1),
        zstd_level(-1),
        zstd_level(-3),
        zstd_level(-5),
        (
            "lz4",
            Box::new(lz4_flex::block::compress),
            Box::new(|compressed, len| lz4_flex::block::decompress(compressed, len).unwrap()),
        ),
        (
            "snappy",
            Box::new(|payload| snap::raw::Encoder::new().compress_vec(payload).unwrap()),
            Box::new(|compressed, _| {
                snap::raw::Decoder::new()
                    .decompress_vec(compressed)
                    .unwrap()
            }),
        ),
    ];
    for (name, compress, decompress) in &codecs {
        let started = Instant::now();
        let compressed = payloads
            .iter()
            .map(|payload| compress(payload))
            .collect::<Vec<_>>();
        let compress_s = started.elapsed().as_secs_f64();
        let started = Instant::now();
        for (compressed, payload) in compressed.iter().zip(&payloads) {
            assert_eq!(decompress(compressed, payload.len()).len(), payload.len());
        }
        let decompress_s = started.elapsed().as_secs_f64();
        let size: usize = compressed.iter().map(Vec::len).sum();
        println!(
            "{name:>8}: ratio={:.2} size_MB={:.1} compress_MB_s={:.0} decompress_MB_s={:.0}",
            raw as f64 / size as f64,
            size as f64 / 1e6,
            raw as f64 / 1e6 / compress_s,
            raw as f64 / 1e6 / decompress_s,
        );
    }
}
