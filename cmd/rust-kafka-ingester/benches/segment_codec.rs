//! Compresses the frames of a real segment file with each candidate codec:
//! `SEGMENT_FILE=<path> cargo bench --bench segment_codec` (`SEGMENT_BATCH=n` compresses n frames
//! together).

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
    // SEGMENT_BATCH frames compressed together, to see what fewer, larger frames would give.
    let batch =
        std::env::var("SEGMENT_BATCH").map_or(1, |batch| batch.parse().expect("SEGMENT_BATCH"));
    let payloads = payloads
        .chunks(batch)
        .map(|frames| frames.concat())
        .collect::<Vec<_>>();
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
            Box::new(|compressed, len| {
                thread_local! {
                    static DECOMPRESSOR: std::cell::RefCell<zstd::bulk::Decompressor<'static>> =
                        std::cell::RefCell::new(zstd::bulk::Decompressor::new().unwrap());
                }
                DECOMPRESSOR.with_borrow_mut(|decompressor| {
                    decompressor.decompress(compressed, len).unwrap()
                })
            }),
        )
    };
    // A dictionary trained on the file's first frames, as a file could keep in its header.
    let started = Instant::now();
    let dictionary = zstd::dict::from_samples(&payloads[..payloads.len().min(2000)], 112 * 1024)
        .expect("train dictionary");
    println!(
        "dictionary: {} KB trained in {:.0} ms",
        dictionary.len() / 1024,
        started.elapsed().as_secs_f64() * 1000.
    );
    let zstd_dictionary = |level: i32| -> Codec {
        let compress_dictionary = dictionary.clone();
        let decompress_dictionary = dictionary.clone();
        (
            Box::leak(format!("zstd {level} dict").into_boxed_str()),
            Box::new(move |payload| {
                thread_local! {
                    static COMPRESSORS: std::cell::RefCell<std::collections::HashMap<i32, zstd::bulk::Compressor<'static>>> =
                        std::cell::RefCell::default();
                }
                COMPRESSORS.with_borrow_mut(|compressors| {
                    compressors
                        .entry(level)
                        .or_insert_with(|| {
                            zstd::bulk::Compressor::with_dictionary(level, &compress_dictionary)
                                .unwrap()
                        })
                        .compress(payload)
                        .unwrap()
                })
            }),
            Box::new(move |compressed, len| {
                thread_local! {
                    static DECOMPRESSORS: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> =
                        const { std::cell::RefCell::new(None) };
                }
                DECOMPRESSORS.with_borrow_mut(|decompressor| {
                    decompressor
                        .get_or_insert_with(|| {
                            zstd::bulk::Decompressor::with_dictionary(&decompress_dictionary)
                                .unwrap()
                        })
                        .decompress(compressed, len)
                        .unwrap()
                })
            }),
        )
    };
    let codecs: Vec<Codec> = vec![
        zstd_dictionary(1),
        zstd_dictionary(-1),
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
        // The best of three runs: other work on the machine makes single runs noisy.
        let (mut compress_s, mut decompress_s, mut compressed) = (f64::MAX, f64::MAX, Vec::new());
        for _ in 0..3 {
            let started = Instant::now();
            compressed = payloads
                .iter()
                .map(|payload| compress(payload))
                .collect::<Vec<_>>();
            compress_s = compress_s.min(started.elapsed().as_secs_f64());
            let started = Instant::now();
            for (compressed, payload) in compressed.iter().zip(&payloads) {
                assert_eq!(decompress(compressed, payload.len()), *payload);
            }
            decompress_s = decompress_s.min(started.elapsed().as_secs_f64());
        }
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
