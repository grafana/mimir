// SPDX-License-Identifier: AGPL-3.0-only

#[cfg(all(target_os = "linux", feature = "jemalloc"))]
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
#[cfg(all(target_os = "linux", feature = "jemalloc"))]
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use pprof::protos::Message;
#[cfg(all(target_os = "linux", feature = "jemalloc"))]
use pprof::protos::{Function, Line, Profile};
use serde::Deserialize;
#[cfg(all(target_os = "linux", feature = "jemalloc"))]
use serde::Serialize;
use tokio::sync::Mutex;

#[derive(Deserialize)]
struct ProfileQuery {
    seconds: Option<u64>,
}

pub async fn start(address: &str) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .with_context(|| format!("bind profiling listener {address}"))?;
    let app = Router::new()
        .route("/debug/pprof/profile", get(cpu_profile))
        .with_state(Arc::new(Mutex::new(())));
    #[cfg(all(target_os = "linux", feature = "jemalloc"))]
    let app = app
        .route("/debug/pprof/allocs", get(heap_profile))
        .route("/debug/pprof/heap", get(heap_profile))
        .route("/debug/allocator/stats", get(allocator_stats));
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            eprintln!("CPU profiling server stopped: {error}");
        }
    });
    eprintln!("phase=profiling_start address={address}");
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "jemalloc"))]
async fn heap_profile() -> Response {
    let Some(control) = jemalloc_pprof::PROF_CTL.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "heap profiling is disabled",
        )
            .into_response();
    };
    let Ok(mut control) = Arc::clone(control).try_lock_owned() else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            "heap profile already running",
        )
            .into_response();
    };
    if !control.activated() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "heap profiling is inactive",
        )
            .into_response();
    }
    match tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        let profile = control.dump_pprof()?;
        normalize_heap_pprof(profile)
    })
    .await
    {
        Ok(Ok(profile)) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            profile,
        )
            .into_response(),
        Ok(Err(error)) => {
            eprintln!("heap profiling failed: {error:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, "heap profiling failed").into_response()
        }
        Err(error) => {
            eprintln!("heap profiling task failed: {error}");
            (StatusCode::INTERNAL_SERVER_ERROR, "heap profiling failed").into_response()
        }
    }
}

#[cfg(all(target_os = "linux", feature = "jemalloc"))]
fn normalize_heap_pprof(compressed: Vec<u8>) -> Result<Vec<u8>> {
    let mut decoded = Vec::new();
    GzDecoder::new(compressed.as_slice()).read_to_end(&mut decoded)?;
    let mut profile = Profile::decode(decoded.as_slice())?;

    // Pyroscope's flamegraph truncation loses samples with unresolved frames.
    if profile
        .location
        .iter()
        .any(|location| location.line.is_empty())
    {
        let name = profile.string_table.len() as i64;
        profile.string_table.push("[unresolved]".to_string());
        let function_id = profile
            .function
            .iter()
            .map(|function| function.id)
            .max()
            .unwrap_or(0)
            + 1;
        profile.function.push(Function {
            id: function_id,
            name,
            system_name: name,
            ..Default::default()
        });
        for location in &mut profile.location {
            if location.line.is_empty() {
                location.line.push(Line {
                    function_id,
                    ..Default::default()
                });
            }
        }
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&profile.encode_to_vec())?;
    Ok(encoder.finish()?)
}

#[cfg(all(target_os = "linux", feature = "jemalloc"))]
#[derive(Serialize)]
struct AllocatorStats {
    allocated_bytes: usize,
    active_bytes: usize,
    resident_bytes: usize,
    metadata_bytes: usize,
    mapped_bytes: usize,
    retained_bytes: usize,
}

#[cfg(all(target_os = "linux", feature = "jemalloc"))]
async fn allocator_stats() -> Response {
    use tikv_jemalloc_ctl::{epoch, stats};

    match tokio::task::spawn_blocking(|| -> Result<AllocatorStats> {
        epoch::advance()?;
        Ok(AllocatorStats {
            allocated_bytes: stats::allocated::read()?,
            active_bytes: stats::active::read()?,
            resident_bytes: stats::resident::read()?,
            metadata_bytes: stats::metadata::read()?,
            mapped_bytes: stats::mapped::read()?,
            retained_bytes: stats::retained::read()?,
        })
    })
    .await
    {
        Ok(Ok(stats)) => axum::Json(stats).into_response(),
        Ok(Err(error)) => {
            eprintln!("allocator stats failed: {error:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, "allocator stats failed").into_response()
        }
        Err(error) => {
            eprintln!("allocator stats task failed: {error}");
            (StatusCode::INTERNAL_SERVER_ERROR, "allocator stats failed").into_response()
        }
    }
}

async fn cpu_profile(
    State(lock): State<Arc<Mutex<()>>>,
    Query(query): Query<ProfileQuery>,
) -> Response {
    let Ok(_guard) = lock.try_lock() else {
        return (StatusCode::TOO_MANY_REQUESTS, "CPU profile already running").into_response();
    };
    let seconds = query.seconds.unwrap_or(10).clamp(1, 30);
    let result = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
        let profiler = pprof::ProfilerGuardBuilder::default()
            .frequency(99)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .context("start CPU profiler")?;
        std::thread::sleep(Duration::from_secs(seconds));
        let report = profiler
            .report()
            .frames_post_processor(qualify_frames)
            .build()
            .context("build CPU profile")?;
        let profile = report.pprof().context("encode CPU profile")?;
        let mut bytes = Vec::new();
        profile
            .encode(&mut bytes)
            .context("serialize CPU profile")?;
        Ok(bytes)
    })
    .await;
    match result {
        Ok(Ok(bytes)) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            bytes,
        )
            .into_response(),
        Ok(Err(error)) => {
            eprintln!("CPU profiling failed: {error:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, "CPU profiling failed").into_response()
        }
        Err(error) => {
            eprintln!("CPU profiling task failed: {error}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "CPU profiling task failed",
            )
                .into_response()
        }
    }
}

/// Names each frame of a stack by its module path. Inlined frames only carry their short name, so
/// profiles would otherwise merge every `next`, `fold` or `{closure#0}` of the program into one
/// function. Closures and async blocks are named after the function of the same file they run in.
fn qualify_frames(frames: &mut pprof::Frames) {
    // Innermost first, so the functions a frame runs in come after it.
    let symbols: Vec<&mut pprof::Symbol> = frames.frames.iter_mut().flatten().collect();
    let files: Vec<Option<String>> = symbols
        .iter()
        .map(|symbol| {
            symbol
                .filename
                .as_deref()
                .map(|file| file.to_string_lossy().into_owned())
        })
        .collect();
    let mut qualified = vec![String::new(); symbols.len()];
    for (index, symbol) in symbols.iter().enumerate().rev() {
        let name = symbol.name();
        // Inlined frames carry their generic arguments, whose paths say nothing of where the
        // function is.
        let name = match name.find('<') {
            Some(generics) if generics > 0 => name[..generics].to_owned(),
            _ => name,
        };
        qualified[index] = if name.contains("::") {
            name.clone()
        } else if name.starts_with('{') {
            let within = (index + 1..symbols.len())
                .find(|outer| files[*outer].is_some() && files[*outer] == files[index])
                .map(|outer| qualified[outer].clone())
                .or_else(|| files[index].as_deref().and_then(module_path));
            within.map_or(name.clone(), |within| format!("{within}::{name}"))
        } else {
            files[index]
                .as_deref()
                .and_then(module_path)
                .map_or(name.clone(), |module| format!("{module}::{name}"))
        };
    }
    for (symbol, name) in symbols.into_iter().zip(qualified) {
        symbol.name = Some(name.into_bytes());
    }
}

/// The module path of a source file of the standard library, a dependency or this crate, which
/// is built in `/src`.
fn module_path(file: &str) -> Option<String> {
    let (root, path) = file.rsplit_once("/src/")?;
    let krate = root.rsplit('/').next().unwrap_or_default();
    // Registry sources are in `<crate>-<version>`.
    let krate = match krate.rsplit_once('-') {
        Some((name, version)) if version.starts_with(|c: char| c.is_ascii_digit()) => name,
        _ => krate,
    };
    let krate = if krate == "src" { "" } else { krate };
    let mut parts: Vec<&str> = path.strip_suffix(".rs")?.split('/').collect();
    if matches!(parts.last(), Some(&("mod" | "lib" | "main"))) {
        parts.pop();
    }
    let module = std::iter::once(krate)
        .chain(parts)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("::")
        .replace('-', "_");
    (!module.is_empty()).then_some(module)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    fn symbol(name: &str, file: &str) -> pprof::Symbol {
        pprof::Symbol {
            name: Some(name.as_bytes().to_vec()),
            addr: None,
            lineno: None,
            filename: Some(PathBuf::from(file)),
        }
    }

    #[test]
    fn frames_are_named_by_module() {
        let std = "/rustc/48a229ce/library/core/src/iter/adapters/map.rs";
        let tokio = "/usr/local/cargo/registry/src/index.crates.io-1949cf8c/tokio-1.53.1/src/runtime/task/mod.rs";
        let store = "/src/src/store.rs";
        let mut frames = pprof::Frames {
            frames: vec![
                vec![
                    symbol("{closure#0}<core::result::Result<(), E>>", store),
                    symbol("{closure#2}", store),
                    symbol("try_fold<a::B, C>", std),
                    symbol("matching", store),
                ],
                vec![
                    symbol("next", store),
                    symbol("<a::B>::poll", "/src/src/lib.rs"),
                ],
                vec![
                    symbol("{async_block#1}", "/src/src/main.rs"),
                    symbol("run", tokio),
                ],
            ],
            thread_name: String::new(),
            thread_id: 0,
            sample_timestamp: std::time::SystemTime::UNIX_EPOCH,
        };
        qualify_frames(&mut frames);
        let names: Vec<Vec<String>> = frames
            .frames
            .iter()
            .map(|frame| frame.iter().map(pprof::Symbol::name).collect())
            .collect();
        assert_eq!(
            names,
            [
                vec![
                    "store::matching::{closure#2}::{closure#0}",
                    "store::matching::{closure#2}",
                    "core::iter::adapters::map::try_fold",
                    "store::matching",
                ],
                vec!["store::next", "<a::B>::poll"],
                // Nothing of the same file runs it.
                vec!["{async_block#1}", "tokio::runtime::task::run"],
            ]
        );
    }
}
