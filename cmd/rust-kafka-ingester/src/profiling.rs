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
        let report = profiler.report().build().context("build CPU profile")?;
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
