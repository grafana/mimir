// SPDX-License-Identifier: AGPL-3.0-only
#ifndef MIMIR_RUST_ENGINE_H
#define MIMIR_RUST_ENGINE_H
#include <stddef.h>
#include <stdint.h>

typedef struct { uint8_t *data; size_t len; } mimir_engine_buffer;
// Inputs are borrowed for the duration of the call. Outputs belong to Rust and
// must be freed exactly once. The caller serializes access, including close.
void *mimir_engine_open(const uint8_t *path, size_t len, uint8_t kafka_replay, mimir_engine_buffer *error);
mimir_engine_buffer mimir_engine_call(void *engine, const uint8_t *request, size_t len);
// Hold engine access only for preparation/failure; serialize complete syncs.
void *mimir_engine_prepare_sync(void *engine, mimir_engine_buffer *error);
mimir_engine_buffer mimir_engine_sync_file(void *file);
void mimir_engine_fail_sync(void *engine);
uint8_t mimir_engine_index_epoch(void *engine, uint8_t include_cold, uint64_t *generation);
void mimir_engine_free(mimir_engine_buffer buffer);
void mimir_engine_close(void *engine);
#endif
