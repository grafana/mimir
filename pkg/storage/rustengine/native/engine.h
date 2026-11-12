// SPDX-License-Identifier: AGPL-3.0-only
#ifndef MIMIR_RUST_ENGINE_H
#define MIMIR_RUST_ENGINE_H
#include <stddef.h>
#include <stdint.h>

typedef struct { uint8_t *data; size_t len; } mimir_engine_buffer;
// Inputs are borrowed for the duration of the call. Outputs belong to Rust and
// must be freed exactly once. The caller serializes access, including close.
void *mimir_engine_open(const uint8_t *path, size_t len, mimir_engine_buffer *error);
mimir_engine_buffer mimir_engine_call(void *engine, const uint8_t *request, size_t len);
void mimir_engine_free(mimir_engine_buffer buffer);
void mimir_engine_close(void *engine);
#endif
