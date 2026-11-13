// SPDX-License-Identifier: AGPL-3.0-only
#ifndef MIMIR_RUST_ENGINE_H
#define MIMIR_RUST_ENGINE_H
#include <stddef.h>
#include <stdint.h>

typedef struct { uint8_t *data; size_t len; size_t cap; } mimir_buffer;
// Inputs are borrowed for the call. Outputs belong to Rust and must be freed exactly once. Engines synchronize
// themselves; only closing must not race other calls.
const void *mimir_open(const uint8_t *opts, size_t len, mimir_buffer *err);
mimir_buffer mimir_call(const void *engine, uint32_t op, const uint8_t *input, size_t len, const uint8_t *data, size_t data_len);
void mimir_free(mimir_buffer buffer);
void mimir_close(const void *engine);
#endif
