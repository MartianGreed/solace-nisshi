#include <stdlib.h>
#include <string.h>

void *rust_lz4_wasm_shim_malloc(size_t size) {
    return malloc(size);
}

void *rust_lz4_wasm_shim_calloc(size_t count, size_t size) {
    return calloc(count, size);
}

void rust_lz4_wasm_shim_free(void *pointer) {
    free(pointer);
}

int rust_lz4_wasm_shim_memcmp(const void *left, const void *right, size_t size) {
    return memcmp(left, right, size);
}

void *rust_lz4_wasm_shim_memcpy(void *destination, const void *source, size_t size) {
    return memcpy(destination, source, size);
}

void *rust_lz4_wasm_shim_memmove(void *destination, const void *source, size_t size) {
    return memmove(destination, source, size);
}

void *rust_lz4_wasm_shim_memset(void *destination, int byte, size_t size) {
    return memset(destination, byte, size);
}
