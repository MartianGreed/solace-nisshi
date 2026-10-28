# Nisshi broker on WASI p2

This experiment runs Nisshi's Kafka request routes, group coordinator and
DynoStore in-memory storage inside a `wasm32-wasip2` component. Wasmtime supplies
WASI TCP sockets, clocks and randomness. The Go client runs natively. There is
no native broker, protocol proxy, or storage service behind the guest.

## Run the proof

Prerequisites:

- The repository's Rust 1.98 toolchain and its `wasm32-wasip2` target.
- [WASI SDK 34](https://github.com/WebAssembly/wasi-sdk/releases/tag/wasi-sdk-34),
  extracted locally. Its compiler builds the existing LZ4 and Zstandard C code.
- Wasmtime 49.0.3, Go 1.26 or later, Bash and Python 3.
- A free loopback TCP port, default `127.0.0.1:19092`.

From the repository root:

```sh
rustup target add --toolchain 1.98 wasm32-wasip2
export WASI_SDK_PATH=/absolute/path/to/wasi-sdk-34.0-arm64-macos
compat/wasm-proof/run.sh
```

Use the SDK archive matching your host. Set `WASI_BROKER_ADDRESS=127.0.0.1:19093`
to choose another port. The script builds with the lockfile, checks startup
without a network grant, starts the guest, runs the client checks, stops the
guest, and requires the client check to fail afterward. It removes only its
own broker process. Logs remain in the printed `target/wasm-proof.*` directory.

The client checks:

- Topic creation and the duplicate-topic error.
- Two partitions, with two successive producer connections and a new consumer.
- Exact keys, binary values, headers and offsets for 12 records per codec.
- Uncompressed, Gzip, Snappy, LZ4 and Zstandard batches.
- Actual producer batch codecs, so client compression fallback cannot pass a
  compressed case using uncompressed data.
- Rejection of negative, empty and oversized frame lengths, followed by a
  successful client request on a new connection.

The test module pins franz-go and its dependencies in `go.mod` and `go.sum`.
It fails when `BOOTSTRAP_SERVERS` is missing. It does not skip broker checks.

## Run the guest separately

```sh
RUSTFLAGS='--cfg tokio_unstable' cargo build --locked -p nisshi-wasi --target wasm32-wasip2
wasmtime run -S inherit-network=y -S udp=n \
  target/wasm32-wasip2/debug/nisshi-wasi.wasm 127.0.0.1:19092
```

`WASI_SDK_PATH` must remain set during the build. `tokio_unstable` enables
Tokio's experimental WASI networking. The launcher uses its current-thread
executor; codec and record inflation work runs inline because WASI p2 does not
provide the native blocking thread pool. A WASI-only C shim supplies the LZ4 allocation
functions omitted by the locked lz4-sys release, using WASI libc for allocation
and release. The guest accepts only an explicit
loopback address and a nonzero port.

To test an already-running guest:

```sh
cd compat/wasm-proof
BOOTSTRAP_SERVERS=127.0.0.1:19092 go test -count=1 -timeout=180s -v ./...
```

## Scope

The experimental launcher has an in-memory backend, plaintext loopback TCP,
a 16 MiB request limit, 64 active connections and 30-second I/O/request deadlines.
It preserves state between client connections. Restarting the guest erases it.

Native launchers keep their native networking, telemetry and backend dependency
features. The WASI build excludes those host integrations and the runtime
Protocol Buffer schema compiler. An attempted `.proto` compilation returns an
explicit unsupported error. This proof does not establish schema-backed topics,
cloud/database backends, authentication/TLS, maintenance scheduling, graceful
shutdown, durable storage, browser support, throughput or production readiness.
