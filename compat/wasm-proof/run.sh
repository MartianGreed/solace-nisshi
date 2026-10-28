#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
: "${WASI_SDK_PATH:?Set WASI_SDK_PATH to an extracted WASI SDK 34 directory}"
address=${WASI_BROKER_ADDRESS:-127.0.0.1:19092}
mkdir -p "$root/target"
run_directory=$(mktemp -d "$root/target/wasm-proof.XXXXXX")
broker_pid=
cleanup() {
    if [[ -n "$broker_pid" ]]; then
        kill "$broker_pid" 2>/dev/null || true
        wait "$broker_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

RUSTFLAGS="${RUSTFLAGS:+$RUSTFLAGS }--cfg tokio_unstable" cargo build --locked -p nisshi-wasi --target wasm32-wasip2
artifact="$root/target/wasm32-wasip2/debug/nisshi-wasi.wasm"
wasmtime --version
rustc --version
printf 'Checkout HEAD: %s\n' "$(git rev-parse HEAD)"
git status --short
python3 - "$artifact" <<'PYHASH'
import hashlib, pathlib, sys
artifact = pathlib.Path(sys.argv[1])
content = artifact.read_bytes()
if not content.startswith(b"\0asm"):
    raise SystemExit("FAIL: artifact is not WebAssembly")
print("Artifact SHA-256:", hashlib.sha256(content).hexdigest())
PYHASH
python3 - "$artifact" "$run_directory/denied-network.log" "$address" <<'PY'
import subprocess, sys
with open(sys.argv[2], 'w') as output:
    denied = subprocess.run(['wasmtime', 'run', sys.argv[1], sys.argv[3]], stdout=output, stderr=subprocess.STDOUT, timeout=60)
if denied.returncode == 0:
    raise SystemExit('FAIL: broker started without a WASI network grant')
print('PASS: broker refused startup without a WASI network grant')
PY
wasmtime run -S inherit-network=y -S udp=n "$artifact" "$address" > "$run_directory/broker.log" 2>&1 &
broker_pid=$!
for ((attempt=0; attempt<600; attempt++)); do
    if grep -Fq "nisshi-wasi ready $address " "$run_directory/broker.log"; then
        break
    fi
    if ! kill -0 "$broker_pid" 2>/dev/null; then
        cat "$run_directory/broker.log" >&2
        echo "WASI broker exited before readiness" >&2
        exit 1
    fi
    sleep 0.1
done
if ! grep -Fq "nisshi-wasi ready $address " "$run_directory/broker.log"; then
    cat "$run_directory/broker.log" >&2
    echo "WASI broker did not become ready within 60 seconds" >&2
    exit 1
fi
(
    cd compat/wasm-proof
    BOOTSTRAP_SERVERS="$address" go test -count=1 -timeout=180s -v ./...
) | tee "$run_directory/client.log"
kill "$broker_pid"
wait "$broker_pid" 2>/dev/null || true
broker_pid=
if (
    cd compat/wasm-proof
    BOOTSTRAP_SERVERS="$address" go test -count=1 -timeout=15s -run '^TestMalformedFramesCloseOnlyTheirConnection$' ./...
) > "$run_directory/no-broker.log" 2>&1; then
    echo "FAIL: client proof passed with its broker stopped" >&2
    exit 1
fi
printf 'PASS: client proof fails after the WASI broker stops\nEvidence: %s\n' "$run_directory"
