#!/usr/bin/env bash
# Diamond GOAWAY failover demo.
#
# Topology:
#   PUBLISHER -> TOP(:5550) -> MID-A(:5551) -> BOTTOM(:5553) <- VERIFIER
#                           -> MID-B(:5552) -^
#
# After ~2.5s, MID-A sends GOAWAY to BOTTOM pointing at MID-B.
# BOTTOM migrates upstream from MID-A to MID-B seamlessly.
# The verifier asserts contiguous group sequence (no gap/dup/truncation).
#
# All processes run under wall-clock timeouts with SIGKILL. No orphans.
# WHY SIGKILL: moq-relay treats SIGTERM as "begin graceful drain" which never
# completes when a peer does not cleanly leave. Plain `timeout` (SIGTERM) cannot
# kill them, so we must use -s KILL everywhere to guarantee termination.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Build the workspace (incremental).
echo "==> Building binaries..."
timeout -s KILL 300 cargo build --manifest-path "$REPO_ROOT/Cargo.toml" \
    -p moq-goaway-demo -p moq-relay 2>&1 | tail -5

RELAY="$REPO_ROOT/target/debug/moq-relay"
PUBLISHER="$REPO_ROOT/target/debug/goaway-publisher"
VERIFIER="$REPO_ROOT/target/debug/goaway-verifier"
MID="$REPO_ROOT/target/debug/goaway-mid"

# Max wall-clock for the entire demo.
DEMO_TIMEOUT=45

# Generate certs if missing.
cd "$SCRIPT_DIR"
if [ ! -f ca.pem ]; then
    echo "==> Generating certificates..."
    printf '[req]\ndistinguished_name = req_dn\n[req_dn]\n' > ca.cnf
    export OPENSSL_CONF="$SCRIPT_DIR/ca.cnf"

    openssl req -x509 -sha256 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
        -days 14 -subj "/CN=goaway demo CA" \
        -keyout ca.key -out ca.pem 2>/dev/null

    for name in top mid-a mid-b bottom; do
        openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
            -subj "/CN=$name" -keyout "$name.key" -out "$name.csr" 2>/dev/null
        openssl x509 -req -sha256 -in "$name.csr" \
            -CA ca.pem -CAkey ca.key -CAcreateserial \
            -days 14 -extfile <(printf "subjectAltName=DNS:localhost\n") \
            -out "$name.crt" 2>/dev/null
        rm -f "$name.csr"
    done
    rm -f ca.cnf
fi

# Cleanup function: force-kill all background jobs (SIGKILL, no grace period).
cleanup() {
    echo "==> Cleaning up..."
    kill -9 $(jobs -p) 2>/dev/null || true
    # Killing the timeout wrappers orphans the actual relay/binary children
    # (reparented to init). Kill the real processes by scoped pattern so they
    # cannot survive past this script.
    pkill -9 -f "$SCRIPT_DIR/(top|mid-b|bottom)\.toml" 2>/dev/null || true
    pkill -9 -f "$REPO_ROOT/target/debug/goaway-(mid|publisher|verifier)" 2>/dev/null || true
    # Bounded wait: reap zombies but never hang if something resists.
    timeout -s KILL 5 bash -c 'wait' 2>/dev/null || true
}
trap cleanup EXIT

# Helper: wait for a relay's certificate endpoint to respond.
wait_for() {
    local url="$1" tries=0
    while ! curl -s "$url" >/dev/null 2>&1; do
        tries=$((tries + 1))
        if [ $tries -gt 40 ]; then
            echo "FATAL: timed out waiting for $url" >&2
            exit 1
        fi
        sleep 0.25
    done
}

echo "==> Starting TOP relay (:5550)..."
timeout -s KILL -k 5 "$DEMO_TIMEOUT" "$RELAY" "$SCRIPT_DIR/top.toml" &
PID_TOP=$!
wait_for "http://localhost:5550/certificate.sha256"

echo "==> Starting MID-B relay (:5552)..."
timeout -s KILL -k 5 "$DEMO_TIMEOUT" "$RELAY" "$SCRIPT_DIR/mid-b.toml" &
PID_MIDB=$!
wait_for "http://localhost:5552/certificate.sha256"

echo "==> Starting MID-A proxy (:5551)..."
timeout -s KILL -k 5 "$DEMO_TIMEOUT" "$MID" \
    --client-connect "https://localhost:5550/" \
    --client-tls-root "$SCRIPT_DIR/ca.pem" \
    --client-version moq-transport-19 \
    --server-bind "[::]:5551" \
    --tls-cert "$SCRIPT_DIR/mid-a.crt" \
    --tls-key "$SCRIPT_DIR/mid-a.key" \
    --server-tls-root "$SCRIPT_DIR/ca.pem" \
    --server-version moq-transport-19 \
    --goaway-uri "https://localhost:5552/" \
    --goaway-delay 2500ms \
    --goaway-timeout 5s &
PID_MIDA=$!
# MID-A is our custom binary with no HTTP endpoint; wait for QUIC to bind.
sleep 2

echo "==> Starting BOTTOM relay (:5553)..."
timeout -s KILL -k 5 "$DEMO_TIMEOUT" "$RELAY" "$SCRIPT_DIR/bottom.toml" &
PID_BOTTOM=$!
wait_for "http://localhost:5553/certificate.sha256"

echo "==> Starting publisher (connecting to TOP)..."
timeout -s KILL -k 5 "$DEMO_TIMEOUT" "$PUBLISHER" \
    --client-connect "https://localhost:5550/" \
    --client-tls-root "$SCRIPT_DIR/ca.pem" \
    --client-version moq-transport-19 \
    --broadcast "goaway-test" \
    --interval 200ms \
    --count 50 &
PID_PUB=$!

# Give the publisher a moment to start emitting groups.
sleep 1

echo "==> Starting verifier (subscribing at BOTTOM)..."
timeout -s KILL -k 5 30 "$VERIFIER" \
    --client-connect "https://localhost:5553/" \
    --client-tls-root "$SCRIPT_DIR/ca.pem" \
    --client-version moq-transport-19 \
    --broadcast "goaway-test" \
    --min-groups 20 \
    --timeout 25s
VERIFIER_EXIT=$?

echo ""
if [ $VERIFIER_EXIT -eq 0 ]; then
    echo "SUCCESS: GOAWAY failover demo passed."
else
    echo "FAILURE: Verifier exited with code $VERIFIER_EXIT"
fi

exit $VERIFIER_EXIT
