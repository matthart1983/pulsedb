#!/usr/bin/env bash
# Start PulseDB server + PulseUI dev server together.
# Usage: ./dev.sh [--data-dir DIR] [--release]

set -e

DATA_DIR="${DATA_DIR:-./pulsedb_data}"
BUILD_MODE="--release"
HTTP_PORT="${HTTP_PORT:-8087}"
TCP_PORT="${TCP_PORT:-8086}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-dir) DATA_DIR="$2"; shift 2 ;;
    --debug) BUILD_MODE=""; shift ;;
    --release) BUILD_MODE="--release"; shift ;;
    --http-port) HTTP_PORT="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

ROOT="$(cd "$(dirname "$0")" && pwd)"

cleanup() {
  echo ""
  echo "Shutting down..."
  kill $SERVER_PID $UI_PID 2>/dev/null
  wait $SERVER_PID $UI_PID 2>/dev/null
  echo "Done."
}
trap cleanup EXIT INT TERM

# Build Rust binary
echo "Building PulseDB${BUILD_MODE:+ (release)}..."
cargo build $BUILD_MODE --quiet

if [ -n "$BUILD_MODE" ]; then
  BIN="$ROOT/target/release/pulsedb"
else
  BIN="$ROOT/target/debug/pulsedb"
fi

# Install UI deps if needed
if [ ! -d "$ROOT/ui/node_modules" ]; then
  echo "Installing UI dependencies..."
  (cd "$ROOT/ui" && npm install --silent)
fi

# Start PulseDB server
echo "Starting PulseDB server (TCP :$TCP_PORT, HTTP :$HTTP_PORT, data: $DATA_DIR)..."
$BIN server --data-dir "$DATA_DIR" --tcp-port "$TCP_PORT" --http-port "$HTTP_PORT" &
SERVER_PID=$!
sleep 1

# Start UI dev server
echo "Starting PulseUI (http://localhost:3000)..."
(cd "$ROOT/ui" && npx vite --port 3000 --host) &
UI_PID=$!

echo ""
echo "  PulseDB  → http://localhost:$HTTP_PORT"
echo "  PulseUI  → http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop both."

wait
