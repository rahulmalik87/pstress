#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_pstress_ch.sh  —  Download and run pstress ClickHouse stress test
#
# Supported platforms:
#   Linux x86_64   — downloads pre-built binary from GitHub Releases
#   macOS (Intel/Apple Silicon) — runs via Docker (requires Docker Desktop)
#
# Usage:
#   bash run_pstress_ch.sh [extra pstress-ch flags]
#
# Environment variables (all optional):
#   CH_HOST      ClickHouse host/IP          (default: 127.0.0.1)
#   CH_PORT      Native protocol port(s)     (default: 9000)
#                Comma-separated for replica testing: CH_PORT=9001,9002
#   CH_USER      ClickHouse user             (default: default)
#   CH_PASS      Password                    (default: empty)
#   CH_DB        Database name               (default: test_db)
#   TABLES       Number of tables            (default: 10)
#   THREADS      Worker threads per node     (default: 10)
#   DURATION      Test duration in seconds    (default: 300)
#   LOGDIR       Directory for log files     (default: /tmp/pstress_ch)
#   PSTRESS_BIN  Path to an existing binary  (Linux only, skips download)
#
# Examples:
#   # Basic single-node test
#   bash run_pstress_ch.sh
#
#   # Remote server
#   CH_HOST=10.0.0.5 CH_PORT=9000 bash run_pstress_ch.sh
#
#   # Two-replica test with mutation stress
#   CH_PORT=9001,9002 bash run_pstress_ch.sh \
#     --ch-alter-update 5 --ch-alter-delete 5 --ch-mutations-sync
#
#   # Quick 60-second smoke test
#   THREADS=50 DURATION=60 bash run_pstress_ch.sh
# ---------------------------------------------------------------------------

set -euo pipefail

RELEASE_URL="https://github.com/rahulmalik87/pstress/releases/download/v1.0-clickhouse/pstress-ch"

# ── Connection / test defaults ───────────────────────────────────────────────
CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-}"
CH_DB="${CH_DB:-test_db}"
TABLES="${TABLES:-10}"
THREADS="${THREADS:-10}"
DURATION="${DURATION:-300}"
LOGDIR="${LOGDIR:-/tmp/pstress_ch}"

mkdir -p "$LOGDIR"

OS="$(uname -s)"
ARCH="$(uname -m)"

# ── Build common args ────────────────────────────────────────────────────────
ARGS=(
  --address  "$CH_HOST"
  --port     "$CH_PORT"
  --user     "$CH_USER"
  --database "$CH_DB"
  --tables   "$TABLES"
  --threads  "$THREADS"
  --seconds  "$DURATION"
  --logdir   "$LOGDIR"
)
[[ -n "$CH_PASS" ]] && ARGS+=(--password "$CH_PASS")
ARGS+=("$@")   # pass any extra flags through

print_summary() {
  echo ""
  echo "Starting pstress-ch:"
  echo "  Host:    $CH_HOST"
  echo "  Port:    $CH_PORT"
  echo "  User:    $CH_USER"
  echo "  DB:      $CH_DB"
  echo "  Tables:  $TABLES  Threads: $THREADS  Duration: ${DURATION}s"
  echo "  Logs:    $LOGDIR"
  echo ""
}

# ── macOS path — run via Docker (downloads binary inside stock Ubuntu) ────────
if [[ "$OS" == "Darwin" ]]; then
  if ! command -v docker &>/dev/null; then
    echo "ERROR: Docker is required on macOS."
    echo "       Install Docker Desktop: https://www.docker.com/products/docker-desktop/"
    exit 1
  fi

  # Replace loopback with host.docker.internal so the container can reach
  # ClickHouse running on the Mac host.
  DOCKER_CH_HOST="$CH_HOST"
  if [[ "$DOCKER_CH_HOST" == "127.0.0.1" || "$DOCKER_CH_HOST" == "localhost" ]]; then
    DOCKER_CH_HOST="host.docker.internal"
  fi

  # Build the pstress-ch argument string to pass into the container shell.
  PSTRESS_ARGS="--address $DOCKER_CH_HOST --port $CH_PORT --user $CH_USER"
  PSTRESS_ARGS+=" --database $CH_DB --tables $TABLES --threads $THREADS"
  PSTRESS_ARGS+=" --seconds $DURATION --logdir /logs"
  [[ -n "$CH_PASS" ]] && PSTRESS_ARGS+=" --password $CH_PASS"
  for arg in "$@"; do PSTRESS_ARGS+=" $arg"; done

  echo "macOS detected — running via Docker (ubuntu:22.04, platform linux/amd64)"
  echo "  (first run will pull the base image ~30 MB)"
  print_summary

  exec docker run --rm \
    --platform linux/amd64 \
    --add-host host.docker.internal:host-gateway \
    -v "$LOGDIR":/logs \
    ubuntu:22.04 \
    bash -c "
      set -e
      apt-get update -qq && apt-get install -y -qq curl 2>/dev/null
      curl -fsSL '$RELEASE_URL' -o /tmp/pstress-ch
      chmod +x /tmp/pstress-ch
      exec /tmp/pstress-ch $PSTRESS_ARGS
    "
fi

# ── Linux path — native binary ───────────────────────────────────────────────
if [[ "$OS" != "Linux" || "$ARCH" != "x86_64" ]]; then
  echo "ERROR: Unsupported platform: $OS/$ARCH"
  echo "       Supported: Linux x86_64, macOS (Intel/Apple Silicon via Docker)"
  exit 1
fi

if [[ -n "${PSTRESS_BIN:-}" ]]; then
  BINARY="$PSTRESS_BIN"
  echo "Using existing binary: $BINARY"
else
  BINARY="$(mktemp -d)/pstress-ch"
  echo "Downloading pstress-ch..."
  if command -v curl &>/dev/null; then
    curl -fsSL -o "$BINARY" "$RELEASE_URL"
  elif command -v wget &>/dev/null; then
    wget -q -O "$BINARY" "$RELEASE_URL"
  else
    echo "ERROR: Neither curl nor wget found."
    exit 1
  fi
  chmod +x "$BINARY"
  echo "Downloaded to: $BINARY"
fi

if ! "$BINARY" --help &>/dev/null; then
  echo "ERROR: Binary failed to run. Requires glibc 2.17+ (RHEL7 / Ubuntu 14.04 or newer)."
  exit 1
fi

ARGS=(
  --address  "$CH_HOST"
  --port     "$CH_PORT"
  --user     "$CH_USER"
  --database "$CH_DB"
  --tables   "$TABLES"
  --threads  "$THREADS"
  --seconds  "$DURATION"
  --logdir   "$LOGDIR"
)
[[ -n "$CH_PASS" ]] && ARGS+=(--password "$CH_PASS")
ARGS+=("$@")

print_summary
exec "$BINARY" "${ARGS[@]}"
