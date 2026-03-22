#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_pstress_ch.sh  —  Download and run pstress ClickHouse stress test
#
# Usage:
#   bash run_pstress_ch.sh [options passed directly to pstress-ch]
#
# Environment variables (all optional, fall back to defaults):
#   CH_HOST      ClickHouse host/IP          (default: 127.0.0.1)
#   CH_PORT      Native protocol port(s)     (default: 9000)
#                Pass comma-separated ports for replica testing:
#                  CH_PORT=9001,9002 bash run_pstress_ch.sh
#   CH_USER      ClickHouse user             (default: default)
#   CH_PASS      Password                    (default: empty)
#   CH_DB        Database name               (default: test_db)
#   TABLES       Number of tables            (default: 10)
#   THREADS      Worker threads per node     (default: 10)
#   SECONDS      Test duration in seconds    (default: 300)
#   LOGDIR       Directory for log files     (default: /tmp/pstress_ch)
#   PSTRESS_BIN  Path to existing binary     (skip download if set)
#
# Examples:
#   # Basic single-node test
#   bash run_pstress_ch.sh
#
#   # Custom host/port
#   CH_HOST=10.0.0.5 CH_PORT=9000 bash run_pstress_ch.sh
#
#   # Two-replica test with backfill and mutations
#   CH_PORT=9001,9002 bash run_pstress_ch.sh \
#     --ch-alter-update 5 --ch-alter-delete 5 --ch-mutations-sync
#
#   # High-concurrency 60-second smoke test
#   THREADS=50 SECONDS=60 bash run_pstress_ch.sh
# ---------------------------------------------------------------------------

set -euo pipefail

# ── Release URL ─────────────────────────────────────────────────────────────
RELEASE_URL="https://github.com/rahulmalik87/pstress/releases/latest/download/pstress-ch"

# ── Connection defaults ──────────────────────────────────────────────────────
CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-}"
CH_DB="${CH_DB:-test_db}"

# ── Test parameters ──────────────────────────────────────────────────────────
TABLES="${TABLES:-10}"
THREADS="${THREADS:-10}"
SECONDS="${SECONDS:-300}"
LOGDIR="${LOGDIR:-/tmp/pstress_ch}"

# ── Platform check ───────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"
if [[ "$OS" != "Linux" || "$ARCH" != "x86_64" ]]; then
  echo "ERROR: pstress-ch binary is built for Linux x86_64."
  echo "       Detected: $OS / $ARCH"
  exit 1
fi

# ── Download binary if needed ────────────────────────────────────────────────
if [[ -n "${PSTRESS_BIN:-}" ]]; then
  BINARY="$PSTRESS_BIN"
  echo "Using existing binary: $BINARY"
else
  BINARY="$(mktemp -d)/pstress-ch"
  echo "Downloading pstress-ch from GitHub..."
  if command -v curl &>/dev/null; then
    curl -fsSL -o "$BINARY" "$RELEASE_URL"
  elif command -v wget &>/dev/null; then
    wget -q -O "$BINARY" "$RELEASE_URL"
  else
    echo "ERROR: Neither curl nor wget found. Install one and retry."
    exit 1
  fi
  chmod +x "$BINARY"
  echo "Downloaded to: $BINARY"
fi

# ── Verify binary executes ───────────────────────────────────────────────────
if ! "$BINARY" --help &>/dev/null; then
  echo "ERROR: Binary does not run. Check glibc version (needs glibc 2.17+)."
  exit 1
fi

# ── Prepare log directory ────────────────────────────────────────────────────
mkdir -p "$LOGDIR"
echo "Log directory: $LOGDIR"

# ── Build argument list ──────────────────────────────────────────────────────
ARGS=(
  --address  "$CH_HOST"
  --port     "$CH_PORT"
  --user     "$CH_USER"
  --database "$CH_DB"
  --tables   "$TABLES"
  --threads  "$THREADS"
  --seconds  "$SECONDS"
  --logdir   "$LOGDIR"
)
if [[ -n "$CH_PASS" ]]; then
  ARGS+=(--password "$CH_PASS")
fi
# Append any extra args passed to this script
ARGS+=("$@")

# ── Run ──────────────────────────────────────────────────────────────────────
echo ""
echo "Starting pstress-ch:"
echo "  Host:    $CH_HOST"
echo "  Port:    $CH_PORT"
echo "  User:    $CH_USER"
echo "  DB:      $CH_DB"
echo "  Tables:  $TABLES  Threads: $THREADS  Duration: ${SECONDS}s"
echo "  Logs:    $LOGDIR"
echo ""

exec "$BINARY" "${ARGS[@]}"
