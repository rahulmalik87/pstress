#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# reproduce_ch.sh  —  Reproduce pstress ClickHouse stress scenario
#
# Downloads pstress-ch and runs three workload configurations against a
# two-replica ClickHouse setup (ports 9000 and 9001 by default).
#
# Supported platforms:
#   Linux x86_64   — downloads pre-built binary from GitHub Releases
#   macOS           — NOT supported (binary is Linux only; use a Linux host)
#
# Usage:
#   bash reproduce_ch.sh [options]
#
# Environment variables:
#   CH_HOST   ClickHouse host   (default: 127.0.0.1)
#   CH_PORT1  First replica port  (default: 9000)
#   CH_PORT2  Second replica port (default: 9001)
#   CH_USER   ClickHouse user   (default: default)
#   CH_PASS   Password          (default: empty)
#   CH_DB     Database          (default: test_db)
#   LOG_DIR   Log directory     (default: /tmp/pstress_ch_reproduce)
# ---------------------------------------------------------------------------

set -euo pipefail

RELEASE_URL="https://github.com/rahulmalik87/pstress/releases/download/v1.0-clickhouse/pstress-ch"
DICT_URL="https://raw.githubusercontent.com/rahulmalik87/pstress/ps-master-clickhouse/src/english_dictionary.txt"

CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT1="${CH_PORT1:-9000}"
CH_PORT2="${CH_PORT2:-9001}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-}"
CH_DB="${CH_DB:-test_db}"
LOG_DIR="${LOG_DIR:-/tmp/pstress_ch_reproduce}"

# ── Platform check ────────────────────────────────────────────────────────────
OS="$(uname -s)"
ARCH="$(uname -m)"
if [[ "$OS" != "Linux" || "$ARCH" != "x86_64" ]]; then
  echo "ERROR: This script requires Linux x86_64. Detected: $OS/$ARCH"
  exit 1
fi

# ── Download binary + dictionary ─────────────────────────────────────────────
BINDIR="$(mktemp -d)"
BINARY="$BINDIR/pstress-ch"

echo "==> Downloading pstress-ch..."
if command -v curl &>/dev/null; then
  curl -fsSL -o "$BINARY"                        "$RELEASE_URL"
  curl -fsSL -o "$BINDIR/english_dictionary.txt" "$DICT_URL"
elif command -v wget &>/dev/null; then
  wget -q -O "$BINARY"                        "$RELEASE_URL"
  wget -q -O "$BINDIR/english_dictionary.txt" "$DICT_URL"
else
  echo "ERROR: curl or wget required."
  exit 1
fi
chmod +x "$BINARY"
echo "    Binary: $BINARY"

# ── Prepare log directory ─────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
echo "    Logs:   $LOG_DIR"
echo ""

# ── Common parameters ─────────────────────────────────────────────────────────
PORTS="${CH_PORT1},${CH_PORT2}"

COMMON_ARGS=(
  "--address=$CH_HOST"
  "--user=$CH_USER"
  "--database=$CH_DB"
  "--null-prob=0"
  "--range=1"
  "--positive-prob=100"
  "--table=10"
  "--only-cl-sql"
  "--records=5"
  "--insert=100"
  "--insert-bulk=100"
  "--delete-precise=10"
  "--update-precise=100"
  "--using-pk=1"
  "--column-type=int,varchar"
  "--single-thread-ddl"
  "--add-column=1000"
  "--drop-column=1000"
  "--pk-prob=100"
  "--seconds=100"
  "--ch-add-column-backfill"
  "--logdir=$LOG_DIR"
)
if [[ -n "$CH_PASS" ]]; then
  COMMON_ARGS+=("--password=$CH_PASS")
fi

# ── Run 1: 4 threads + kill transaction ──────────────────────────────────────
echo "==> Run 1 — port $PORTS, 4 threads, kill-trx-prob-k=1"
"$BINARY" --port="$PORTS" --threads=4 "${COMMON_ARGS[@]}" --single-thread-ddl --kill-trx-prob-k=1
echo "    Run 1 complete."
echo ""

# ── Run 2: 4 threads ─────────────────────────────────────────────────────────
echo "==> Run 2 — port $PORTS, 4 threads"
"$BINARY" --port="$PORTS" --threads=4 "${COMMON_ARGS[@]}" --single-thread-ddl
echo "    Run 2 complete."
echo ""

# ── Run 3: 4 threads ─────────────────────────────────────────────────────────
echo "==> Run 3 — port $PORTS, 4 threads"
"$BINARY" --port="$PORTS" --threads=4 "${COMMON_ARGS[@]}"
echo "    Run 3 complete."
echo ""

echo "==> All runs finished. Logs in: $LOG_DIR"
