# Building and running pstress-ch (ClickHouse)

Verified on Ubuntu with gcc 15.2.0 / cmake 4.2.3 against ClickHouse 26.7.1.1.

ClickHouse is the primary target of this branch. MySQL (`-DMYSQL=ON`) and DuckDB
(`-DDUCKDB=ON`) still build; see the README for those.

---

## 1. Prerequisites

### System packages

```bash
sudo apt-get update
sudo apt-get install -y cmake g++ make git libssl-dev libabsl-dev zlib1g-dev
```

`libssl-dev` is not optional — `cmake/SetupCompiler.cmake` does
`INCLUDE(FindOpenSSL REQUIRED)`, so cmake configure fails without it.
`libabsl-dev` is required by clickhouse-cpp.

### clickhouse-cpp client library

This is the C++ *client* library pstress links against
([ClickHouse/clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)). It is a
different project from the ClickHouse *server* repo (`ClickHouse/ClickHouse`), which
does **not** contain it.

```bash
git clone --depth=1 https://github.com/ClickHouse/clickhouse-cpp.git ~/clickhouse-cpp
cmake -S ~/clickhouse-cpp -B ~/clickhouse-cpp/build \
  -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DWITH_OPENSSL=ON
cmake --build ~/clickhouse-cpp/build --parallel $(nproc)
sudo cmake --install ~/clickhouse-cpp/build      # installs to /usr/local
```

**`-DWITH_OPENSSL=ON` is required for `--secure`** (i.e. for ClickHouse Cloud).
clickhouse-cpp defaults it to `OFF`, and without it `GetSocketFactory()` silently
ignores the SSL options and hands back a plaintext socket — the connection then
fails with `can't receive string data: Connection reset by peer` rather than
anything mentioning TLS. Verify it took:

```bash
nm -C /usr/local/lib/libclickhouse-cpp-lib.a | grep -c SSLSocket   # must be > 0
```

If you are re-running cmake over an existing build tree, note that changing this
flag requires no clean — but you must re-run `cmake --install`.

> Use the SSH remote (`git@github.com:ClickHouse/clickhouse-cpp.git`) on hosts where
> HTTPS to github.com is blocked.

**Install to the default `/usr/local` prefix.** `src/CMakeLists.txt` links three
contrib static libs by absolute path — `/usr/local/lib/libcityhash.a`,
`liblz4.a`, `libzstdstatic.a` — which clickhouse-cpp's `cmake --install` provides.
A different prefix will fail at link time.

Confirm the install produced all five artifacts:

```bash
ls /usr/local/lib/lib{clickhouse-cpp-lib,cityhash,lz4,zstdstatic}.a \
   /usr/local/include/clickhouse/client.h
```

---

## 2. Build

```bash
cmake -S . -B bld -DCLICKHOUSE=ON \
  -DCLICKHOUSE_INCLUDE_DIR=/usr/local/include \
  -DCLICKHOUSE_LIBRARY=/usr/local/lib/libclickhouse-cpp-lib.a \
  -DCMAKE_BUILD_TYPE=Release
make -C bld -j$(nproc)          # -> bld/src/pstress-ch
```

Rebuild after a code change:

```bash
make -C bld -j$(nproc)
```

### Build options that matter

| Option | Default | Notes |
|---|---|---|
| `STRICT_FLAGS` | `ON` | `-Wall -Werror -pedantic-errors`. Builds clean on gcc 15.2 — leave it on; CI uses the default. |
| `STRICT_CPU` | `ON` | Adds `-march=native`. Fine locally. **Release builds must use `-DSTRICT_CPU=OFF`**, otherwise the published binary SIGILLs on other CPUs. With it off, GCC `target_clones` provides runtime AVX-512/AVX2 dispatch. |
| `ASAN` | `OFF` | `-fsanitize=address`. |
| `DEBUG` / `CMAKE_BUILD_TYPE=Debug` | — | `-ggdb3` instead of `-O3`. |

cmake 4.x is fine: this project declares `cmake_minimum_required(VERSION 3.5)`
(the lowest version cmake 4 still accepts) and clickhouse-cpp declares 3.13.

---

## 3. Run a local ClickHouse server

Any reachable server works. To use a server built from the ClickHouse source repo:

```bash
export CH_BASE=~/ch-pstress
mkdir -p $CH_BASE/data

cat > $CH_BASE/config.xml <<EOF
<clickhouse>
    <logger>
        <level>information</level>
        <log>$CH_BASE/server.log</log>
        <errorlog>$CH_BASE/server.err.log</errorlog>
        <size>100M</size>
        <count>3</count>
    </logger>
    <tcp_port>9000</tcp_port>
    <http_port>8123</http_port>
    <listen_host>127.0.0.1</listen_host>
    <path>$CH_BASE/data/</path>
    <tmp_path>$CH_BASE/tmp/</tmp_path>
    <user_files_path>$CH_BASE/user_files/</user_files_path>
    <format_schema_path>$CH_BASE/format_schemas/</format_schema_path>
    <user_directories>
        <users_xml><path>$CH_BASE/users.xml</path></users_xml>
        <local_directory><path>$CH_BASE/data/access/</path></local_directory>
    </user_directories>
    <mark_cache_size>5368709120</mark_cache_size>
    <default_profile>default</default_profile>
    <default_database>default</default_database>
</clickhouse>
EOF

cat > $CH_BASE/users.xml <<'EOF'
<clickhouse>
    <profiles><default></default></profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>
    <quotas><default></default></quotas>
</clickhouse>
EOF

~/ClickHouse/build/programs/clickhouse server --config-file=$CH_BASE/config.xml &
```

Stop it with `pkill -f 'clickhouse server'`.

Note the server **rejects `--path` as a command-line override** ("Unknown option
specified"), which is why the paths go in the config file. Single-node mode needs no
Keeper and no `{shard}`/`{replica}` macros — pstress only uses the `Replicated`
engines when `--port` contains a comma.

---

## 4. Smoke test

```bash
./bld/src/pstress-ch --port 9000 --tables 5 --threads 4 --seconds 60 \
  --logdir /tmp/pstress-smoke --step 1 --no-json
```

Passing run:

- `==> Schema verification: PASS` — printed at both start and end
- `COMPLETED` and exit status 0
- `* NODE SUMMARY [clickhouse:9000]` with a healthy ratio (~99% success)

Verify the ClickHouse-specific invariants directly:

```sql
SELECT name, engine_full FROM system.tables WHERE database='test_db';
-- expect ReplacingMergeTree(_pstress_ver) on every table

SELECT DISTINCT type FROM system.columns
WHERE database='test_db' AND name='_pstress_ver';   -- expect UInt64
```

### Gotchas

- **Always pass `--step 1`** (or a fresh `--logdir`). With `--step` omitted,
  pstress scans the logdir and silently runs `max existing step + 1`; only step 1
  and `--prepare` drop and recreate tables.
- **`--no-json` is currently required.** Without it, initial bulk load fails with
  `Cannot insert data into JSON column: Cannot read JSON object from JSON element:
  [...]` — `src/json.cpp` emits JSON *arrays* for odd-numbered columns, and
  ClickHouse's `JSON` type only accepts objects at top level. Drop the flag once
  that is fixed; it is the check that proves the fix.
- **Don't read the exit status through a pipe.** `./pstress-ch ... | tail` reports
  `tail`'s status, so a failed run looks like success. Redirect to a file, or use
  `${PIPESTATUS[0]}`.
- Several options legitimately report `success=>0` on ClickHouse (`DropIndex`,
  `AddIndex`, `Analyze`, `AddTable`) — they are MySQL-only DDL, not regressions.

### ClickHouse Cloud

Cloud only accepts the native protocol on **port 9440 with TLS**, so it needs
`--secure` (which also makes 9440 the default port, instead of 9000):

```bash
./bld/src/pstress-ch --secure \
  --host <service>.<region>.aws.clickhouse.cloud \
  --user default --password "$CH_PASSWORD" \
  --database test --tables 5 --threads 4 --seconds 60 \
  --logdir /tmp/pstress-cloud --step 1 --no-json
```

- `--host` is an alias for `--address` (same option slot, last one wins), so
  either spelling works everywhere.
- Requires clickhouse-cpp built with `-DWITH_OPENSSL=ON` (see section 1).
- The server cert is verified against the system CA store and SNI is sent; both
  are clickhouse-cpp defaults, so no CA configuration is needed.
- Connect timeout is raised from 5s to 30s under `--secure` — a Cloud service
  resuming from idle regularly exceeds 5s.
- Quote the password in the shell. It is also echoed in cleartext on the
  `Command:` line pstress prints at startup and in `*_general_step_*.log`.
- Single-node only: don't pass a comma-separated `--port`. Cloud presents one
  endpoint, and a comma additionally switches schema generation to
  `Replicated`/`ReplicatedReplacingMergeTree`, which Cloud manages itself.

### Two-replica mode

```bash
./bld/src/pstress-ch --port 9000,9001 --tables 10 --threads 5 --seconds 300 \
  --logdir /tmp/pstress-ch --ch-verify-interval 30
```

A comma in `--port` switches to `ReplicatedReplacingMergeTree` and a `Replicated`
database, so the servers need ClickHouse Keeper plus `{shard}`/`{replica}` macros
configured. pstress then compares `count()` and
`sum(cityHash64(toString(tuple(*))))` across replicas at startup, every
`--ch-verify-interval` seconds, and at the end.
