# pstress — working notes for AI assistants and new contributors

pstress is a probability-driven concurrent workload generator for databases.
This fork's primary target is **ClickHouse** (`pstress-ch`); the MySQL and
DuckDB backends must stay buildable but get no new features. Everything is in
`src/` (~15k lines of C++17). There is no unit test suite: the test result of a
run *is* the verification described below.

## Branches, releases, identity

- Active branch: `master` (`ps-master-clickhouse` was merged into it and
  deleted on 2026-10-09).
- **Pushing `master` republishes the public binary**
  (`.github/workflows/release-ch.yml` uploads to release `v1.0-clickhouse`).
  Treat a push as a release: build and smoke-run locally first.
- Commit only when asked. Author is `Rahul Malik <contact.rmalik@gmail.com>`
  from the repo-local `.git/config`; never touch global git config or add
  signing flags.
- Do not start pstress runs, `start.sh`, or anything that mutates a cluster
  unless asked. Two runs on the same `--database` corrupt each other's replica
  checksums and produce false MISMATCH reports. When a run is requested,
  `pgrep -af pstress` first, then use a fresh `--database` and `--logdir`.

## Build and smoke run

Prerequisite: [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
built with `-DBUILD_SHARED_LIBS=OFF -DWITH_OPENSSL=ON` and installed to
`/usr/local` (`src/CMakeLists.txt` hardcodes `/usr/local/lib/{libcityhash,liblz4,libzstdstatic}.a`).
`WITH_OPENSSL` defaults OFF upstream; without it `--secure` silently opens a
plaintext socket. Check an install with
`nm -C /usr/local/lib/libclickhouse-cpp-lib.a | grep -c SSLSocket` (0 = no TLS).

```bash
cmake -S . -B bld -DCLICKHOUSE=ON \
  -DCLICKHOUSE_INCLUDE_DIR=/usr/local/include \
  -DCLICKHOUSE_LIBRARY=/usr/local/lib/libclickhouse-cpp-lib.a -DCMAKE_BUILD_TYPE=Release
make -C bld -j$(nproc)            # -> bld/src/pstress-ch
```

`STRICT_FLAGS=ON` (default, `-Wall -Werror -pedantic-errors`) builds clean on
gcc 15; do not turn it off. `STRICT_CPU=ON` (default) adds `-march=native`;
release builds use `-DSTRICT_CPU=OFF` (portable x86-64 plus runtime SIMD
dispatch). Exactly one of `MYSQL`/`DUCKDB`/`CLICKHOUSE` must be ON
(`cmake/FindFork.cmake`); it defines `FORK="..."` and `USE_*`.

Smoke run against a local single-node server (~60s, exit 0 expected):

```bash
./bld/src/pstress-ch --port 9000 --tables 5 --threads 4 --seconds 60 \
  --logdir /tmp/smoke --step 1 --no-json
```

Pass = `Schema verification: PASS` at start and end, `COMPLETED`, and a
NODE SUMMARY success ratio around 99%. MySQL-only actions (`AddIndex` without
TYPE, `Analyze`, composite PRIMARY KEY) legitimately fail on ClickHouse. When
checking exit status through a pipe use `PIPESTATUS`, not `$?`.

Local machine notes: the server binary is `~/ClickHouse/build/programs/clickhouse`
(run it with `--config-file=`, it rejects `--path`); clickhouse-cpp source is
at `~/clickhouse-cpp`; GitHub over HTTPS is blocked here, SSH works.

## Code map

| File | What lives there |
|---|---|
| `common.hpp` | `Option` struct and the `Option::Opt` enum (every option's identity and weight) |
| `options_table.cpp` | **The option table.** One row per option: enum, `--name`, type, default, flags, help |
| `help.cpp` | `--help` printing, `--dump-options-json`, option-probability file, server-option parsing |
| `pstress.cpp` | `main()`: getopt loop, per-fork defaults, step detection, node/verifier threads, end-of-run verification |
| `run_method.cpp` | `Thd1::run_some_query()`: the workload loop, one `switch` over the option picked by weight |
| `random_test.hpp` | Declarations: `Table`/`Column`/`Index` hierarchy, `Thd1`, the public free functions |
| `random_test.cpp` | Workload core: option validation (`sum_of_all_options`), `pick_some_option`, `execute_sql`, result-comparison oracles, `load_metadata`, run-wide globals |
| `random_values.cpp` | Random generators: `rand_int`, strings, dictionary words, dates and times |
| `column.cpp` | `Column`/`Index` classes: type names, column definitions, `rand_value()` |
| `table_create.cpp` | Table construction: random columns and indexes, `Table::definition()`, primary-key helpers |
| `table_alter.cpp` | **DDL under load**: DropCreate, Optimize, Check, Truncate, partitions, ALTER column/index actions, MySQL instance alters |
| `table_dml.cpp` | **DML under load**: SELECT/UPDATE/DELETE/REPLACE, WHERE builders, prepared SELECTs, ClickHouse ALTER UPDATE/DELETE |
| `ch_features.cpp` | ClickHouse-specific DDL: engine resolution, materialized views, projections, text/int skip indexes, table settings under load |
| `ch_table_settings.cpp` | The `--table-settings-file` pool: parsing, per-table picks, `alter:`/`session:` entries |
| `workload_helpers.hpp` | Declarations shared between the files above that are not public API (`rng`, run-wide globals, cross-file helpers) |
| `table.cpp` | INSERT construction, metadata JSON (de)serialisation |
| `ch_verify.cpp/.hpp` | ClickHouse schema, replica and materialized-view verification |
| `ClickhouseDatabase.hpp`, `MySQLDatabase.hpp`, `DuckdbDatabase.hpp` | `DatabaseInterface` implementations, picked per thread by `#ifdef` in `thread.cpp` |
| `ch_client_options.hpp` | Shared clickhouse-cpp `ClientOptions` builder; **three places open connections** (`ClickhouseDatabase::connect`, `make_clients`, `ch_verify_schema`) |
| `node.cpp`, `thread.cpp` | One node per `--port`, N worker threads per node |
| `*.sql`, `*.txt` in `src/` | Grammar files, dictionary, table-settings pool; copied next to the binary at build |
| `third_party/` | vendored `inih++` and `rapidjson` |

When moving code between these files, a helper another file needs loses its
`static` and gets a declaration in `workload_helpers.hpp`; nothing else changes.

Include-order trap: `#include <clickhouse/client.h>` must come **before** the
pstress headers in any file that mixes them (pstress has its own `::Column`).

## Options

- **Discover options from the binary, not the source**:
  `./bld/src/pstress-ch --dump-options-json` prints every option with name,
  enum, type, default, effective value, `sql`/`ddl` flags and help. Pass other
  flags alongside it to see what a given command line resolves to. E.g.
  `--dump-options-json | jq '.[] | select(.sql) | {name, value, ddl}'` lists
  the workload actions and their weights.
- **To add an option**: append the enum value at the **end** of `Option::Opt`
  (before `MAX`), add one `OPT(...)` row in `options_table.cpp`, read it with
  `opt_int(X)` / `opt_bool(X)` / `opt_string(X)`. Never insert into the middle
  of the enum: step metadata files record options by enum value.
- The long name rarely matches the enum name (`INITIAL_RECORDS_IN_TABLE` is
  `--records`); look it up in the table or the dump.
- `opt_int`/`opt_bool` macros include a trailing `;`, so they only work as a
  whole statement. In an expression write `options->at(Option::X)->getInt()`.
- An option's **int value is its probability weight** when it has the `sql`
  flag; `run_some_query()` dispatches on it. New workload options ship with a
  **non-zero default** in the range of their neighbours (add-column=1,
  drop-column=1, create-table roll=50); 0 only when the feature is unsound
  by default (oracles needing a deterministic workload, Cloud-only knobs).
- Test "was it given on the command line" with `->cl`, never by comparing
  against the default. ClickHouse defaults for address/user/database/port/
  grammar-file are applied in `pstress.cpp` after parsing, guarded by `->cl`.
- `--host` is an alias remapped to `ADDRESS` in the getopt switch.
- `--config-file` is INI. A `[pstress]` section sets any option by long name
  (`apply_config_file_options` in `pstress.cpp`, right after the getopt loop,
  so file values also set `->cl`); a command-line flag wins over the file and
  an unknown key aborts. `[node]` sections with `run = yes` are the legacy
  pquery per-server layout and only engage when one is present.

## ClickHouse invariants (cross-file, no compiler enforcement)

1. `_pstress_ver UInt64` is injected into every table at CREATE and filled
   with `toUnixTimestamp64Micro(now64())` on every INSERT path. It is never in
   pstress metadata, so every metadata-vs-schema comparison skips it
   explicitly (`DropColumn` refuses it, `ch_verify_schema` ignores it).
2. The engine is always a Replacing variant: `MergeTree()` becomes
   `ReplacingMergeTree(_pstress_ver)`, or `ReplicatedReplacingMergeTree(...)`
   when `--port` has a comma. Multi-port mode creates the database as
   `ENGINE = Replicated(...)`, whose mutations reject non-deterministic
   functions (`rand64`, `now`), hence client-side literals for backfills.
3. `ORDER BY` must be a superset of `PRIMARY KEY` (PK is a prefix). The code
   collects `pk_cols` including composite extras whose `primary_key` flag is
   not set. Do not "simplify" this back to iterating `primary_key` flags.
4. `Nullable(...)` is emitted only when the column is nullable **and**
   `--null-prob > 0`. That condition is duplicated in `Column::definition()`,
   `ModifyColumn` and `ch_verify_schema()`; all three must agree or startup
   schema verification aborts the run.
5. Partition tables (`--partition-prob`, name suffix `_p`) are
   `Partition::CH_KEY`: a `p_col INT` column with `is_partition` set and
   `PARTITION BY ip_col`, values `0 .. number_of_part - 1` with
   `number_of_part` rolled from 2 to `--max-partitions` (8 by default here,
   64 max). The column must stay **out of** `PRIMARY KEY` / `ORDER BY`, or the
   primary index prunes what the part min-max index should; and it is a key
   column to the server, so `SetClause`, `DropColumn`, `ModifyColumn` and
   `ColumnRename` all skip it. `ch_partitioned(table)` is the test; the four
   MySQL kinds never roll on this fork. Half the tables fill partitions
   clock-wise (`--partition-clock-prob`), the rest uniformly.

## Verification model (this is the oracle)

- **Schema check** (`ch_verify_schema`): metadata columns vs `system.columns`,
  at the start of every step (via `std::call_once` in `run_some_query`) and
  at the end. Mismatch aborts.
- **Replica check** (`ch_verify_replicas`): per table, `count()` and
  `sum(cityHash64(toString(tuple(*))))` on every replica, with
  `SETTINGS use_query_cache=0`, after `wait_for_replication()` drains
  `system.replication_queue` (60s cap). At startup, every
  `--ch-verify-interval` seconds, and at the end.
- **MV check** (`--verify-mv`): view vs source; only sound with an insert-only,
  non-collapsing source (`--engine=MergeTree --no-update --no-delete
  --truncate=0 --recreate-table=0 --drop-column=0 --rename-column=0
  --modify-column=0 --kill-trx-prob-k=0`). Anything else is reported as SKIP.
- Workers hold `g_ch_verify_mutex` shared per iteration; the verifier takes
  it exclusive, so workers pause at iteration boundaries.
- Never weaken or skip a check to make a run pass. A `COMPLETED` run that
  skipped verification proves nothing. A mismatch is a finding to investigate.

## Things that look like bugs and are not

- **Step drift.** Without `--step`, `detect_next_step()` scans `--logdir`
  (default `/tmp`) and uses max+1. Steps >= 2 run no DDL and no initial load,
  so a repeated command with `--seconds=0` runs zero queries and still prints
  `COMPLETED` with a `-nan%` ratio. Reproduce with `--step 1` and a fresh
  `--logdir`; read the `Auto-detected step N` line before reading code.
- **`--column-types` does not control the PK type**; `--non-int-pk-prob`
  (default 25) does. It also does not control column count (`--columns`).
- **`--modify-column` on ClickHouse mostly changes nothing**: `VARCHAR(n)`,
  `CHAR(n)`, `TEXT`, `BLOB` are all `String`. Only `DECIMAL(p,s)` retypes.
- **An MV a few rows short of its source is usually a failed or killed
  INSERT**, not a server bug: an INSERT into a table with views commits the
  source first and then each view separately. `--kill-trx-prob-k` is non-zero
  by default and cancels inserts mid-push. Grep `^F INSERT` in the thread logs
  before reporting. Leftover `mv_*` views from a previous run also poison the
  next one.
- **Pre-existing CREATE TABLE failures by seed**: a `Nullable` column 0 used
  as the `ORDER BY` fallback when there is no PK, and `DECIMAL` with precision
  0. Any change that adds a `rand_int()` call shifts the RNG stream and makes
  a previously good seed fail. Confirm with the new feature off before blaming
  it; constrain with `--column-types=int --non-int-pk-prob=0 --null-prob=0`.
- **A replicated run that appears hung for ~10 minutes** on one table is
  usually a server-side lightweight-update / patch-part stall
  (`system.replication_queue` shows `MUTATE_PART` and `DROP_PART` postponed
  with climbing `num_postponed`). It drains; do not kill it.
- **Startup verification on a fresh cluster** dies with `Database <db> does
  not exist` because `ch_verify_startup()` runs before the workers that create
  the database. Pre-create it with the same `Replicated(...)` DDL.
- `src/clickhouse_table_settings.txt` prob-100 lines name settings that must
  exist on the server; `ch_validate_table_settings()` aborts on purpose when
  one does not. Retire the stale line, do not weaken the check.

## How to extend

Find the existing seam and add to it; do not build a parallel harness. A new
workload action is: an enum value, an `OPT(...)` row with `OPT_SQL`
(`| OPT_DDL` for DDL), a `case` in `run_some_query()`, and a `Table` method
that generates the SQL. A new consistency check hooks into `ch_verify.cpp`
and the end of `main()`. Connection-level changes go through
`ch_client_options.hpp` and must cover all three connection sites.
