# What is Pstress?

Pstress is an open-source, probability-based database testing tool designed to simulate concurrent workloads and test database recovery under failure conditions. It generates random SQL transactions based on user-provided options, allowing you to test features, regression, and crash recovery. It can stress-test a single server or a cluster with multi-threaded workloads.

Pstress consists of two main components:
- **Workload Module**: A multi-threaded C++ program that creates random metadata and SQL queries to execute.
- **Driver Script**: A Bash script that manages concurrency and crash recovery testing by integrating with the workload module.

# What’s New in Pstress?

- **Flexible Execution**: Run Pstress standalone against an active server or use the driver script with a configuration file. With the script, the server starts automatically, and Pstress executes with options from the config file. The script stops the server after a set time, saves the data directory, restarts with varied settings, and resumes the load—each cycle is called a “step.”
- **Concurrent Load**: During each step, multiple threads execute diverse SQL combinations across separate connections.
- **Crash Recovery Testing**: After each step, Pstress performs crash recovery using the previous step’s data directory.
- **Customizable Workloads**: Generate anything from single-threaded INSERTs to complex multi-threaded transactions, depending on your options.
- **Server Failure Simulation**: Force a shutdown or kill the server to test recovery.
- **Detailed Logging**: Issues are logged in Pstress run logs. By default, each trial saves the data directory, server error logs, thread logs, binaries, and configuration file.

# How to Build Pstress?

1. **Prerequisites**: Install CMake (version 2.6 or higher), a C++ compiler (GCC 4.7+ or equivalent), and development files for MySQL or DuckDB. You may also need OpenSSL or other dependencies based on your setup.
2. **Navigate**: Change to the `pstress` directory.
3. **Run CMake**: Use the appropriate options:
   - `-DMYSQL=ON`: Build Pstress with MySQL support.
   - `-DDUCKDB=ON`: Build Pstress with DuckDB support.
   - Add `-DSTATIC_LIBRARY=OFF` for dynamic linking (default is dynamic).
4. **Custom Paths**: If MySQL or DuckDB is in a non-standard location, set:
   - `-DMYSQL_INCLUDE_DIR` and `-DMYSQL_LIBRARY` (for MySQL).
   - `-DDUCKDB_INCLUDE_DIR` and `-DDUCKDB_LIBRARY` (for DuckDB).
   - Or use `-DBASEDIR` to point to an extracted binary directory for automatic detection (recommended).
5. **Binary Naming**: The resulting binary gets a suffix:
   - `pstress-ms` for MySQL.
   - `pstress-dd` for DuckDB.

# Example of compiling pstress with MySQL
```
cd pstress
mkdir bld && cd bld
cmake .. -DMYSQL=ON -DMYSQL_LIBRARY=$MYSQL_BUILD_DIR/library_output_directory/libmysqlclient.so -DMYSQL_INCLUDE_DIR=$EXTERNAL_OUTPUT_DIRECTORY/include
make
```

# Example of compiling pstress with Duckdb
```
cd pstress
mkdir bld2 && cd bld2
cmake .. -DDUCKDB=ON -DDUCKDB_LIBRARY=$DUCKDB_BUILD_DIR/src/libduckdb.so -DDUCKDB_INCLUDE_DIR=$DUCKDB_BUILD_DIR/src/include
make
```

---

# Building pstress with ClickHouse

## Prerequisites

Install the [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) client library:

```bash
git clone https://github.com/ClickHouse/clickhouse-cpp.git
cd clickhouse-cpp
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
sudo make install   # installs to /usr/local by default
```

## Compile pstress-ch

```bash
cd pstress
mkdir bld && cd bld
cmake .. -DCLICKHOUSE=ON
make -j$(nproc)
# Binary: bld/src/pstress-ch
```

If clickhouse-cpp is installed in a non-standard location:

```bash
cmake .. -DCLICKHOUSE=ON \
  -DCLICKHOUSE_INCLUDE_DIR=/path/to/clickhouse-cpp/include \
  -DCLICKHOUSE_LIBRARY=/path/to/clickhouse-cpp/build/libclickhouse-cpp-lib.a
```

## Running pstress-ch

**Defaults** (no need to specify unless overriding):
- `--address 127.0.0.1`
- `--user default`
- `--database test_db`

**Auto-detect step**: If `--step` is omitted, pstress scans `--logdir` for existing metadata files and automatically sets the next step (step 1 if none found, otherwise max existing step + 1). You can always override with `--step N`.

**Single node — first run (create tables + load data):**
```bash
./bld/src/pstress-ch --port 9000 --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch
```

**Single node — subsequent run (auto-detected step, resume workload):**
```bash
./bld/src/pstress-ch --port 9000 --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch
```

**Two replicas on the same host (different ports):**
```bash
./bld/src/pstress-ch --address 127.0.0.1 --port 9000,9001 \
  --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch
```

**Two replicas on different hosts:**
```bash
./bld/src/pstress-ch --address 192.168.1.10,192.168.1.11 --port 9000,9001 \
  --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch
```

**With periodic replica verification every 30 seconds:**
```bash
./bld/src/pstress-ch --port 9000,9001 --tables 10 --threads 5 --seconds 300 \
  --logdir /tmp/pstress-ch --ch-verify-interval 30
```

**Useful options for ClickHouse:**

| Option | Default | Description |
|--------|---------|-------------|
| `--port 9000,9001` | `9000` | Two-replica mode — splits load, verifies schema+checksums at end |
| `--address addr1,addr2` | `127.0.0.1` | One address per port for replicas on different hosts |
| `--step N` | auto | Force a specific step; omit for auto-detection from logdir |
| `--ch-verify-interval N` | `0` | Verify replica count+checksum every N seconds during run |
| `--null-prob=0` | `20` | No NULL values; columns use plain types (no `Nullable`) |
| `--no-json` | off | Disable JSON columns |
| `--single-thread-ddl` | off | Only thread 0 runs DDL (reduces schema conflicts) |
| `--only-cl-sql` | off | Run only the SQL operations specified on the command line |
| `--add-column=N` | `1` | Probability weight for ALTER TABLE ADD COLUMN |
| `--drop-column=N` | `1` | Probability weight for ALTER TABLE DROP COLUMN |
| `--insert-bulk=N` | `0` | Probability weight for bulk INSERT |
| `--ch-alter-update=N` | `0` | Probability weight for ALTER TABLE UPDATE mutations |
| `--ch-alter-delete=N` | `0` | Probability weight for ALTER TABLE DELETE mutations |
| `--ch-mutations-sync` | off | Append `SETTINGS mutations_sync=2` to all mutations (ADD/DROP COLUMN, ALTER UPDATE/DELETE) |
| `--threads N` | `10` | Threads per node |
| `--tables N` | `10` | Number of tables to create |
| `--seconds N` | `100` | How long to run the workload |

## ClickHouse mutations (ALTER UPDATE / ALTER DELETE)

ClickHouse uses `ALTER TABLE ... UPDATE` and `ALTER TABLE ... DELETE` instead of standard `UPDATE`/`DELETE`. These are background mutations that rewrite data parts.

Enable them with probability weights:

```bash
./bld/src/pstress-ch --port 9000 --tables 10 --threads 5 --seconds 120 \
  --logdir /tmp/pstress-ch \
  --ch-alter-update 5 --ch-alter-delete 3 --only-cl-sql
```

Each mutation targets approximately **50–70% of the table's row range** using a `BETWEEN` clause on the integer primary key, ensuring meaningful data churn rather than single-row updates.

Pass `--ch-mutations-sync` to append `SETTINGS mutations_sync = 2`, which makes mutations complete synchronously on all replicas before the next query proceeds. Without the flag, mutations run asynchronously (default):

```bash
# synchronous mutations:
./bld/src/pstress-ch --ch-alter-update 5 --ch-alter-delete 3 \
  --ch-mutations-sync ...

# asynchronous (default, no flag needed):
./bld/src/pstress-ch --ch-alter-update 5 --ch-alter-delete 3 ...
```

## Schema verification

**Startup check (step ≥ 2):** Before the workload begins, pstress compares the saved metadata against the live ClickHouse schema. If any column is missing or has a nullability mismatch, pstress aborts immediately rather than running a workload against a broken schema.

**End-of-run check:** After every run, pstress re-verifies schema and (in two-node mode) replica consistency.

Example output:
```
[14:32:01] ==> Schema verification: metadata vs ClickHouse (127.0.0.1:9000)...
  tt_1                  OK (5 columns)
  tt_2                  OK (4 columns)
  tt_3                  OK (6 columns)
[14:32:01] ==> Schema verification: PASS

[14:32:01] ==> Verifying replica consistency for 3 tables across 2 replicas...
  tt_1                  r1[cnt=10000    csum=1234567890]  r2[cnt=10000    csum=1234567890]  => OK
  tt_2                  r1[cnt=5000     csum=9876543210]  r2[cnt=5000     csum=9876543210]  => OK
[14:32:01] ==> Replica verification: PASS
```

## DDL log timestamps

All entries in the general (DDL) log file include a `[HH:MM:SS]` timestamp by default, making it straightforward to correlate DDL operations with server-side events.

---


# Contributors
* Alexey Bychko - C++ code, cmake extensions
* Roel Van de Paar - invention, scripted framework
* Rahul Malik - pstress developer
* Mohit Joshi - pstress developer
* Claude (Anthropic) - ClickHouse backend, ReplicatedMergeTree support, replica verification, schema verification
* For the full list of contributors, please see [CONTRIBUTORS](https://github.com/Percona-QA/pstress/blob/master/doc/CONTRIBUTORS)
