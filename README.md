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

**Single node — step 1 (create tables + load data):**
```bash
./bld/src/pstress-ch --port 9000 --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch --step 1
```

**Single node — step 2 (resume workload on existing tables):**
```bash
./bld/src/pstress-ch --port 9000 --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch --step 2
```

**Two replicas on the same host (different ports):**
```bash
./bld/src/pstress-ch --address 127.0.0.1 --port 9000,9001 \
  --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch --step 1
```

**Two replicas on different hosts:**
```bash
./bld/src/pstress-ch --address 192.168.1.10,192.168.1.11 --port 9000,9001 \
  --tables 10 --threads 5 --seconds 60 \
  --logdir /tmp/pstress-ch --step 1
```

**With periodic replica verification every 30 seconds:**
```bash
./bld/src/pstress-ch --port 9000,9001 --tables 10 --threads 5 --seconds 300 \
  --logdir /tmp/pstress-ch --ch-verify-interval 30 --step 2
```

**Useful options for ClickHouse:**

| Option | Description |
|--------|-------------|
| `--port 9000,9001` | Two-replica mode — splits load, verifies schema+checksums at end |
| `--address addr1,addr2` | One address per port for replicas on different hosts |
| `--step 1` | Drop existing tables and start fresh |
| `--step 2` | Resume workload on existing tables |
| `--ch-verify-interval N` | Verify replica count+checksum every N seconds during run |
| `--null-prob=0` | No NULL values; columns use plain types (no `Nullable`) |
| `--no-json` | Disable JSON columns |
| `--single-thread-ddl` | Only thread 0 runs DDL (reduces schema conflicts) |
| `--only-cl-sql` | Run only the SQL operations specified on the command line |
| `--add-column=N` | Probability weight for ALTER TABLE ADD COLUMN |
| `--drop-column=N` | Probability weight for ALTER TABLE DROP COLUMN |
| `--insert-bulk=N` | Probability weight for bulk INSERT |
| `--threads N` | Threads per node |
| `--tables N` | Number of tables to create |
| `--seconds N` | How long to run the workload |

## End-of-run verification

At the end of every run pstress-ch automatically:
1. **Schema verification** — compares each table's columns in pstress metadata against `system.columns` in ClickHouse and reports missing columns, extra columns, and nullability mismatches.
2. **Replica verification** (two-node mode only) — waits for replication queues to drain, then compares row counts and checksums across all replicas.

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

---


# Contributors
* Alexey Bychko - C++ code, cmake extensions
* Roel Van de Paar - invention, scripted framework
* Rahul Malik - pstress developer
* Mohit Joshi - pstress developer
* Claude (Anthropic) - ClickHouse backend, ReplicatedMergeTree support, replica verification, schema verification
* For the full list of contributors, please see [CONTRIBUTORS](https://github.com/Percona-QA/pstress/blob/master/doc/CONTRIBUTORS)
