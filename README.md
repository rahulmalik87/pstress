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

# Help Section
First, take a quick look at ``` ./pstress-dd --help --verbos, ./pstress-ms --help --verbose ``` to see available modes and options.

# Example of commad to run pstress
```
pstress-ms --table=1 --column=10 --seconds=10 --threads=10 --socket=$SOCKET
```


# Contributors
* Alexey Bychko - C++ code, cmake extensions
* Roel Van de Paar - invention, scripted framework
* Rahul Malik - pstress developer
* Mohit Joshi - pstress developer
* For the full list of contributors, please see [CONTRIBUTORS](https://github.com/Percona-QA/pstress/blob/master/doc/CONTRIBUTORS)
